"""Function-based, parameter-driven local image imports."""

from __future__ import annotations

import hashlib
import json
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Literal, Mapping

import pytz
import yaml
from PIL import Image
from shapely.geometry import Point, Polygon, box
from timezonefinder import TimezoneFinder
from wcmatch import glob as wcglob

from ..geoclasses.geoimageframe import GeoImageFrame
from ..import_config import (
    build_import_params_mapping,
    calculate_input_sha,
    normalize_import_yaml,
)
from .local import (
    ImportCancelledError,
    _create_thumbnail_dataset,
    _get_exif_data,
    _get_raster_metadata,
    _normalize_metadata_value,
    _to_decimal,
)


def _wcmatch_flags() -> int:
    """Enable the useful opt-in wcmatch syntax and traversal features."""
    names = (
        "GLOBSTAR",
        "BRACE",
        "EXTGLOB",
        "NEGATE",
        "NEGATEALL",
        "SPLIT",
        "IGNORECASE",
        "DOTGLOB",
        "FOLLOW",
        "GLOBTILDE",
        "GLOBSTARLONG",
        "NUMRANGE",
    )
    flags = 0
    for name in names:
        flags |= getattr(wcglob, name, 0)
    return flags


WCMATCH_FLAGS = _wcmatch_flags()
SUPPORTED_SIDECAR_EXTENSIONS = (".json", ".geojson", ".yaml", ".yml", ".imd")

WORLDVIEW_BOUND_KEYS = (
    "ULLon",
    "ULLat",
    "URLon",
    "URLat",
    "LRLon",
    "LRLat",
    "LLLon",
    "LLLat",
)


def discover_image_paths(file_glob: str) -> list[Path]:
    """Return unique files from one full-path wcmatch pattern."""
    if not isinstance(file_glob, str) or not file_glob.strip():
        raise ValueError("`source_file_glob` must be a non-empty string.")
    matches = wcglob.glob(file_glob, flags=WCMATCH_FLAGS)
    return sorted({Path(match).resolve() for match in matches if Path(match).is_file()})


def _load_json_sidecar(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_yaml_sidecar(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _parse_imd_scalar(value: str) -> Any:
    """Convert a WorldView IMD scalar to a JSON-compatible value."""
    value = value.strip().rstrip(";")
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _load_worldview_imd_sidecar(path: Path) -> dict[str, Any]:
    """Convert a WorldView IMD file to the product/image/bounds mapping."""
    product: dict[str, Any] = {}
    image: dict[str, Any] = {}
    first_band: dict[str, Any] | None = None
    current_group: str | None = None
    group_values: dict[str, Any] = {}

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("BEGIN_GROUP") and "=" in line:
                current_group = line.split("=", 1)[1].strip().rstrip(";")
                group_values = {}
                continue
            if line.startswith("END_GROUP"):
                if current_group == "IMAGE_1":
                    image = dict(group_values)
                elif (
                    current_group
                    and current_group.startswith("BAND_")
                    and first_band is None
                ):
                    first_band = dict(group_values)
                current_group = None
                group_values = {}
                continue
            if line == "END;" or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            key = key.strip()
            parsed_value = _parse_imd_scalar(raw_value)
            if current_group is None:
                product[key] = parsed_value
            else:
                group_values[key] = parsed_value

    missing = [key for key in WORLDVIEW_BOUND_KEYS if key not in (first_band or {})]
    if missing:
        raise ValueError(
            "Missing WorldView bounds in {}: {}".format(path, ", ".join(missing))
        )
    bounds = {
        key: float(first_band[key])  # type: ignore[index]
        for key in WORLDVIEW_BOUND_KEYS
    }
    longitude_values = [bounds[key] for key in ("ULLon", "URLon", "LRLon", "LLLon")]
    latitude_values = [bounds[key] for key in ("ULLat", "URLat", "LRLat", "LLLat")]
    bounds.update(
        min_x=min(longitude_values),
        min_y=min(latitude_values),
        max_x=max(longitude_values),
        max_y=max(latitude_values),
    )
    return {"product": product, "image": image, "bounds": bounds}


SIDECAR_LOADERS = {
    ".json": _load_json_sidecar,
    ".geojson": _load_json_sidecar,
    ".yaml": _load_yaml_sidecar,
    ".yml": _load_yaml_sidecar,
    ".imd": _load_worldview_imd_sidecar,
}


def resolve_sidecar(image_path: Path, pattern: str | None) -> dict[str, Any]:
    """Load one explicitly supported sidecar as a JSON-like mapping."""
    if not pattern:
        return {}
    substituted = pattern.replace("{parent}", str(image_path.parent)).replace(
        "{base}", image_path.stem
    )
    matches = wcglob.glob(substituted, flags=WCMATCH_FLAGS)
    paths = sorted(
        {Path(match).resolve() for match in matches if Path(match).is_file()}
    )
    if len(paths) != 1:
        if not paths:
            raise ValueError(
                "`source_sidecar_glob` matched no file for {} using {!r}.".format(
                    image_path, substituted
                )
            )
        raise ValueError(
            "`source_sidecar_glob` matched more than one file for {}: {}".format(
                image_path,
                ", ".join(str(path) for path in paths),
            )
        )
    sidecar_path = paths[0]
    suffix = sidecar_path.suffix.lower()
    loader = SIDECAR_LOADERS.get(suffix)
    if loader is None:
        raise ValueError(
            "Unsupported sidecar format {!r}; supported formats are {}.".format(
                suffix or sidecar_path.name,
                ", ".join(SUPPORTED_SIDECAR_EXTENSIONS),
            )
        )
    value = loader(sidecar_path)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Sidecar metadata must contain a mapping at its root.")
    return _normalize_metadata_value(value)


def _lookup(mapping: Mapping[str, Any], path: str) -> Any:
    value: Any = mapping
    for component in path.split(".") if path else ():
        if not isinstance(value, Mapping) or component not in value:
            return None
        value = value[component]
    return value


def _file_values(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "name": path.name,
        "stem": path.stem,
        "suffix": path.suffix.lower(),
        "size": stat.st_size,
        "created_at": datetime.fromtimestamp(stat.st_ctime).astimezone().isoformat(),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(),
    }


def _geometry_values(geometry: Point | Polygon | None) -> dict[str, Any]:
    if geometry is None:
        return {}
    values = {
        "wkt": geometry.wkt,
        "bounds": list(geometry.bounds),
    }
    if isinstance(geometry, Point):
        values.update({"x": geometry.x, "y": geometry.y})
    return values


def _parse_exif_time(value: Any, geometry: Point | Polygon | None) -> str | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    parsed = None
    for date_format in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, date_format)
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Unable to parse EXIF time {!r}.".format(raw)) from exc
    if parsed.tzinfo is not None:
        return parsed.isoformat()
    if geometry is None:
        return parsed.isoformat()
    point = geometry if isinstance(geometry, Point) else geometry.centroid
    timezone_name = TimezoneFinder().timezone_at(lat=point.y, lng=point.x)
    if not timezone_name:
        return parsed.isoformat()
    return pytz.timezone(timezone_name).localize(parsed).isoformat()


def resolve_source_value(
    source: str,
    *,
    exif: Mapping[str, Any],
    raster: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    file: Mapping[str, Any],
    geometry: Point | Polygon | None,
    default: Any = None,
) -> Any:
    """Resolve one declarative source expression."""
    if source == "constant":
        return default
    if not isinstance(source, str) or "." not in source:
        return None
    namespace, path = source.split(".", 1)
    contexts = {
        "exif": exif,
        "raster": raster,
        "sidecar": sidecar,
        "file": file,
        "geometry": _geometry_values(geometry),
    }
    if namespace == "exif_parse_time_to_timez":
        return _parse_exif_time(_lookup(exif, path), geometry)
    context = contexts.get(namespace)
    return _lookup(context, path) if context is not None else None


def build_metadata(
    schema: Mapping[str, Any],
    *,
    exif: Mapping[str, Any],
    raster: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    file: Mapping[str, Any],
    geometry: Point | Polygon | None,
) -> dict[str, Any]:
    """Recursively materialize the user-defined metadata structure."""
    output: dict[str, Any] = {}
    for key, definition in schema.items():
        if not isinstance(definition, Mapping):
            raise ValueError("Metadata entry {!r} must be a mapping.".format(key))
        if "source" not in definition:
            output[key] = build_metadata(
                definition,
                exif=exif,
                raster=raster,
                sidecar=sidecar,
                file=file,
                geometry=geometry,
            )
            continue
        default = definition.get("default")
        value = resolve_source_value(
            definition["source"],
            exif=exif,
            raster=raster,
            sidecar=sidecar,
            file=file,
            geometry=geometry,
            default=default,
        )
        if value is None:
            value = default
        if definition.get("required") and value is None:
            raise ValueError("Required metadata value {!r} is missing.".format(key))
        output[key] = _normalize_metadata_value(value)
    return output


def _coordinate(value: Any) -> float | None:
    if value is None:
        return None
    return float(_to_decimal(value))


def _transform_geometry(geometry, input_crs: str | None, output_crs: str):
    if not input_crs or input_crs == output_crs:
        return geometry
    from osgeo import osr

    source_ref = osr.SpatialReference()
    target_ref = osr.SpatialReference()
    source_ref.SetFromUserInput(input_crs)
    target_ref.SetFromUserInput(output_crs)
    source_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    target_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    transform = osr.CoordinateTransformation(source_ref, target_ref)

    def convert(x, y):
        transformed = transform.TransformPoint(float(x), float(y))
        return transformed[0], transformed[1]

    if isinstance(geometry, Point):
        return Point(*convert(geometry.x, geometry.y))
    return Polygon([convert(x, y) for x, y in geometry.exterior.coords])


def build_geometry(
    *,
    source: str,
    output_crs: str,
    exif: Mapping[str, Any],
    raster: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    latitude: str,
    latitude_reference: str,
    longitude: str,
    longitude_reference: str,
    footprint: str,
    min_x: str | None,
    min_y: str | None,
    max_x: str | None,
    max_y: str | None,
    input_crs: str | None,
):
    """Build a point or footprint from the selected geometry source."""
    contexts = {"exif": exif, "raster": raster, "sidecar": sidecar}

    def source_value(expression):
        if not expression or "." not in expression:
            return None
        namespace, path = expression.split(".", 1)
        return _lookup(contexts.get(namespace, {}), path)

    if source == "point_from_exif":
        lat = _coordinate(source_value(latitude))
        lon = _coordinate(source_value(longitude))
        if lat is None or lon is None:
            return None
        if str(source_value(latitude_reference) or "").upper() == "S":
            lat = -abs(lat)
        if str(source_value(longitude_reference) or "").upper() == "W":
            lon = -abs(lon)
        return _transform_geometry(Point(lon, lat), "EPSG:4326", output_crs)

    if source == "bounds_from_image":
        transform = raster.get("geotransform")
        width = raster.get("width")
        height = raster.get("height")
        if transform is None or width is None or height is None:
            return None

        def pixel_to_map(pixel_x, pixel_y):
            origin_x, pixel_width, rotation_x, origin_y, rotation_y, pixel_height = (
                transform
            )
            return (
                origin_x + pixel_x * pixel_width + pixel_y * rotation_x,
                origin_y + pixel_x * rotation_y + pixel_y * pixel_height,
            )

        corners = [
            pixel_to_map(0, 0),
            pixel_to_map(width, 0),
            pixel_to_map(width, height),
            pixel_to_map(0, height),
        ]
        value = Polygon(corners + [corners[0]])
        if footprint == "center":
            value = value.centroid
        return _transform_geometry(value, raster.get("projection"), output_crs)

    if source == "bounds_from_sidecar":
        coordinates = [source_value(item) for item in (min_x, min_y, max_x, max_y)]
        if any(value is None for value in coordinates):
            return None
        return _transform_geometry(
            box(*(float(value) for value in coordinates)),
            input_crs,
            output_crs,
        )
    raise ValueError("Unsupported geometry source: {!r}.".format(source))


def calculate_file_fingerprint(
    image_path: Path,
    *,
    enabled: bool,
    mode: str,
    sample_size: int = 65536,
) -> str | None:
    """Calculate a robust or sampled SHA-256 file fingerprint."""
    if not enabled:
        return None
    if mode not in {"robust", "quick"}:
        raise ValueError("`fingerprint_mode` must be 'robust' or 'quick'.")
    hasher = hashlib.sha256()
    file_size = image_path.stat().st_size
    with image_path.open("rb") as handle:
        if mode == "robust":
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        else:
            offsets = {
                0,
                max(file_size // 2 - sample_size // 2, 0),
                max(file_size - sample_size, 0),
            }
            for offset in sorted(offsets):
                handle.seek(offset)
                hasher.update(handle.read(sample_size))
            hasher.update(str(file_size).encode("utf-8"))
    return hasher.hexdigest()


def _load_image_record(
    image_path: Path, parameters: Mapping[str, Any]
) -> dict[str, Any] | None:
    try:
        with Image.open(image_path) as image:
            exif = _normalize_metadata_value(_get_exif_data(image))
    except Exception:
        exif = {}
    try:
        raster = _get_raster_metadata(image_path)
    except Exception:
        raster = {}
    sidecar = resolve_sidecar(image_path, parameters["source_sidecar_glob"])
    file = _file_values(image_path)
    geometry = build_geometry(
        source=parameters["geometry_source"],
        output_crs=parameters["geometry_output_crs"],
        exif=exif,
        raster=raster,
        sidecar=sidecar,
        latitude=parameters["geometry_latitude"],
        latitude_reference=parameters["geometry_latitude_reference"],
        longitude=parameters["geometry_longitude"],
        longitude_reference=parameters["geometry_longitude_reference"],
        footprint=parameters["geometry_footprint"],
        min_x=parameters["geometry_min_x"],
        min_y=parameters["geometry_min_y"],
        max_x=parameters["geometry_max_x"],
        max_y=parameters["geometry_max_y"],
        input_crs=parameters["geometry_input_crs"],
    )
    if geometry is None and parameters["geometry_required"]:
        raise ValueError("Required geometry could not be resolved.")

    contexts = dict(
        exif=exif, raster=raster, sidecar=sidecar, file=file, geometry=geometry
    )
    name = resolve_source_value(
        parameters["name_source"], default=parameters["name_default"], **contexts
    )
    if name is None:
        name = parameters["name_default"]
    if name is None and parameters["name_required"]:
        raise ValueError("Required name could not be resolved.")
    image_url = resolve_source_value(parameters["image_url_source"], **contexts)
    if image_url is None and parameters["image_url_required"]:
        raise ValueError("Required image_url could not be resolved.")

    thumbnail = None
    if parameters["thumbnail_enabled"]:
        thumbnail = _create_thumbnail_dataset(
            image_path,
            size=(parameters["thumbnail_width"], parameters["thumbnail_height"]),
            resampling=parameters["thumbnail_resampling"],
        )
    return {
        "name": str(name) if name is not None else None,
        "image_url": str(image_url) if image_url is not None else None,
        "geometry": geometry,
        "metadata": build_metadata(parameters["metadata"], **contexts),
        "thumbnail": thumbnail,
        "fingerprint": calculate_file_fingerprint(
            image_path,
            enabled=parameters["fingerprint_enabled"],
            mode=parameters["fingerprint_mode"],
        ),
        "input_sha": parameters["input_sha"],
        "import_params": parameters["import_params"],
    }


def _chunked(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _frame(records, output_crs):
    frame = GeoImageFrame(records, geometry="geometry")
    frame.set_crs(output_crs, inplace=True)
    return frame


def import_local_images(
    *,
    source_file_glob: str,
    source_sidecar_glob: str | None = None,
    name_source: str = "file.name",
    name_required: bool = True,
    name_default: Any = None,
    image_url_source: str = "file.path",
    image_url_required: bool = True,
    geometry_source: str = "point_from_exif",
    geometry_output_crs: str = "EPSG:4326",
    geometry_required: bool = True,
    geometry_latitude: str = "exif.GPSInfo.GPSLatitude",
    geometry_latitude_reference: str = "exif.GPSInfo.GPSLatitudeRef",
    geometry_longitude: str = "exif.GPSInfo.GPSLongitude",
    geometry_longitude_reference: str = "exif.GPSInfo.GPSLongitudeRef",
    geometry_footprint: str = "bounds",
    geometry_min_x: str | None = None,
    geometry_min_y: str | None = None,
    geometry_max_x: str | None = None,
    geometry_max_y: str | None = None,
    geometry_input_crs: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    thumbnail_enabled: bool = True,
    thumbnail_width: int = 256,
    thumbnail_height: int = 256,
    thumbnail_resampling: str = "lanczos",
    fingerprint_enabled: bool = True,
    fingerprint_mode: Literal["robust", "quick"] = "robust",
    max_workers: int = 1,
    batch_size: int = 100,
    return_as_yield: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
    skip_images_in_postgresql=None,
    skip_existing: bool = True,
    on_error: Literal["skip", "warn", "error"] = "skip",
    cancel_event: threading.Event | None = None,
):
    """Import local images using explicit Python parameters, never YAML input."""
    if max_workers < 1 or batch_size < 1:
        raise ValueError("`max_workers` and `batch_size` must be positive integers.")
    if on_error not in {"skip", "warn", "error"}:
        raise ValueError("`on_error` must be 'skip', 'warn', or 'error'.")
    metadata = dict(metadata or {})
    config = build_import_params_mapping(
        source_file_glob=source_file_glob,
        source_sidecar_glob=source_sidecar_glob,
        name_source=name_source,
        name_required=name_required,
        name_default=name_default,
        image_url_source=image_url_source,
        image_url_required=image_url_required,
        geometry_source=geometry_source,
        geometry_output_crs=geometry_output_crs,
        geometry_required=geometry_required,
        geometry_latitude=geometry_latitude,
        geometry_latitude_reference=geometry_latitude_reference,
        geometry_longitude=geometry_longitude,
        geometry_longitude_reference=geometry_longitude_reference,
        geometry_footprint=geometry_footprint,
        geometry_min_x=geometry_min_x,
        geometry_min_y=geometry_min_y,
        geometry_max_x=geometry_max_x,
        geometry_max_y=geometry_max_y,
        geometry_input_crs=geometry_input_crs,
        metadata=metadata,
        thumbnail_enabled=thumbnail_enabled,
        thumbnail_width=thumbnail_width,
        thumbnail_height=thumbnail_height,
        thumbnail_resampling=thumbnail_resampling,
        fingerprint_enabled=fingerprint_enabled,
        fingerprint_mode=fingerprint_mode,
    )
    canonical_yaml = normalize_import_yaml(config)
    input_sha = calculate_input_sha(config)
    parameters = dict(
        source_sidecar_glob=source_sidecar_glob,
        name_source=name_source,
        name_required=name_required,
        name_default=name_default,
        image_url_source=image_url_source,
        image_url_required=image_url_required,
        geometry_source=geometry_source,
        geometry_output_crs=geometry_output_crs,
        geometry_required=geometry_required,
        geometry_latitude=geometry_latitude,
        geometry_latitude_reference=geometry_latitude_reference,
        geometry_longitude=geometry_longitude,
        geometry_longitude_reference=geometry_longitude_reference,
        geometry_footprint=geometry_footprint,
        geometry_min_x=geometry_min_x,
        geometry_min_y=geometry_min_y,
        geometry_max_x=geometry_max_x,
        geometry_max_y=geometry_max_y,
        geometry_input_crs=geometry_input_crs,
        metadata=metadata,
        thumbnail_enabled=thumbnail_enabled,
        thumbnail_width=thumbnail_width,
        thumbnail_height=thumbnail_height,
        thumbnail_resampling=thumbnail_resampling,
        fingerprint_enabled=fingerprint_enabled,
        fingerprint_mode=fingerprint_mode,
        input_sha=input_sha,
        import_params=canonical_yaml,
    )
    paths = discover_image_paths(source_file_glob)
    if skip_existing and skip_images_in_postgresql is not None:
        paths = [
            Path(path) for path in skip_images_in_postgresql.filter_existing_rows(paths)
        ]
    if not paths:
        raise ValueError("No new files match `source_file_glob`.")

    def batches():
        processed = 0
        total = len(paths)
        if progress_callback:
            progress_callback(0, total)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            def load_batch(batch):
                records = []
                for path in batch:
                    try:
                        record = _load_image_record(path, parameters)
                    except Exception as exc:
                        if on_error == "error":
                            raise
                        if on_error == "warn":
                            warnings.warn(
                                "Skipped {}: {}".format(path, exc),
                                stacklevel=2,
                            )
                        continue
                    if record is not None:
                        records.append(record)
                return records

            futures = {
                executor.submit(load_batch, batch): batch
                for batch in _chunked(paths, batch_size)
            }
            for future in as_completed(futures):
                if cancel_event is not None and cancel_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    raise ImportCancelledError("Image import cancelled.")
                batch = futures[future]
                try:
                    records = [
                        record for record in future.result() if record is not None
                    ]
                except Exception as exc:
                    if on_error == "error":
                        raise
                    warnings.warn("Skipped import batch: {}".format(exc), stacklevel=2)
                    records = []
                processed += len(batch)
                if progress_callback:
                    progress_callback(processed, total)
                if records:
                    yield _frame(records, geometry_output_crs)

    if return_as_yield:
        return batches()
    frames = list(batches())
    if not frames:
        raise ValueError("Matched files, but none could be imported.")
    records = [record for frame in frames for record in frame.to_dict(orient="records")]
    return _frame(records, geometry_output_crs)
