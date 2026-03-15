import numbers
import re
import warnings
import hashlib

from datetime import datetime
from pathlib import Path

import numpy as np
import pytz

from PIL import Image
from PIL.ExifTags import GPSTAGS, TAGS
from shapely import Point, Polygon
from timezonefinder import TimezoneFinder

from ..geoclasses.geoimageframe import GeoImageFrame

try:
    from osgeo import gdal
except ImportError:
    gdal = None


class SearchLocalToGeoImageFrame:
    """Orchestrate file discovery and dispatch to importer classes."""

    def __new__(
        cls,
        directory,
        import_types={"GeoTaggedImage": r".*\.JPG$"},
        additional_columns=None,
        create_thumbnail=True,
        thumbnail_size=(256, 256),
        fingerprint=None,
    ):
        """Return a `GeoImageFrame` directly from the import configuration."""
        if cls is SearchLocalToGeoImageFrame:
            if import_types is None:
                import_types = {"GeoTaggedImage": r".*"}

            if not isinstance(import_types, dict):
                raise TypeError(
                    "`import_types` must be a dict like {GeoTaggedImage: r'.*\\.JPG$'}."
                )

            if not import_types:
                raise ValueError("`import_types` must contain at least one importer class.")
            if fingerprint not in (None, "robust", "quick"):
                raise ValueError("`fingerprint` must be one of None, 'robust', or 'quick'.")

            all_records = []
            matched_file_count = 0

            for importer_ref, pattern in import_types.items():
                importer_cls = cls._resolve_importer_class(importer_ref)
                if not issubclass(importer_cls, SearchLocalToGeoImageFrame):
                    raise TypeError(
                        "Importer keys must be subclasses of ImportImages."
                    )

                image_paths = cls._discover_paths(directory, pattern)
                matched_file_count += len(image_paths)
                for image_path in image_paths:
                    try:
                        record = importer_cls.load(
                            image_path=image_path,
                            query_from=directory,
                            search_re=pattern,
                            import_type=importer_cls.__name__,
                            additional_columns=additional_columns,
                            create_thumbnail=create_thumbnail,
                            thumbnail_size=thumbnail_size,
                            fingerprint=fingerprint,
                        )
                    except Exception as exc:
                        warnings.warn(f"Error processing {image_path}: {exc}. Skipped.")
                        continue

                    if record is not None:
                        all_records.append(record)

            if matched_file_count == 0:
                raise ValueError("The directory does not contain any images matching `import_types`.")

            if not all_records:
                raise ValueError("No valid images were processed into a GeoImageFrame.")

            frame = GeoImageFrame(all_records, geometry="geometry")
            frame.set_crs(epsg=4326, inplace=True)
            return frame

        return super().__new__(cls)

    @classmethod
    def _resolve_importer_class(cls, importer):
        """Resolve an importer class from a class object or its class name."""
        if isinstance(importer, str):
            importer_cls = globals().get(importer)
            if not isinstance(importer_cls, type):
                raise TypeError(
                    f"`import_types` key '{importer}' does not resolve to an importer class."
                )
            return importer_cls

        return importer

    @classmethod
    def _discover_paths(cls, directory, pattern):
        """Resolve a single regex pattern against `directory`."""
        base_path = Path(directory)
        if not isinstance(pattern, str):
            raise TypeError("Each `import_types` value must be a single regex string.")

        compiled_pattern = re.compile(pattern)
        matched_paths = []
        for path in base_path.rglob("*"):
            if not path.is_file():
                continue

            relative_path = path.relative_to(base_path).as_posix()
            file_name = path.name
            if compiled_pattern.search(relative_path) or compiled_pattern.search(file_name):
                matched_paths.append(path)
        return sorted(set(matched_paths))

class GeoTaggedImage(SearchLocalToGeoImageFrame):
    """Importer for EXIF geotagged images."""

    @classmethod
    def load(
        cls,
        image_path,
        query_from=None,
        search_re=None,
        import_type=None,
        additional_columns=None,
        create_thumbnail=True,
        thumbnail_size=(256, 256),
        fingerprint=None,
    ):
        """Load a single geotagged image into a GeoImageFrame-compatible record."""
        with Image.open(image_path) as img:
            exif_data = _normalize_metadata_value(_get_exif_data(img))

        source = _extract_source(image_path)
        fingerprint_data = _calculate_fingerprint(image_path, fingerprint)
        raster = _get_raster_metadata(image_path)
        geometry_data = _extract_latlon_from_metadata(
            image_path=image_path,
            exif_data=exif_data,
            get_geotagging=_get_geotagging,
            get_coordinates=_get_coordinates,
        )
        if geometry_data is None:
            return None

        camera_data = _extract_camera(
            exif_data=exif_data,
            get_focal_length=_get_focal_length,
            infer_camera_type=_infer_camera_type,
        )
        sensor_data = _extract_sensor_values(
            geotags=geometry_data["geotags"],
            exif_data=exif_data,
            get_image_altitude=_get_image_altitude,
            get_image_direction=_get_image_direction,
            to_float32_or_nan=_to_float32_or_nan,
        )
        thumbnail_data = _extract_thumbnail(
            image_path=image_path,
            create_thumbnail=create_thumbnail,
            thumbnail_size=thumbnail_size,
        )
        captured_at = _extract_datetime(
            exif_data=exif_data,
            latitude=geometry_data["latitude"],
            longitude=geometry_data["longitude"],
        )
        metadata = _build_metadata(
            source=source,
            query_from=query_from,
            import_type=import_type or cls.__name__,
            search_re=search_re,
            fingerprint=fingerprint_data,
            raster=raster,
            exif_data=exif_data,
            geotags=geometry_data["geotags"],
            captured_at=captured_at,
            camera_data=camera_data,
            sensor_data=sensor_data,
        )

        image_data = {
            "name": source["name"],
            "image_url": source["path"],
            "geometry": geometry_data["geometry"],
            "metadata": metadata,
            "thumbnail": thumbnail_data,
            "fingerprint": fingerprint_data["value"] if fingerprint_data else None,
        }

        return _apply_additional_columns(
            image_data=image_data,
            metadata=metadata,
            additional_columns=additional_columns,
        )


class GeoTransformImage(SearchLocalToGeoImageFrame):
    """Importer for raster images that already contain a geotransform."""

    @classmethod
    def load(
        cls,
        image_path,
        query_from=None,
        search_re=None,
        import_type=None,
        additional_columns=None,
        create_thumbnail=True,
        thumbnail_size=(256, 256),
        fingerprint=None,
    ):
        """Load a single georeferenced raster into a GeoImageFrame-compatible record."""
        source = _extract_source(image_path)
        fingerprint_data = _calculate_fingerprint(image_path, fingerprint)
        raster = _get_raster_metadata(image_path)
        geometry_data = _extract_geometry_from_geotransform(
            image_path=image_path,
            raster=raster,
        )
        if geometry_data is None:
            return None

        thumbnail_data = _extract_thumbnail(
            image_path=image_path,
            create_thumbnail=create_thumbnail,
            thumbnail_size=thumbnail_size,
        )
        metadata = {
            "input_params": {
                "query_from": query_from,
                "import_type": import_type or cls.__name__,
                "search_re": search_re,
            },
            "source": source,
            "fingerprint": fingerprint_data,
            "raster": raster,
        }

        image_data = {
            "name": source["name"],
            "image_url": source["path"],
            "geometry": geometry_data["geometry"],
            "metadata": metadata,
            "thumbnail": thumbnail_data,
            "fingerprint": fingerprint_data["value"] if fingerprint_data else None,
        }

        return _apply_additional_columns(
            image_data=image_data,
            metadata=metadata,
            additional_columns=additional_columns,
        )


 # ------------ Helpers


def _normalize_metadata_value(value):
    """Convert metadata values into Python-native, JSON-friendly objects."""
    if isinstance(value, dict):
        return {
            str(key): _normalize_metadata_value(val)
            for key, val in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_normalize_metadata_value(item) for item in value]

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")

    if isinstance(value, numbers.Number) or value is None or isinstance(value, str):
        return value

    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        if value.denominator == 0:
            return None
        return float(value.numerator) / float(value.denominator)

    if hasattr(value, "num") and hasattr(value, "den"):
        if value.den == 0:
            return None
        return float(value.num) / float(value.den)

    return str(value)


def _to_float32_or_nan(value):
    """Convert a value to `np.float32`, returning `np.nan` when missing."""
    if value is None:
        return np.nan
    return np.float32(value)


def _infer_camera_type(focal_length):
    """Infer a coarse camera type from focal length only."""
    if not focal_length:
        return np.nan

    if focal_length < 1.5:
        return "fisheye"
    return "perspective"


def _to_decimal(coord_tuple):
    """Convert coordinates from EXIF tuple or ratio notation into decimals."""
    if isinstance(coord_tuple, list):
        coord_tuple = tuple(coord_tuple)

    if isinstance(coord_tuple, tuple) and len(coord_tuple) == 3:
        return (
            float(coord_tuple[0])
            + float(coord_tuple[1]) / 60
            + float(coord_tuple[2]) / 3600
        )

    if isinstance(coord_tuple, str) and "/" in coord_tuple:
        num, denom = coord_tuple.split("/")
        if float(denom) == 0:
            return None
        return float(num) / float(denom)

    return coord_tuple


def _get_geotagging(exif):
    """Extract GPS metadata from an EXIF dictionary."""
    if not exif:
        raise ValueError("No EXIF metadata found")

    gps_data = exif.get("GPSInfo")
    if not gps_data:
        raise ValueError("No EXIF geotagging found")

    geotagging = {}
    for key, val in GPSTAGS.items():
        data_value = gps_data.get(key) or gps_data.get(val)
        if data_value:
            geotagging[val] = data_value

    return geotagging


def _get_image_altitude(geotags):
    """Return altitude from geotags when available."""
    return geotags.get("GPSAltitude")


def _get_image_direction(geotags):
    """Return image direction from geotags when available."""
    return geotags.get("GPSImgDirection")


def _get_coordinates(geotags):
    """Return latitude and longitude from EXIF GPS tags."""
    lat = _to_decimal(geotags["GPSLatitude"])
    lon = _to_decimal(geotags["GPSLongitude"])

    if geotags["GPSLatitudeRef"] == "S":
        lat = -lat

    if geotags["GPSLongitudeRef"] == "W":
        lon = -lon

    return lat, lon


def _get_focal_length(exif_data):
    """Return focal length from EXIF data as a float."""
    focal_length = exif_data.get("FocalLength")

    if focal_length is None:
        return None

    if isinstance(focal_length, numbers.Number):
        return float(focal_length)

    if (
        isinstance(focal_length, tuple)
        and len(focal_length) == 2
        and focal_length[1] != 0
    ):
        return float(focal_length[0]) / focal_length[1]

    if (
        hasattr(focal_length, "num")
        and hasattr(focal_length, "den")
        and focal_length.den != 0
    ):
        return float(focal_length.num) / float(focal_length.den)

    return None


def _metadata_lookup(metadata, key_path):
    """Resolve a dotted metadata path, returning `np.nan` when absent."""
    current = metadata
    for key in key_path.split("."):
        if not isinstance(current, dict) or key not in current:
            return np.nan
        current = current[key]
    return current


def _apply_additional_columns(image_data, metadata, additional_columns):
    """Apply user-requested derived metadata columns to the record."""
    for column_info in additional_columns or []:
        if isinstance(column_info, str):
            image_data[column_info] = np.nan
        elif isinstance(column_info, tuple) and len(column_info) == 2:
            col_name, key_path = column_info
            image_data[col_name] = _metadata_lookup(metadata, key_path)

    return image_data


def _extract_source(image_path):
    """Extract source metadata for the original file."""
    path = Path(image_path)
    return {
        "path": str(path),
        "name": path.name,
        "suffix": path.suffix.lower(),
    }


def _calculate_fingerprint(image_path, mode, sample_size=65536):
    """Calculate a robust or quick file fingerprint."""
    if mode is None:
        return None

    path = Path(image_path)
    if mode not in ("robust", "quick"):
        raise ValueError("Fingerprint mode must be None, 'robust', or 'quick'.")

    hasher = hashlib.blake2b(digest_size=32)
    file_size = path.stat().st_size

    with path.open("rb") as handle:
        if mode == "robust":
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        else:
            offsets = [0]
            if file_size > sample_size:
                offsets.append(max((file_size // 2) - (sample_size // 2), 0))
                offsets.append(max(file_size - sample_size, 0))

            for offset in sorted(set(offsets)):
                handle.seek(offset)
                hasher.update(handle.read(sample_size))
            hasher.update(str(file_size).encode("utf-8"))

    return {
        "mode": mode,
        "algorithm": "blake2b",
        "value": hasher.hexdigest(),
    }


def _extract_latlon_from_metadata(image_path, exif_data, get_geotagging, get_coordinates):
    """Extract normalized GPS metadata and geometry."""
    geotags = get_geotagging(exif_data)
    lat, lon = get_coordinates(geotags)
    if lat is None or lon is None:
        warnings.warn(
            f"Skipping {image_path}: invalid GPS coordinates (lat={lat}, lon={lon}).",
            stacklevel=2,
        )
        return None

    return {
        "geotags": geotags,
        "latitude": lat,
        "longitude": lon,
        "geometry": Point(lon, lat),
    }


def _extract_geometry_from_geotransform(image_path, raster):
    """Extract a footprint polygon from raster geotransform metadata."""
    geotransform = raster.get("geotransform")
    width = raster.get("width")
    height = raster.get("height")

    if geotransform is None:
        warnings.warn(
            f"Skipping {image_path}: raster geotransform is missing.",
            stacklevel=2,
        )
        return None

    if width is None or height is None:
        warnings.warn(
            f"Skipping {image_path}: raster dimensions are missing.",
            stacklevel=2,
        )
        return None

    def _pixel_to_map(pixel_x, pixel_y):
        origin_x, pixel_width, rotation_x, origin_y, rotation_y, pixel_height = geotransform
        map_x = origin_x + pixel_x * pixel_width + pixel_y * rotation_x
        map_y = origin_y + pixel_x * rotation_y + pixel_y * pixel_height
        return map_x, map_y

    upper_left = _pixel_to_map(0, 0)
    upper_right = _pixel_to_map(width, 0)
    lower_right = _pixel_to_map(width, height)
    lower_left = _pixel_to_map(0, height)

    return {
        "geometry": Polygon(
            [upper_left, upper_right, lower_right, lower_left, upper_left]
        )
    }


def _extract_datetime(exif_data, latitude, longitude):
    """Extract ISO-8601 capture time from EXIF DateTime."""
    tf = TimezoneFinder()
    captured_at_str = exif_data.get("DateTime")
    if captured_at_str:
        captured_at_naive = datetime.strptime(
            captured_at_str, "%Y:%m:%d %H:%M:%S"
        )
        tz_name = tf.timezone_at(lat=latitude, lng=longitude)
        if tz_name:
            local_tz = pytz.timezone(tz_name)
            return local_tz.localize(captured_at_naive).isoformat()
        return captured_at_naive.isoformat()
    return None


def _extract_camera(exif_data, get_focal_length, infer_camera_type):
    """Extract focal length, camera type, and camera parameter string."""
    focal_length = get_focal_length(exif_data)
    camera_type = infer_camera_type(focal_length)
    camera_parameters = np.nan
    if focal_length is not None:
        camera_parameters = ",".join([str(focal_length), "None", "None"])

    return {
        "focal_length": focal_length,
        "camera_type": camera_type,
        "camera_parameters": camera_parameters,
    }


def _extract_sensor_values(
    geotags,
    exif_data,
    get_image_altitude,
    get_image_direction,
    to_float32_or_nan,
):
    """Extract altitude, direction, and orientation values."""
    return {
        "altitude": to_float32_or_nan(get_image_altitude(geotags)),
        "compass_angle": to_float32_or_nan(get_image_direction(geotags)),
        "exif_orientation": to_float32_or_nan(exif_data.get("Orientation")),
    }


def _extract_thumbnail(image_path, create_thumbnail, thumbnail_size):
    """Extract thumbnail dataset when requested."""
    thumbnail = None
    if create_thumbnail:
        thumbnail = _create_thumbnail_dataset(
            image_path,
            size=thumbnail_size,
        )

    return thumbnail


def _build_metadata(
    source,
    query_from,
    import_type,
    search_re,
    fingerprint,
    raster,
    exif_data,
    geotags,
    captured_at,
    camera_data,
    sensor_data,
):
    """Build the metadata dictionary stored on the record."""
    return {
        "input_params": {
            "query_from": query_from,
            "import_type": import_type,
            "search_re": search_re,
        },
        "source": source,
        "fingerprint": fingerprint,
        "raster": raster,
        "exif": exif_data,
        "gps": geotags,
        "captured_at": captured_at,
        "camera": camera_data,
        "sensor": sensor_data,
    }


def _get_exif_data(img):
    """Return EXIF metadata from a PIL image as a tag-name keyed dictionary."""
    exif_data = {}
    exif = img.getexif()
    if exif:
        for tag, value in exif.items():
            tag_name = TAGS.get(tag, tag)
            if tag_name == "GPSInfo":
                gps_info = {}
                if isinstance(value, dict):
                    gps_items = value.items()
                else:
                    try:
                        gps_items = exif.get_ifd(tag).items()
                    except Exception:
                        gps_items = ()

                for gps_tag, gps_value in gps_items:
                    gps_info[GPSTAGS.get(gps_tag, gps_tag)] = gps_value
                exif_data[tag_name] = gps_info
            else:
                exif_data[tag_name] = value
    return exif_data


def _get_raster_metadata(image_path):
    """Read GDAL raster metadata for an image."""
    dataset = gdal.Open(str(image_path))
    if dataset is None:
        raise ValueError(f"Unable to open image with GDAL: {image_path}")

    projection = dataset.GetProjectionRef()
    geotransform = dataset.GetGeoTransform(can_return_null=True)

    return {
        "width": dataset.RasterXSize,
        "height": dataset.RasterYSize,
        "bands": dataset.RasterCount,
        "projection": projection or None,
        "geotransform": tuple(geotransform) if geotransform is not None else None,
        "format": dataset.GetDriver().ShortName,
    }


def _fit_thumbnail_size(width, height, max_size):
    """Preserve aspect ratio while fitting within `max_size`."""
    max_width, max_height = max_size
    if width <= 0 or height <= 0:
        raise ValueError("Thumbnail source dimensions must be positive.")

    scale = min(max_width / width, max_height / height)
    scale = min(scale, 1.0)

    return max(1, int(round(width * scale))), max(1, int(round(height * scale)))


def _create_thumbnail_dataset(image_path, size=(256, 256)):
    """Create a low-resolution in-memory GDAL thumbnail dataset."""
    if gdal is None:
        warnings.warn(
            "GDAL is not installed. Thumbnails will be set to None.",
            stacklevel=2,
        )
        return None

    dataset = gdal.Open(str(image_path))
    if dataset is None:
        raise ValueError(f"Unable to open image with GDAL: {image_path}")

    thumb_width, thumb_height = _fit_thumbnail_size(
        dataset.RasterXSize,
        dataset.RasterYSize,
        size,
    )

    options = gdal.TranslateOptions(
        format="MEM",
        width=thumb_width,
        height=thumb_height,
        resampleAlg="lanczos",
    )
    thumbnail = gdal.Translate("", dataset, options=options)

    if thumbnail is None:
        raise ValueError(f"Failed to create thumbnail dataset for {image_path}")

    return thumbnail
