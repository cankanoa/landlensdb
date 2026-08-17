"""Import parameter serialization shared by Python, PostgreSQL, and QGIS."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

import yaml

EXAMPLE_IMPORT_PARAMS_PATH = Path(__file__).with_name("examples") / "import_params.yaml"
IMPORT_PRESET_PATHS = {
    "Defaults": EXAMPLE_IMPORT_PARAMS_PATH,
    "Geotagged photos": Path(__file__).with_name("examples") / "geotagged_photos.yaml",
    "Georeferenced rasters": Path(__file__).with_name("examples")
    / "georeferenced_rasters.yaml",
    "WorldView-3 (TIL + IMD)": Path(__file__).with_name("examples") / "worldview3.yaml",
}


def load_example_import_yaml() -> str:
    """Return the commented import template used by the QGIS editor."""
    return EXAMPLE_IMPORT_PARAMS_PATH.read_text(encoding="utf-8")


def load_import_presets() -> dict[str, str]:
    """Return the built-in quick-import presets in display order."""
    return {
        name: path.read_text(encoding="utf-8")
        for name, path in IMPORT_PRESET_PATHS.items()
    }


def parse_import_yaml(value: str | Mapping[str, Any]) -> dict[str, Any]:
    """Parse YAML or copy an existing mapping and require a mapping root."""
    if isinstance(value, str):
        parsed = yaml.safe_load(value)
    elif isinstance(value, Mapping):
        parsed = dict(value)
    else:
        raise TypeError("Import parameters must be YAML text or a mapping.")
    if not isinstance(parsed, dict):
        raise ValueError("Import parameter YAML must contain a mapping at its root.")
    return parsed


def normalize_import_yaml(value: str | Mapping[str, Any]) -> str:
    """Return stable YAML without comments or formatting differences."""
    parsed = parse_import_yaml(value)
    return yaml.safe_dump(
        parsed,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
        width=4096,
    )


def calculate_input_sha(value: str | Mapping[str, Any]) -> str:
    """Return the SHA-256 digest of normalized import parameters."""
    normalized = normalize_import_yaml(value)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _flatten_mapping(mapping: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in mapping.items():
        parameter_name = "{}_{}".format(prefix, key) if prefix else str(key)
        if isinstance(value, Mapping):
            flattened.update(_flatten_mapping(value, parameter_name))
        else:
            flattened[parameter_name] = value
    return flattened


def import_yaml_to_function_params(value: str | Mapping[str, Any]) -> dict[str, Any]:
    """Translate nested YAML keys to flat Python keyword arguments.

    ``metadata`` is deliberately preserved as one nested mapping. It is the
    only import argument whose structure is user-defined.
    """
    parsed = parse_import_yaml(value)
    metadata = parsed.pop("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("`metadata` must be a mapping.")
    parameters = _flatten_mapping(parsed)
    parameters["metadata"] = metadata
    return parameters


def build_import_params_mapping(
    *,
    source_file_glob: str,
    source_sidecar_glob: str | None,
    name_source: str,
    name_required: bool,
    name_default: Any,
    image_url_source: str,
    image_url_required: bool,
    geometry_source: str,
    geometry_output_crs: str,
    geometry_required: bool,
    geometry_latitude: str,
    geometry_latitude_reference: str,
    geometry_longitude: str,
    geometry_longitude_reference: str,
    geometry_footprint: str,
    geometry_min_x: str | None,
    geometry_min_y: str | None,
    geometry_max_x: str | None,
    geometry_max_y: str | None,
    geometry_input_crs: str | None,
    metadata: Mapping[str, Any],
    thumbnail_enabled: bool,
    thumbnail_width: int,
    thumbnail_height: int,
    thumbnail_resampling: str,
    fingerprint_enabled: bool,
    fingerprint_mode: str,
) -> dict[str, Any]:
    """Build the effective nested import configuration from function args."""
    source = {"file_glob": source_file_glob}
    if source_sidecar_glob:
        source["sidecar_glob"] = source_sidecar_glob

    geometry: dict[str, Any] = {
        "source": geometry_source,
        "output_crs": geometry_output_crs,
        "required": bool(geometry_required),
    }
    if geometry_source == "point_from_exif":
        geometry.update(
            {
                "latitude": geometry_latitude,
                "latitude_reference": geometry_latitude_reference,
                "longitude": geometry_longitude,
                "longitude_reference": geometry_longitude_reference,
            }
        )
    elif geometry_source == "bounds_from_image":
        geometry["footprint"] = geometry_footprint
    elif geometry_source == "bounds_from_sidecar":
        geometry.update(
            {
                "min_x": geometry_min_x,
                "min_y": geometry_min_y,
                "max_x": geometry_max_x,
                "max_y": geometry_max_y,
                "input_crs": geometry_input_crs,
            }
        )

    return {
        "source": source,
        "name": {
            "source": name_source,
            "required": bool(name_required),
            "default": name_default,
        },
        "image_url": {
            "source": image_url_source,
            "required": bool(image_url_required),
        },
        "geometry": geometry,
        "metadata": dict(metadata),
        "thumbnail": {
            "enabled": bool(thumbnail_enabled),
            "width": int(thumbnail_width),
            "height": int(thumbnail_height),
            "resampling": thumbnail_resampling,
        },
        "fingerprint": {
            "enabled": bool(fingerprint_enabled),
            "mode": fingerprint_mode,
        },
    }
