"""Compact JSON import configurations shared by Python, PostgreSQL, and QGIS."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

IMPORT_TEMPLATE_DIRECTORY = Path(__file__).with_name("examples")
REQUIRED_FIELDS = ("file_glob", "name", "image_url", "geometry")
GEOMETRY_MODES = {"point_from_exif", "bounds_from_image"}
GEOMETRY_CORNERS = ("upper_left", "upper_right", "lower_right", "lower_left")


def load_example_import_json() -> str:
    """Use the first available template when no configuration has been saved."""
    return next(iter(load_import_presets().values()), "{}")


def load_import_presets() -> dict[str, str]:
    """Discover the current JSON files on every call, using filenames as labels."""
    return {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(
            IMPORT_TEMPLATE_DIRECTORY.glob("*"), key=lambda path: path.name.casefold()
        )
        if path.is_file() and path.suffix.lower() == ".json"
    }


def validate_import_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Require the compact model and reject unsupported configuration options."""
    if not isinstance(config, Mapping):
        raise ValueError("Import configuration must be a JSON object.")
    allowed = set(REQUIRED_FIELDS) | {
        "sidecar_glob",
        "metadata",
        "thumbnail",
        "fingerprint",
    }
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(
            "Unknown import options: {}".format(", ".join(sorted(unknown)))
        )
    for key in ("file_glob", "name", "image_url"):
        if not isinstance(config.get(key), str) or not config[key].strip():
            raise ValueError("`{}` must be a non-empty string.".format(key))
    geometry = config.get("geometry")
    if isinstance(geometry, dict):
        if set(geometry) != set(GEOMETRY_CORNERS):
            raise ValueError(
                "`geometry` must contain exactly these four corners: {}.".format(
                    ", ".join(GEOMETRY_CORNERS)
                )
            )
        for corner in GEOMETRY_CORNERS:
            pair = geometry[corner]
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError(
                    "`geometry.{}` must be [longitude, latitude].".format(corner)
                )
            for value in pair:
                if not (
                    type(value) in (int, float)
                    or isinstance(value, str)
                    and value.strip()
                ):
                    raise ValueError(
                        "`geometry.{}` coordinates must be numbers or metadata paths.".format(
                            corner
                        )
                    )
                if (
                    isinstance(value, str)
                    and value.startswith("sidecar.")
                    and not config.get("sidecar_glob")
                ):
                    raise ValueError("Sidecar geometry paths require `sidecar_glob`.")
    elif not isinstance(geometry, str) or geometry not in GEOMETRY_MODES:
        raise ValueError("Unsupported geometry: {!r}.".format(geometry))
    if "sidecar_glob" in config and (
        not isinstance(config["sidecar_glob"], str)
        or not config["sidecar_glob"].strip()
    ):
        raise ValueError("`sidecar_glob` must be a non-empty string.")
    for key in ("metadata", "thumbnail", "fingerprint"):
        if key in config and not isinstance(config[key], dict):
            raise ValueError("`{}` must be a JSON object.".format(key))
    for key, options in (
        ("thumbnail", {"enabled", "width", "height", "resampling"}),
        ("fingerprint", {"enabled", "mode"}),
    ):
        section = config.get(key, {})
        if set(section) - options:
            raise ValueError("Unknown {} options.".format(key))
        if "enabled" in section and not isinstance(section["enabled"], bool):
            raise ValueError("`{}.enabled` must be a boolean.".format(key))
    thumbnail = config.get("thumbnail", {})
    for key in ("width", "height"):
        if key in thumbnail and (type(thumbnail[key]) is not int or thumbnail[key] < 1):
            raise ValueError("`thumbnail.{}` must be a positive integer.".format(key))
    if "resampling" in thumbnail and (
        not isinstance(thumbnail["resampling"], str)
        or not thumbnail["resampling"].strip()
    ):
        raise ValueError("`thumbnail.resampling` must be a non-empty string.")
    if config.get("fingerprint", {}).get("mode", "robust") not in {"robust", "quick"}:
        raise ValueError("`fingerprint.mode` must be 'robust' or 'quick'.")
    # Snapshot the parsed JSON so callers cannot change a running import's config.
    return json.loads(json.dumps(dict(config), allow_nan=False))


def parse_import_json(text: str) -> dict[str, Any]:
    """Parse and validate one JSON configuration; YAML is not accepted."""
    return validate_import_config(json.loads(text))


def normalize_import_json(value: str | Mapping[str, Any]) -> str:
    config = (
        parse_import_json(value)
        if isinstance(value, str)
        else validate_import_config(value)
    )
    return json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def calculate_input_sha(value: str | Mapping[str, Any]) -> str:
    """Hash the JSON configuration independently of whitespace and key order."""
    return hashlib.sha256(normalize_import_json(value).encode("utf-8")).hexdigest()
