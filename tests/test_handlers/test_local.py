from pathlib import Path

import yaml

from landlensdb.handlers.importer import import_local_images, resolve_sidecar
from landlensdb.import_config import (
    calculate_input_sha,
    import_yaml_to_function_params,
    load_example_import_yaml,
    load_import_presets,
    normalize_import_yaml,
)


def _example_params():
    params = import_yaml_to_function_params(load_example_import_yaml())
    params.update(
        source_file_glob=str(Path("test_data/local").resolve() / "**/*.jpg"),
        thumbnail_enabled=False,
    )
    return params


def test_normalized_yaml_removes_comments_and_has_stable_sha():
    first = """# comment\nsource:\n  file_glob: '/images/**/*.jpg'\nmetadata: {}\n"""
    second = """metadata: {}\nsource: {file_glob: '/images/**/*.jpg'}\n"""

    assert "# comment" not in normalize_import_yaml(first)
    assert normalize_import_yaml(first) == normalize_import_yaml(second)
    assert calculate_input_sha(first) == calculate_input_sha(second)


def test_yaml_keys_flatten_to_function_parameters_but_metadata_does_not():
    params = import_yaml_to_function_params(load_example_import_yaml())

    assert params["source_file_glob"] == "/path/to/photos/**/*.@(jpg|jpeg|png|tif|tiff)"
    assert params["geometry_latitude"] == "exif.GPSInfo.GPSLatitude"
    assert params["thumbnail_width"] == 256
    assert isinstance(params["metadata"], dict)
    assert "metadata_camera_model_source" not in params


def test_all_built_in_presets_map_to_import_function_parameters():
    presets = load_import_presets()

    assert list(presets) == [
        "Defaults",
        "Geotagged photos",
        "Georeferenced rasters",
        "WorldView-3 (TIL + IMD)",
    ]
    for value in presets.values():
        params = import_yaml_to_function_params(value)
        assert params["source_file_glob"].startswith("/path/to/")
        assert isinstance(params["metadata"], dict)

    worldview = import_yaml_to_function_params(presets["WorldView-3 (TIL + IMD)"])
    assert worldview["source_sidecar_glob"] == "{parent}/{base}.imd"
    assert worldview["geometry_min_x"] == "sidecar.bounds.min_x"


def test_parameterized_import_stores_canonical_params_and_sha():
    images = import_local_images(**_example_params())

    assert len(images) == 3
    assert set(("input_sha", "import_params", "metadata", "thumbnail")) <= set(
        images.columns
    )
    assert images["input_sha"].nunique() == 1
    assert images.iloc[0]["input_sha"] == calculate_input_sha(
        images.iloc[0]["import_params"]
    )
    assert "#" not in images.iloc[0]["import_params"]
    assert yaml.safe_load(images.iloc[0]["import_params"])["source"][
        "file_glob"
    ] == str(Path("test_data/local").resolve() / "**/*.jpg")


def test_progress_and_skip_existing_are_runtime_only():
    updates = []

    class FakeDbFilter:
        def filter_existing_rows(self, paths):
            return [str(paths[0])]

    images = import_local_images(
        **_example_params(),
        progress_callback=lambda processed, total: updates.append((processed, total)),
        skip_images_in_postgresql=FakeDbFilter(),
        skip_existing=True,
    )

    assert len(images) == 1
    assert updates[0] == (0, 1)
    stored = yaml.safe_load(images.iloc[0]["import_params"])
    assert set(stored["source"]) == {"file_glob"}
    assert "max_workers" not in stored
    assert "batch_size" not in stored
    assert "skip_existing" not in stored
    assert "on_error" not in stored
    assert "algorithm" not in stored["fingerprint"]


def test_sidecar_glob_rejects_more_than_one_file(tmp_path):
    image = tmp_path / "photo.jpg"
    image.write_bytes(b"image")
    (tmp_path / "photo.yml").write_text("one: 1", encoding="utf-8")
    (tmp_path / "photo.yaml").write_text("two: 2", encoding="utf-8")

    try:
        resolve_sidecar(image, "{parent}/{base}.@(yml|yaml)")
    except ValueError as exc:
        assert "more than one file" in str(exc)
    else:
        raise AssertionError("Multiple sidecars should be rejected")


def test_sidecar_glob_requires_one_file_when_configured(tmp_path):
    image = tmp_path / "photo.jpg"
    image.write_bytes(b"image")

    try:
        resolve_sidecar(image, "{parent}/{base}.json")
    except ValueError as exc:
        assert "matched no file" in str(exc)
    else:
        raise AssertionError("A configured sidecar glob must match one file")


def test_worldview_imd_sidecar_is_converted_to_json_notation(tmp_path):
    image = tmp_path / "scene.TIL"
    image.write_bytes(b"image")
    (tmp_path / "scene.IMD").write_text(
        """generationTime = \"2024-01-01T00:00:00Z\";
BEGIN_GROUP = IMAGE_1
  satId = \"WV03\";
  firstLineTime = \"2024-01-01T01:02:03Z\";
END_GROUP = IMAGE_1
BEGIN_GROUP = BAND_P
  ULLon = -158.2;
  ULLat = 21.7;
  URLon = -157.8;
  URLat = 21.7;
  LRLon = -157.8;
  LRLat = 21.3;
  LLLon = -158.2;
  LLLat = 21.3;
END_GROUP = BAND_P
END;
""",
        encoding="utf-8",
    )

    sidecar = resolve_sidecar(image, "{parent}/{base}.imd")

    assert sidecar["product"]["generationTime"] == "2024-01-01T00:00:00Z"
    assert sidecar["image"]["satId"] == "WV03"
    assert sidecar["bounds"]["min_x"] == -158.2
    assert sidecar["bounds"]["max_y"] == 21.7


def test_unsupported_sidecar_format_is_rejected(tmp_path):
    image = tmp_path / "photo.jpg"
    image.write_bytes(b"image")
    (tmp_path / "photo.txt").write_text("not: accepted", encoding="utf-8")

    try:
        resolve_sidecar(image, "{parent}/{base}.txt")
    except ValueError as exc:
        assert "supported formats" in str(exc)
        assert ".imd" in str(exc)
    else:
        raise AssertionError("Unsupported sidecars should be rejected")


def test_on_error_error_propagates_import_failures(tmp_path):
    (tmp_path / "invalid.jpg").write_bytes(b"not an image")

    params = _example_params()
    params.update(
        source_file_glob=str(tmp_path / "*.jpg"),
        geometry_required=True,
    )

    try:
        import_local_images(**params, on_error="error")
    except ValueError as exc:
        assert "Required geometry" in str(exc)
    else:
        raise AssertionError("on_error='error' must propagate per-file errors")
