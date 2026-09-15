"""Source and exact-path browse thumbnails, independent of metadata sidecars."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from PIL import Image

from landlensdb.handlers import importer
from landlensdb.import_config import validate_import_config


@pytest.fixture
def config(tmp_path):
    image = tmp_path / "scene.TIF"
    Image.new("RGB", (40, 20), "red").save(image)
    return {
        "file_glob": str(image),
        "name": "file.name",
        "image_url": "file.path",
        "geometry": {
            "upper_left": [0, 1],
            "upper_right": [1, 1],
            "lower_right": [1, 0],
            "lower_left": [0, 0],
        },
        "metadata": {"sidecar_path": "./missing.json", "value": "sidecar.value"},
        "thumbnail": {
            "enabled": "source",
            "width": 10,
            "height": 10,
            "resampling": "nearest",
        },
    }


@pytest.mark.parametrize("mode", ["source", True, False, "sidecar"])
def test_thumbnail_modes_read_the_selected_file(config, tmp_path, mode):
    config["thumbnail"]["enabled"] = mode
    Image.new("RGB", (16, 32), "blue").save(tmp_path / "scene-BROWSE.JPG")
    row = importer.import_local_images(config, on_error="error").iloc[0]
    assert row["metadata"] == {"value": None}
    thumbnail = row["thumbnail"]
    if mode is False:
        assert thumbnail is None
    elif mode == "sidecar":
        assert (thumbnail.RasterXSize, thumbnail.RasterYSize) == (5, 10)
        red, green, blue = thumbnail.ReadAsArray()[:, 0, 0]
        assert blue > 200 and red < 10 and green < 10
    else:
        assert (thumbnail.RasterXSize, thumbnail.RasterYSize) == (10, 5)
        assert tuple(thumbnail.ReadAsArray()[:, 0, 0]) == (255, 0, 0)


def test_thumbnail_sidecar_has_default_dimensions(config, tmp_path):
    config["thumbnail"] = {"enabled": "sidecar"}
    Image.new("RGB", (600, 300)).save(tmp_path / "scene-BROWSE.JPG")
    thumbnail = importer.import_local_images(config, on_error="error").iloc[0][
        "thumbnail"
    ]
    assert (thumbnail.RasterXSize, thumbnail.RasterYSize) == (256, 128)


def test_thumbnail_sidecar_uses_same_relative_solver(config, tmp_path, monkeypatch):
    image = tmp_path / "images/scene.TIF"
    image.parent.mkdir()
    image.write_bytes(b"source must not be decoded")
    browse = tmp_path / "previews/scene.jpg"
    browse.parent.mkdir()
    Image.new("RGB", (24, 12)).save(browse)
    (image.parent / "metadata.json").write_text('{"value": 42}')
    config["file_glob"] = str(image)
    config["metadata"]["sidecar_path"] = "./metadata.json"
    config["thumbnail"].update(enabled="sidecar", sidecar_path="../previews/{base}.jpg")
    forbidden = Mock(side_effect=AssertionError("source image was opened"))
    monkeypatch.setattr(importer.Image, "open", forbidden)
    monkeypatch.setattr(importer, "_get_raster_metadata", forbidden)
    row = importer.import_local_images(config, on_error="error").iloc[0]
    assert row["metadata"] == {"value": 42}
    assert row["thumbnail"].RasterXSize == 10
    forbidden.assert_not_called()


def test_missing_browse_does_not_read_source_or_metadata_parser(config, monkeypatch):
    config["thumbnail"]["enabled"] = "sidecar"
    forbidden = Mock(side_effect=AssertionError("missing browse must not be read"))
    monkeypatch.setattr(importer, "_create_thumbnail_dataset", forbidden)
    monkeypatch.setitem(importer.SIDECAR_LOADERS, ".json", forbidden)
    row = importer.import_local_images(config, on_error="error").iloc[0]
    assert row["thumbnail"] is None
    assert row["metadata"] == {"value": None}
    forbidden.assert_not_called()


@pytest.mark.parametrize("value", [None, 0, 1, [], {}, "", "true", "browse"])
def test_invalid_thumbnail_modes_are_rejected(config, value):
    config["thumbnail"]["enabled"] = value
    with pytest.raises(ValueError, match="thumbnail.enabled"):
        validate_import_config(config)


@pytest.mark.parametrize("path", ["*.JPG", "{parent}/{base}.JPG", "/browse.JPG"])
def test_thumbnail_path_uses_metadata_path_validation(config, path):
    config["thumbnail"].update(enabled="sidecar", sidecar_path=path)
    with pytest.raises(ValueError, match="sidecar_path"):
        validate_import_config(config)


def test_source_mode_does_not_accept_ignored_sidecar_setting(config):
    config["thumbnail"]["sidecar_path"] = "./browse.JPG"
    with pytest.raises(ValueError, match="requires"):
        validate_import_config(config)


def test_only_top_level_metadata_sidecar_path_is_reserved(config, tmp_path):
    (tmp_path / "scene.json").write_text('{"value": 42}')
    config["metadata"] = {
        "sidecar_path": "./{base}.json",
        "value": "sidecar.value",
        "nested": {"sidecar_path": "ordinary user metadata"},
    }
    row = importer.import_local_images(config, on_error="error").iloc[0]
    assert row["metadata"] == {
        "value": 42,
        "nested": {"sidecar_path": "ordinary user metadata"},
    }


def test_top_level_metadata_sidecar_setting_reports_new_location(config):
    config["sidecar_path"] = config["metadata"].pop("sidecar_path")
    with pytest.raises(ValueError, match="metadata.sidecar_path"):
        validate_import_config(config)
