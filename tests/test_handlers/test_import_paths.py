"""Keep the discovered path as the image identifier, including mapped drives."""

from pathlib import PureWindowsPath

import pytest

from landlensdb.handlers import importer


def config(pattern):
    return {
        "file_glob": pattern,
        "name": "file.name",
        "image_url": "file.path",
        "geometry": {
            "upper_left": [0, 1],
            "upper_right": [1, 1],
            "lower_right": [1, 0],
            "lower_left": [0, 0],
        },
        "metadata": {"name": "file.name", "image_url": "file.path"},
        "thumbnail": {"enabled": False},
    }


@pytest.mark.parametrize(
    "path",
    [
        r"S:\Satellite_Imagery\Big_Island\scene.TIL",
        r"\\IMAGERY\SatelliteImagery\Satellite_Imagery\Big_Island\scene.TIL",
    ],
)
def test_windows_discovery_preserves_the_glob_path_in_row_and_metadata(
    monkeypatch, path
):
    class MatchedWindowsPath(PureWindowsPath):
        def is_file(self):
            return True

        def absolute(self):
            assert self.is_absolute()
            return self

        def resolve(self):
            pytest.fail("Discovery must not expand drive mappings or resolve links")

    monkeypatch.setattr(importer, "Path", MatchedWindowsPath)
    monkeypatch.setattr(
        importer.wcglob, "iglob", lambda *args, **kwargs: iter([path, path])
    )
    images = importer.import_local_images(config(path), on_error="error")
    assert len(images) == 1
    assert images.iloc[0]["image_url"] == path
    assert images.iloc[0]["metadata"]["image_url"] == path


def test_relative_glob_keeps_link_name_and_adjacent_sidecar(tmp_path, monkeypatch):
    original = tmp_path / "original.jpg"
    original.write_bytes(b"Metadata-only import")
    linked = tmp_path / "selected.jpg"
    linked.symlink_to(original)
    linked.with_suffix(".json").write_text('{"value": "beside selected file"}')
    monkeypatch.chdir(tmp_path)
    settings = config("./selected*.jpg")
    settings["metadata"].update(sidecar_path="./{base}.json", value="sidecar.value")
    row = importer.import_local_images(settings, on_error="error").iloc[0]
    assert row["name"] == "selected.jpg"
    assert row["image_url"] == str(linked)
    assert row["metadata"]["image_url"] == str(linked)
    assert row["metadata"]["value"] == "beside selected file"
