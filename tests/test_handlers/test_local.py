from pathlib import Path

from shapely.geometry import Point

from landlensdb.handlers.local import SearchLocalToGeoImageFrame


class FakeDbFilter:
    def __init__(self, kept_paths):
        self.kept_paths = [str(path) for path in kept_paths]
        self.calls = []

    def filter_existing_rows(self, image_paths):
        self.calls.append([str(path) for path in image_paths])
        return list(self.kept_paths)


def test_search_local_progress_callback(monkeypatch):
    discovered_paths = [Path("a.jpg"), Path("b.jpg"), Path("c.jpg")]
    progress_updates = []

    monkeypatch.setattr(
        SearchLocalToGeoImageFrame,
        "_discover_paths",
        classmethod(lambda cls, directory, pattern: list(discovered_paths)),
    )
    monkeypatch.setattr(
        SearchLocalToGeoImageFrame,
        "_load_task",
        staticmethod(
            lambda task: {
                "name": Path(task["image_path"]).name,
                "image_url": str(task["image_path"]),
                "geometry": Point(0, 0),
            }
        ),
    )

    images = SearchLocalToGeoImageFrame(
        "unused",
        create_thumbnail=False,
        progress_callback=lambda processed, total: progress_updates.append((processed, total)),
    )

    assert len(images) == 3
    assert progress_updates == [(0, 3), (1, 3), (2, 3), (3, 3)]


def test_search_local_skips_existing_rows(monkeypatch):
    discovered_paths = [Path("a.jpg"), Path("b.jpg"), Path("c.jpg")]
    fake_db = FakeDbFilter([Path("b.jpg")])
    loaded_paths = []

    monkeypatch.setattr(
        SearchLocalToGeoImageFrame,
        "_discover_paths",
        classmethod(lambda cls, directory, pattern: list(discovered_paths)),
    )
    monkeypatch.setattr(
        SearchLocalToGeoImageFrame,
        "_load_task",
        staticmethod(
            lambda task: loaded_paths.append(str(task["image_path"])) or {
                "name": Path(task["image_path"]).name,
                "image_url": str(task["image_path"]),
                "geometry": Point(0, 0),
            }
        ),
    )

    images = SearchLocalToGeoImageFrame(
        "unused",
        create_thumbnail=False,
        skip_images_in_postgresql=fake_db,
    )

    assert len(images) == 1
    assert loaded_paths == ["b.jpg"]
    assert fake_db.calls == [["a.jpg", "b.jpg", "c.jpg"]]
