from landlensdb.geoclasses.geoimageframe import (
    GeoImageFrame,
    _generate_arrow_icon,
    _generate_arrow_svg,
)
from landlensdb.handlers.db import Postgres
from landlensdb.handlers.local import (
    GeoTaggedImage,
    GeoTransformImage,
    SearchLocalToGeoImageFrame,
)
from shapely.geometry import Point


def test_generate_arrow_icon():
    icon = _generate_arrow_icon(90)
    assert icon is not None, "Icon should not be None"


def test_generate_arrow_svg():
    svg_str = _generate_arrow_svg(45)
    assert svg_str is not None, "SVG string should not be None"


def test_geoimageframe_initialization(sample_data):
    gdf = GeoImageFrame(sample_data)
    assert gdf is not None, "GeoImageFrame should not be None"


def test_verify_structure(sample_geoimageframe):
    # Testing if structure is verified without error
    sample_geoimageframe._verify_structure()


def test_to_dict_records(sample_geoimageframe):
    records = sample_geoimageframe.to_dict_records()
    assert isinstance(records, list), "Should return a list"


def test_geoimageframe_accepts_metadata_column():
    gdf = GeoImageFrame(
        {
            "image_url": ["http://example.com/image.jpg"],
            "name": ["Sample"],
            "metadata": [{"source": {"path": "http://example.com/image.jpg"}}],
            "geometry": [Point(0, 0)],
        }
    )
    assert isinstance(gdf.at[0, "metadata"], dict)


def test_geotagged_image_loads_metadata_and_thumbnail_columns():
    images = SearchLocalToGeoImageFrame(
        "test_data/local",
        import_types={GeoTaggedImage: r".*\.JPG$"},
        create_thumbnail=False,
    )

    assert "metadata" in images.columns
    assert "thumbnail" in images.columns
    assert isinstance(images.iloc[0]["metadata"], dict)
    assert images.iloc[0]["thumbnail"] is None


def test_import_images_routes_to_importer_class():
    images = SearchLocalToGeoImageFrame(
        "test_data/local",
        import_types={GeoTaggedImage: r".*\.JPG$"},
        additional_columns=[("camera_model", "exif.Model")],
        create_thumbnail=False,
    )

    assert len(images) > 0
    assert "camera_model" in images.columns


def test_geotransform_image_is_publicly_importable():
    assert GeoTransformImage.__name__ == "GeoTransformImage"


def test_to_postgis_delegates_to_postgres_upsert_images(monkeypatch, sample_geoimageframe):
    captured = {}
    fake_engine = type(
        "FakeEngine",
        (),
        {"connect": lambda self: None},
    )()

    def fake_upsert(self, gif, table_name, conflict="update", if_exists="upsert", *args, **kwargs):
        captured["gif"] = gif
        captured["table_name"] = table_name
        captured["if_exists"] = if_exists
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(Postgres, "upsert_images", fake_upsert)

    result = sample_geoimageframe.to_postgis("images", fake_engine, if_exists="append", chunksize=100)

    assert result == "ok"
    assert captured["gif"] is sample_geoimageframe
    assert captured["table_name"] == "images"
    assert captured["if_exists"] == "append"
    assert captured["kwargs"] == {"chunksize": 100}
