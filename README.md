# landlensdb

Import geolocated photos and rasters, manage them in PostGIS, and query them from QGIS. The Python library also supports Mapillary imagery and road-network alignment.

[PyPI](https://pypi.org/project/landlensdb/) · [Documentation](docs/index.md) · [Examples](docs/examples/getting-started.ipynb)

## Install

```sh
pip install landlensdb
```

Requires Python 3.10+ and GDAL 3.5+. Database features require PostgreSQL 14+ with PostGIS 3.5+ and `postgis_raster`. See [installation](docs/installation.md).

## Import images

```python
from landlensdb import import_local_images, load_import_presets, parse_import_json

config = parse_import_json(load_import_presets()["geotagged_photos.json"])
config["file_glob"] = "/data/photos/**/*.@(jpg|JPG|png|PNG|jpeg|JPEG)"
images = import_local_images(config, output_crs="EPSG:4326", on_error="warn")
```

Paste a normal Windows path into **Search Glob**. In JSON source, backslashes must be doubled, for example `"file_glob": "C:\\Photos\\*.@(jpg|JPG|png|PNG|jpeg|JPEG)"`; JSON decoding restores the original string. The importer only quotes Windows backslashes for wcmatch, without detecting or rewriting paths. Forward slashes also work; add `**/` before `*` to include subfolders.

[JSON templates](landlensdb/examples) cover geotagged photos, georeferenced rasters, and WorldView-3 imagery. Each configuration requires `file_glob`, `name`, `image_url`, and `geometry`.

- Values such as `file.name`, `exif.Model`, and `sidecar.product.numColumns` resolve metadata paths; other values are literals. Dotted paths can index arrays.
- Geometry uses `point_from_exif`, `bounds_from_image`, or four named corners containing WGS84 `[longitude, latitude]` values or metadata paths. See the [WorldView template](landlensdb/examples/worldview3.json).
- Optional `sidecar_glob` substitutes `{parent}` and `{base}` and must match one JSON, GeoJSON, YAML, or WorldView IMD file.
- Fingerprinting is off by default; enable it with `"fingerprint": {"enabled": true}`. Output CRS, workers, batch size, and error handling are runtime arguments.

Imports return a `GeoImageFrame` with geometry, metadata, thumbnails, and the configuration plus its hash. Use `Postgres.upsert_images` for database writes and updates.

## QGIS plugin

Build with `make qgis-build`, then install `qgis_plugin_landlensdb.zip` through QGIS **Install from ZIP**.

- **Import Parameters** edits Search Glob; **Select** builds it from a folder, extension checkboxes, and optional **Recursive** search. **Advanced settings** opens the full JSON. Settings are saved in QGIS.
- Each row's **View JSON** opens a wrapping editor. **Copy** copies its text; **Update Import Text** saves it as the current QGIS Import Parameters.
- **Apply Template** loads a JSON file from the template folder. The list refreshes whenever the popup opens; editing settings does not edit templates.
- **Add** imports new images. The top **Actions** menu applies to the whole table; each row's menu applies to that import group.
- Selecting a table checks its required column names and types. Invalid tables cannot be imported into until their schema is corrected.
- Set threads, batch size, error handling, and output CRS below the table. CRS must match the destination table.
- **File Spatial Query** uses a vector file; **Select Bbox Query** lets you draw a rectangle on the map.

## Development

```sh
pip install -e '.[dev,docs]'
pre-commit install
pytest tests
mkdocs serve
```

Database tests need a local `landlens_test` database with PostGIS enabled. See [contributing](CONTRIBUTING.md). Licensed under [MIT](LICENSE.md).
