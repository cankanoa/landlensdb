# Sidecar lookup and local import performance audit

## Path lookup

Use `sidecar_path` relative to each discovered image's directory. The only
placeholder is `{base}`, the image filename without its final extension.
Examples: `./{base}.json`, `./metadata.yaml`, `../metadata/{base}.json`, and
`../../{base}.IMD`. The WorldView template uses `./{base}.IMD`.

Each lookup substitutes `{base}`, joins the path, and performs one `is_file()`
check. Only a present regular file is opened and parsed. There is no glob,
directory enumeration, sorting, or Python `resolve()` walk. The filesystem
handles parent directories and symlinks, preserving the meaning of `..` after
a symlink. Filename case follows the filesystem; there is no extension search.

A missing file leaves sidecar metadata empty and the image import continues.
Required geometry, name, or URL values must still resolve; their failures and
invalid sidecar contents follow the configured `on_error` policy.

Validation is cached by template in a bounded cache. File existence and parsed
contents are checked afresh, so edits and newly created sidecars are visible.
The old `sidecar_glob` option reports a migration error with a `sidecar_path`
example.

## Other work removed from the import loop

- Determine required metadata sources once per import, including geometry,
  name, URL, and nested metadata objects and arrays. Open images through Pillow
  for EXIF or GDAL for raster metadata only when those sources are needed.
- Read file size and timestamps only when those fields are referenced.
- Initialize timezone lookup data once per worker that needs it, instead of
  once per photo. Each worker has its own instance.
- Hash the already normalized import JSON rather than validating and
  serializing it again to calculate the input hash.
- Avoid the unused file-size check for robust fingerprints.

Image discovery still uses `file_glob`. Enabled thumbnails still require image
processing; robust fingerprints still read the complete file. These operations
can dominate total import time for large imagery. Disable thumbnails and
fingerprints for imports that need only metadata and footprints. Large imports
also retain discovered paths and submitted batches in memory; this audit does
not change their scheduling or output buffering.

## Local measurement

Measured on macOS 26.2 (arm64), Python 3.10.20, against the resolver from commit
`80091d6`. Both versions used the same temporary directory and a small JSON
sidecar with nested metadata. The baseline used `{parent}/{base}.json` and the
current implementation used `./{base}.json`. For absent files, the baseline's
missing-file exception was caught to continue the benchmark.

Times below are microseconds per lookup, including parsing when present, using
the median of three warmed runs (10 lookups per baseline run and 1,000 per new
run). Neighboring files were unrelated `.txt` files. These measurements describe
this local filesystem and cache state, not an end-to-end import speed guarantee.

| Neighboring files | Sidecar | Old glob (µs) | Direct path (µs) |
| ---: | --- | ---: | ---: |
| 0 | Missing | 3,225.32 | 5.43 |
| 0 | Present | 2,992.35 | 36.75 |
| 1,000 | Missing | 4,792.05 | 5.30 |
| 1,000 | Present | 4,456.08 | 72.47 |
| 10,000 | Missing | 18,064.55 | 5.34 |
| 10,000 | Present | 17,768.84 | 41.27 |

Regression tests assert exactly one file check and zero directory-search or
`resolve()` calls per sidecar lookup. They also cover adjacent and ancestor
paths, symlinks, literal special characters in image basenames, missing and
changing files, parsing failures, threaded imports, metadata source selection,
and timezone instance reuse. These operation-count tests protect the lookup
behavior independently of timing noise.

Run the offline library suite with:

```sh
python -m pytest tests --ignore=tests/test_tutorial_core.py
```

The excluded tutorial module also contains live PostgreSQL, Mapillary, and road
network integration examples that need their external services.
