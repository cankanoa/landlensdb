"""Low-level local image helpers used by the parameter-driven importer."""

import numbers
import warnings

from osgeo import gdal
from PIL.ExifTags import GPSTAGS, TAGS


class ImportCancelledError(Exception):
    """Raised when a local image import is cancelled by the user."""


def _normalize_metadata_value(value):
    """Convert metadata values into Python-native, JSON-friendly objects."""
    if isinstance(value, dict):
        return {str(key): _normalize_metadata_value(item) for key, item in value.items()}
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


def _to_decimal(value):
    """Convert an EXIF DMS tuple or ratio notation into decimal degrees."""
    if isinstance(value, list):
        value = tuple(value)
    if isinstance(value, tuple) and len(value) == 3:
        return float(value[0]) + float(value[1]) / 60 + float(value[2]) / 3600
    if isinstance(value, str) and "/" in value:
        numerator, denominator = value.split("/", 1)
        if float(denominator) == 0:
            return None
        return float(numerator) / float(denominator)
    return value


def _get_exif_data(image):
    """Return EXIF metadata as a tag-name keyed dictionary."""
    values = {}
    exif = image.getexif()
    if not exif:
        return values
    for tag, value in exif.items():
        tag_name = TAGS.get(tag, tag)
        if tag_name != "GPSInfo":
            values[tag_name] = value
            continue
        if isinstance(value, dict):
            gps_items = value.items()
        else:
            try:
                gps_items = exif.get_ifd(tag).items()
            except Exception:
                gps_items = ()
        values[tag_name] = {
            GPSTAGS.get(gps_tag, gps_tag): gps_value
            for gps_tag, gps_value in gps_items
        }
    return values


def _get_raster_metadata(image_path):
    """Read raster dimensions, projection, geotransform, and format."""
    dataset = gdal.Open(str(image_path))
    if dataset is None:
        raise ValueError("Unable to open image with GDAL: {}".format(image_path))
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
    """Preserve aspect ratio while fitting within ``max_size``."""
    max_width, max_height = max_size
    if width <= 0 or height <= 0:
        raise ValueError("Thumbnail source dimensions must be positive.")
    scale = min(max_width / width, max_height / height, 1.0)
    return max(1, round(width * scale)), max(1, round(height * scale))


def _create_thumbnail_dataset(image_path, size=(256, 256), resampling="lanczos"):
    """Create a low-resolution in-memory GDAL thumbnail dataset."""
    dataset = gdal.Open(str(image_path))
    if dataset is None:
        warnings.warn(
            "Unable to open {} with GDAL; thumbnail is null.".format(image_path),
            stacklevel=2,
        )
        return None
    width, height = _fit_thumbnail_size(
        dataset.RasterXSize,
        dataset.RasterYSize,
        size,
    )
    thumbnail = gdal.Translate(
        "",
        dataset,
        options=gdal.TranslateOptions(
            format="MEM",
            width=width,
            height=height,
            resampleAlg=resampling,
        ),
    )
    if thumbnail is None:
        raise ValueError("Failed to create thumbnail for {}".format(image_path))
    return thumbnail
