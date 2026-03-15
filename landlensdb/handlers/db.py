import json
import math
import numbers

from geoalchemy2 import WKBElement
from shapely.geometry.base import BaseGeometry
from shapely.wkb import loads
from sqlalchemy import create_engine, MetaData, Table, select, and_, text, update
from sqlalchemy.dialects.postgresql import insert

from ..geoclasses.geoimageframe import GeoImageFrame


class Postgres:
    """
    A class for managing image-related postgres database operations.

    Attributes:
        DATABASE_URL (str): The URL of the database to connect to.
        engine (Engine): SQLAlchemy engine for database connections.
        result_set (ResultProxy): The result of the last query executed.
        selected_table (Table): The table object for query operations.
    """

    def __init__(self, database_url):
        """
        Initializes the ImageDB class with the given database URL.

        Args:
            database_url (str): The URL of the database to connect to.
        """
        self.DATABASE_URL = database_url
        self.engine = create_engine(self.DATABASE_URL)
        self.result_set = None
        self.selected_table = None

    @staticmethod
    def _convert_geometries_to_wkt(record):
        """
        Converts Shapely geometries to WKT (Well-Known Text) format.

        Args:
            record (dict): A dictionary containing keys and values, where values can be Shapely geometries.

        Returns:
            dict: The record with geometry objects converted to WKT strings.
        """
        for key, value in record.items():
            if isinstance(value, BaseGeometry):
                record[key] = value.wkt
        return record

    @staticmethod
    def _convert_dicts_to_json(record):
        """
        Normalize records so JSON/JSONB values contain only JSON-serializable types.

        Args:
            record (dict): A dictionary where values may include other dictionaries.

        Returns:
            dict: The normalized record.
        """
        def _clean_string(value):
            return value.replace("\x00", "")

        def _normalize_json_value(value):
            if isinstance(value, dict):
                return {
                    _clean_string(str(key)): _normalize_json_value(item)
                    for key, item in value.items()
                }
            if isinstance(value, (list, tuple, set)):
                return [_normalize_json_value(item) for item in value]
            if isinstance(value, str):
                return _clean_string(value)
            if value is None or isinstance(value, bool):
                return value
            if isinstance(value, BaseGeometry):
                return value.wkt
            if hasattr(value, "item"):
                try:
                    return _normalize_json_value(value.item())
                except Exception:
                    pass
            if hasattr(value, "tolist"):
                try:
                    return _normalize_json_value(value.tolist())
                except Exception:
                    pass
            if hasattr(value, "numerator") and hasattr(value, "denominator"):
                try:
                    return _normalize_json_value(float(value))
                except Exception:
                    pass
            if isinstance(value, numbers.Integral):
                return int(value)
            if isinstance(value, numbers.Real):
                numeric_value = float(value)
                return numeric_value if math.isfinite(numeric_value) else None
            if isinstance(value, bytes):
                return _clean_string(value.decode("utf-8", errors="replace"))
            return _clean_string(str(value))

        def _default_json(value):
            return _normalize_json_value(value)

        normalized_record = _normalize_json_value(record)
        return json.loads(json.dumps(normalized_record, default=_default_json, allow_nan=False))

    def table(self, table_name):
        """
        Selects a table for performing queries on.

        Args:
            table_name (str): Name of the table to select.

        Returns:
            ImageDB: Returns self to enable method chaining.
        """
        metadata = MetaData()
        self.selected_table = Table(table_name, metadata, autoload_with=self.engine)
        self.result_set = self.selected_table.select()
        return self

    def filter(self, **kwargs):
        """
        Applies filters to the selected table based on provided conditions.

        Args:
            **kwargs: Key-value pairs representing filters to apply.

        Returns:
            ImageDB: Returns self to enable method chaining.

        Raises:
            ValueError: If an unsupported operation or a nonexistent column is specified.
        """
        filters = []

        for k, v in kwargs.items():
            if "__" in k:
                field_name, operation = k.split("__", 1)
            else:
                field_name = k
                operation = "eq"

            column = getattr(self.selected_table.columns, field_name, None)
            if column is None:
                raise ValueError(
                    f"Column '{field_name}' not found in table '{self.selected_table.name}'"
                )

            if operation == "eq":
                filters.append(column == v)
            elif operation == "gt":
                filters.append(column > v)
            elif operation == "lt":
                filters.append(column < v)
            elif operation == "gte":
                filters.append(column >= v)
            elif operation == "lte":
                filters.append(column <= v)
            else:
                raise ValueError(f"Unsupported operation '{operation}'")

        self.result_set = self.result_set.where(and_(*filters))
        return self

    def all(self):
        """
        Executes the query and returns the result as a GeoImageFrame.

        Returns:
            GeoImageFrame: The result of the query as a GeoImageFrame object.

        Raises:
            TypeError: If geometries are not of type Point.
        """
        with self.engine.connect() as conn:
            result = conn.execute(self.result_set)
            data = [row._asdict() for row in result.fetchall()]

        if not data:
            return GeoImageFrame([])  # Adjust according to your GeoImageFrame handling

        df_data = {col: [] for col in data[0].keys()}

        for d in data:
            for col, value in d.items():
                if isinstance(value, WKBElement):
                    try:
                        point_geom = loads(
                            bytes(value.data)
                        )  # convert WKBElement to Shapely geometry
                        if point_geom.geom_type != "Point":
                            raise TypeError("All geometries must be of type Point.")
                        df_data[col].append(point_geom)
                    except Exception as e:
                        print(f"Failed to process data {value.data}. Error: {e}")
                else:
                    df_data[col].append(value)

        return GeoImageFrame(df_data)

    def get_distinct_values(self, table_name, column_name):
        """
        Gets distinct values from a specific column of a table.

        Args:
            table_name (str): Name of the table to query.
            column_name (str): Name of the column to get distinct values from.

        Returns:
            list: A list of distinct values from the specified column.

        Raises:
            ValueError: If the specified column is not found in the table.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)

        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table = metadata.tables[table_name]

        if column_name not in table.columns:
            raise ValueError(
                f"Column '{column_name}' not found in table '{table_name}'"
            )

        column = table.columns[column_name]

        distinct_query = select(column).distinct()
        with self.engine.connect() as conn:
            result = conn.execute(distinct_query)

        distinct_values = [row[0] for row in result.fetchall()]
        return distinct_values

    @staticmethod
    def _qualified_table_name(table):
        if table.schema:
            return '"{}"."{}"'.format(table.schema, table.name)
        return '"{}"'.format(table.name)

    def upsert_images(self, gif, table_name, conflict="update"):
        """
        Inserts or updates image data in the specified table.

        Args:
            gif (GeoImageFrame): The data frame containing image data.
            table_name (str): The name of the table to upsert into.
            conflict (str, optional): Conflict resolution strategy ("update" or "nothing"). Defaults to "update".

        Raises:
            ValueError: If an invalid conflict resolution type is provided.
        """
        data = gif.to_dict(orient="records")

        meta = MetaData()
        table = Table(table_name, meta, autoload_with=self.engine)
        thumbnail_updates = []

        with self.engine.begin() as conn:
            geometry_column = table.columns.get("geometry")
            if geometry_column is not None:
                current_geometry_type = getattr(geometry_column.type, "geometry_type", None)
                current_geometry_type = (
                    current_geometry_type.upper() if isinstance(current_geometry_type, str) else None
                )
                if current_geometry_type not in (None, "GEOMETRY"):
                    conn.execute(
                        text(
                            "ALTER TABLE {} "
                            "ALTER COLUMN geometry TYPE geometry(Geometry, 4326) "
                            "USING ST_SetSRID(geometry, 4326)".format(
                                self._qualified_table_name(table)
                            )
                        )
                    )
            for record in data:
                thumbnail_value = record.pop("thumbnail", None)
                record = self._convert_geometries_to_wkt(record)
                record = self._convert_dicts_to_json(record)
                fingerprint_value = record.get("fingerprint")

                if conflict == "update" and fingerprint_value and "fingerprint" in table.columns:
                    updates = {
                        key: value
                        for key, value in record.items()
                    }
                    fingerprint_update = (
                        update(table)
                        .where(table.c.fingerprint == fingerprint_value)
                        .values(**updates)
                    )
                    fingerprint_result = conn.execute(fingerprint_update)
                    if fingerprint_result.rowcount:
                        continue
                elif conflict == "nothing" and fingerprint_value and "fingerprint" in table.columns:
                    fingerprint_exists = conn.execute(
                        select(table.c.fingerprint).where(table.c.fingerprint == fingerprint_value)
                    ).first()
                    if fingerprint_exists:
                        continue

                insert_stmt = insert(table).values(**record)
                if conflict == "update":
                    updates = {
                        key: getattr(insert_stmt.excluded, key)
                        for key in record
                        if key != "image_url"
                    }
                    constraint_name = f"{table.name}_image_url_key"
                    on_conflict_stmt = insert_stmt.on_conflict_do_update(
                        constraint=constraint_name,
                        set_=updates
                    )
                elif conflict == "nothing":
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
                else:
                    raise ValueError(
                        "Invalid conflict resolution type. Choose 'update' or 'nothing'."
                    )

                conn.execute(on_conflict_stmt)

                if thumbnail_value is not None:
                    thumbnail_updates.append(
                        {
                            "image_url": record["image_url"],
                            "thumbnail": thumbnail_value,
                        }
                    )

            if thumbnail_updates and "thumbnail" in table.columns:
                update_stmt = text(
                    f"UPDATE {table.name} "
                    f"SET thumbnail = ST_FromGDALRaster(:thumbnail_raster) "
                    f"WHERE image_url = :image_url"
                )
                for update_values in thumbnail_updates:
                    thumbnail_raster = gif._thumbnail_to_gdal_raster(
                        update_values["thumbnail"]
                    )
                    conn.execute(
                        update_stmt,
                        {
                            "thumbnail_raster": thumbnail_raster,
                            "image_url": update_values["image_url"],
                        },
                    )
