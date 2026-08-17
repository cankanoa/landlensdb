# -*- coding: utf-8 -*-
"""QGIS import workflow grouped by canonical import parameter SHA."""

import threading
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import sql
from qgis.PyQt import QtCore, QtWidgets
from qgis.core import Qgis
from sqlalchemy import create_engine

from ..landlensdb import (
    Postgres,
    calculate_input_sha,
    import_local_images,
    import_yaml_to_function_params,
    load_example_import_yaml,
    load_import_presets,
    normalize_import_yaml,
)
from ..landlensdb.handlers.importer import discover_image_paths
from ..landlensdb.handlers.local import ImportCancelledError
from ..shared.connection_utils import (
    connection_kwargs,
    fetch_base_tables,
    load_connection_settings,
    validate_connection_values,
)
from ..shared.import_settings import (
    has_saved_import_parameters,
    load_import_parameters,
    save_import_parameters,
)
from ..shared.metadata_settings import fetch_metadata_tree
from ..shared.yaml_editor import ImportYamlDialog


class AddTableDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super(AddTableDialog, self).__init__(parent)
        self.setWindowTitle("Add Table")
        layout = QtWidgets.QVBoxLayout(self)
        row = QtWidgets.QHBoxLayout()
        row.addWidget(QtWidgets.QLabel("Name"))
        self.name_input = QtWidgets.QLineEdit(self)
        row.addWidget(self.name_input, 1)
        layout.addLayout(row)
        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def table_name(self):
        return self.name_input.text().strip()


class ImportTab(QtWidgets.QWidget):
    """Import images and show one row per canonical import configuration."""

    COUNT_COLUMN = 0
    FILE_GLOB_COLUMN = 1
    IMPORT_PARAMS_COLUMN = 2
    ACTIONS_COLUMN = 3
    HEADERS = ["Rows", "file_glob", "import_params", "Actions"]

    def __init__(self, iface, parent=None):
        super(ImportTab, self).__init__(parent)
        self.iface = iface
        self.connection_values = load_connection_settings()
        self._selected_table = None
        self._cancel_import_event = threading.Event()
        self._import_active = False

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        table_row = QtWidgets.QHBoxLayout()
        table_row.addWidget(QtWidgets.QLabel("Table:"))
        self.table_button = QtWidgets.QToolButton(self)
        self.table_button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.table_button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.table_button.setArrowType(QtCore.Qt.DownArrow)
        self.table_button.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed
        )
        table_row.addWidget(self.table_button, 1)
        self.refresh_button = QtWidgets.QPushButton("Refresh", self)
        self.refresh_button.clicked.connect(self.refresh_table)
        table_row.addWidget(self.refresh_button)
        layout.addLayout(table_row)

        self.import_table = QtWidgets.QTableWidget(self)
        self.import_table.setColumnCount(len(self.HEADERS))
        self.import_table.setHorizontalHeaderLabels(self.HEADERS)
        self.import_table.verticalHeader().setVisible(False)
        self.import_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.import_table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.import_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.import_table.setHorizontalScrollMode(
            QtWidgets.QAbstractItemView.ScrollPerPixel
        )
        self.import_table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.import_table.horizontalHeader().setMinimumSectionSize(24)
        self.import_table.horizontalHeader().setStretchLastSection(False)
        self.import_table.itemDoubleClicked.connect(
            self.open_selected_group_import_parameters
        )
        layout.addWidget(self.import_table, 1)

        runtime_row = QtWidgets.QHBoxLayout()
        runtime_row.addWidget(QtWidgets.QLabel("Threads:"))
        self.thread_count_input = QtWidgets.QSpinBox(self)
        self.thread_count_input.setRange(1, 256)
        self.thread_count_input.setValue(4)
        runtime_row.addWidget(self.thread_count_input)
        runtime_row.addWidget(QtWidgets.QLabel("Batch size:"))
        self.batch_size_input = QtWidgets.QSpinBox(self)
        self.batch_size_input.setRange(1, 10000)
        self.batch_size_input.setValue(100)
        runtime_row.addWidget(self.batch_size_input)
        runtime_row.addWidget(QtWidgets.QLabel("On error:"))
        self.on_error_input = QtWidgets.QComboBox(self)
        self.on_error_input.addItems(["skip", "warn", "error"])
        runtime_row.addWidget(self.on_error_input)
        self.open_yaml_button = QtWidgets.QPushButton("Import Parameters…", self)
        self.open_yaml_button.clicked.connect(self.open_import_parameters)
        runtime_row.addWidget(self.open_yaml_button)
        self.actions_button = self._build_actions_button()
        runtime_row.addWidget(self.actions_button)
        self.progress_bar = QtWidgets.QProgressBar(self)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        runtime_row.addWidget(self.progress_bar, 1)
        self.cancel_button = QtWidgets.QPushButton("Cancel", self)
        self.cancel_button.clicked.connect(self._cancel_active_import)
        runtime_row.addWidget(self.cancel_button)
        layout.addLayout(runtime_row)

        self._refresh_table_choices()
        self.load_records([])
        self._set_import_active(False)

    def showEvent(self, event):
        super(ImportTab, self).showEvent(event)
        self._refresh_table_choices()

    def reload_connection_settings(self, values=None):
        self.connection_values = dict(values or load_connection_settings())
        self._refresh_table_choices()

    def current_table_name(self):
        return self._selected_table

    def _refresh_table_choices(self, selected_table=None):
        tables = fetch_base_tables(self.connection_values)
        current = selected_table or self._selected_table
        self._selected_table = (
            current if current in tables else (tables[0] if tables else None)
        )
        menu = QtWidgets.QMenu(self.table_button)
        for table_name in tables:
            menu.addAction(
                table_name,
                lambda checked=False, name=table_name: self._select_table(name),
            )
        if tables:
            menu.addSeparator()
            drop_menu = menu.addMenu("Delete Table")
            for table_name in tables:
                drop_menu.addAction(
                    table_name,
                    lambda checked=False, name=table_name: self.drop_selected_table(
                        name
                    ),
                )
        menu.addAction("Add Table…", self.add_table)
        self.table_button.setMenu(menu)
        self.table_button.setText(self._selected_table or "Choose Table")

    def _select_table(self, table_name):
        self._selected_table = table_name
        self.table_button.setText(table_name)
        self.refresh_table()

    def add_table(self):
        valid, message = validate_connection_values(self.connection_values)
        if not valid:
            self._show_message(message, Qgis.Critical)
            return
        dialog = AddTableDialog(self)
        if not dialog.exec_() or not dialog.table_name():
            return
        table_name = dialog.table_name()
        schema_name = (
            self.connection_values.get("schema", "public").strip() or "public"
        )
        try:
            with psycopg2.connect(
                **connection_kwargs(self.connection_values)
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster")
                    cursor.execute(
                        sql.SQL("""CREATE TABLE {}.{} (
                                image_url text NOT NULL,
                                name text NOT NULL,
                                geometry geometry(Geometry, 4326) NOT NULL,
                                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                                thumbnail raster,
                                fingerprint text,
                                input_sha text NOT NULL,
                                import_params text NOT NULL
                            )""").format(
                            sql.Identifier(schema_name), sql.Identifier(table_name)
                        )
                    )
                    cursor.execute(
                        sql.SQL(
                            "ALTER TABLE {}.{} ADD CONSTRAINT {} UNIQUE (image_url)"
                        ).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier("{}_image_url_key".format(table_name)),
                        )
                    )
                    cursor.execute(
                        sql.SQL("CREATE INDEX {} ON {}.{} (input_sha)").format(
                            sql.Identifier("{}_input_sha_idx".format(table_name)),
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )
                connection.commit()
        except Exception as exc:
            self._show_message("Could not create table: {}".format(exc), Qgis.Critical)
            return
        self._refresh_table_choices(table_name)
        self.load_records([])

    def drop_selected_table(self, table_name=None):
        table_name = table_name or self.current_table_name()
        if not table_name:
            return
        if (
            QtWidgets.QMessageBox.question(
                self, "Drop Table", 'Drop table "{}"?'.format(table_name)
            )
            != QtWidgets.QMessageBox.Yes
        ):
            return
        schema_name = self.connection_values.get("schema", "public").strip() or "public"
        try:
            with psycopg2.connect(
                **connection_kwargs(self.connection_values)
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("DROP TABLE {}.{}").format(
                            sql.Identifier(schema_name), sql.Identifier(table_name)
                        )
                    )
                connection.commit()
        except Exception as exc:
            self._show_message("Could not drop table: {}".format(exc), Qgis.Critical)
            return
        self._selected_table = None
        self._refresh_table_choices()
        self.load_records([])

    def refresh_table(self):
        table_name = self.current_table_name()
        if not table_name:
            self.load_records([])
            return
        schema_name = self.connection_values.get("schema", "public").strip() or "public"
        try:
            with psycopg2.connect(
                **connection_kwargs(self.connection_values)
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL(
                            "SELECT input_sha, COUNT(*), MIN(import_params) "
                            "FROM {}.{} WHERE input_sha IS NOT NULL "
                            "GROUP BY input_sha ORDER BY input_sha"
                        ).format(
                            sql.Identifier(schema_name), sql.Identifier(table_name)
                        )
                    )
                    records = [
                        {
                            "input_sha": row[0],
                            "row_count": row[1],
                            "import_params": row[2],
                        }
                        for row in cursor.fetchall()
                    ]
                connection.commit()
        except Exception as exc:
            self._show_message(
                "Could not load import groups: {}".format(exc), Qgis.Critical
            )
            return
        self.load_records(records)

    def load_records(self, records):
        self.import_table.setRowCount(len(records or []))
        for row_index, record in enumerate(records or []):
            input_sha = str(record.get("input_sha") or "")
            count = int(record.get("row_count") or 0)
            import_params = record.get("import_params")
            count_item = QtWidgets.QTableWidgetItem(str(count))
            count_item.setData(QtCore.Qt.UserRole, input_sha)
            count_item.setData(QtCore.Qt.UserRole + 1, import_params)
            self.import_table.setItem(row_index, self.COUNT_COLUMN, count_item)

            file_glob = self._file_glob_from_import_params(import_params)
            file_glob_item = QtWidgets.QTableWidgetItem(file_glob)
            file_glob_item.setToolTip(file_glob)
            self.import_table.setItem(
                row_index, self.FILE_GLOB_COLUMN, file_glob_item
            )
            self.import_table.setCellWidget(
                row_index,
                self.IMPORT_PARAMS_COLUMN,
                self._build_import_params_button(input_sha),
            )
            self.import_table.setCellWidget(
                row_index,
                self.ACTIONS_COLUMN,
                self._build_actions_button(input_sha, import_params),
            )
        if self.import_table.rowCount():
            self.import_table.selectRow(0)
        QtCore.QTimer.singleShot(0, self._resize_import_columns)

    @staticmethod
    def _file_glob_from_import_params(import_params):
        if not import_params:
            return ""
        try:
            parameters = import_yaml_to_function_params(import_params)
        except Exception:
            return ""
        return str(parameters.get("source_file_glob") or "")

    def _resize_import_columns(self):
        """Give file_glob spare width while retaining content-size minima."""
        if not hasattr(self, "import_table"):
            return
        for column in range(self.import_table.columnCount()):
            self.import_table.resizeColumnToContents(column)
        used_width = sum(
            self.import_table.columnWidth(column)
            for column in range(self.import_table.columnCount())
        )
        spare_width = self.import_table.viewport().width() - used_width
        if spare_width > 0:
            self.import_table.setColumnWidth(
                self.FILE_GLOB_COLUMN,
                self.import_table.columnWidth(self.FILE_GLOB_COLUMN) + spare_width,
            )

    def resizeEvent(self, event):
        super(ImportTab, self).resizeEvent(event)
        QtCore.QTimer.singleShot(0, self._resize_import_columns)

    def selected_input_sha(self):
        row = self.import_table.currentRow()
        if row < 0:
            return None
        item = self.import_table.item(row, self.COUNT_COLUMN)
        return item.data(QtCore.Qt.UserRole) if item else None

    def _fetch_first_import_params(self, input_sha):
        table_name = self.current_table_name()
        if not table_name or not input_sha:
            return None
        schema_name = self.connection_values.get("schema", "public").strip() or "public"
        with psycopg2.connect(
            **connection_kwargs(self.connection_values)
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        "SELECT import_params FROM {}.{} WHERE input_sha = %s ORDER BY image_url LIMIT 1"
                    ).format(sql.Identifier(schema_name), sql.Identifier(table_name)),
                    (input_sha,),
                )
                row = cursor.fetchone()
        return row[0] if row else None

    def open_import_parameters(self, *_args):
        example = load_example_import_yaml()
        yaml_text = load_import_parameters(example)
        self._show_import_parameters(yaml_text)

    def _show_import_parameters(self, yaml_text):
        dialog = ImportYamlDialog(
            yaml_text,
            normalize_import_yaml,
            presets=load_import_presets(),
            parent=self,
        )
        if dialog.exec_():
            save_import_parameters(dialog.yaml_text())
            self._show_message("Import parameters saved.", Qgis.Info)

    def _build_import_params_button(self, input_sha):
        button = QtWidgets.QPushButton("View YAML", self)
        button.clicked.connect(
            lambda checked=False, sha=input_sha: self.open_group_import_parameters(sha)
        )
        return button

    def open_selected_group_import_parameters(self, *_args):
        input_sha = self.selected_input_sha()
        if input_sha:
            self.open_group_import_parameters(input_sha)

    def open_group_import_parameters(self, input_sha):
        try:
            yaml_text = self._fetch_first_import_params(input_sha)
        except Exception as exc:
            self._show_message(
                "Could not load import parameters: {}".format(exc), Qgis.Critical
            )
            return
        if not yaml_text:
            self._show_message(
                "This import group has no stored import parameters.", Qgis.Warning
            )
            return
        self._show_import_parameters(yaml_text)

    def _build_actions_button(self, input_sha=None, yaml_text=None):
        button = QtWidgets.QToolButton(self)
        button.setText("Actions")
        button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        menu = QtWidgets.QMenu(button)
        if input_sha:
            callbacks = (
                ("Update", lambda: self.run_row_updates(input_sha, yaml_text, False)),
                (
                    "Update New",
                    lambda: self.run_row_updates(input_sha, yaml_text, True),
                ),
                ("Drop Old", lambda: self.run_row_drop_old(input_sha, yaml_text)),
                ("Drop All", lambda: self.run_row_drop_all(input_sha, yaml_text)),
                (
                    "Sync (Drop Old/Update)",
                    lambda: self.run_row_sync(input_sha, yaml_text),
                ),
                (
                    "Fetch Metadata Structure",
                    lambda checked=False: self.fetch_metadata(input_sha),
                ),
            )
        else:
            callbacks = (
                ("Update", lambda: self.run_all_updates(False)),
                ("Update New", lambda: self.run_all_updates(True)),
                ("Drop Old", self.run_all_drop_old),
                ("Drop All", self.run_all_drop_all),
                ("Sync (Drop Old/Update)", self.run_all_sync),
                (
                    "Fetch Metadata Structure",
                    lambda checked=False: self.fetch_metadata("all"),
                ),
            )
        for label, callback in callbacks:
            menu.addAction(label, callback)
        button.setMenu(menu)
        return button

    def _saved_config(self):
        yaml_text = load_import_parameters(load_example_import_yaml())
        return calculate_input_sha(yaml_text), yaml_text

    def _row_configs(self):
        configs = []
        for row in range(self.import_table.rowCount()):
            item = self.import_table.item(row, self.COUNT_COLUMN)
            if item is None:
                continue
            input_sha = item.data(QtCore.Qt.UserRole)
            yaml_text = item.data(QtCore.Qt.UserRole + 1)
            if input_sha and not yaml_text:
                yaml_text = self._fetch_first_import_params(input_sha)
            if input_sha and yaml_text:
                configs.append((input_sha, yaml_text))
        return configs

    def _update_configs(self):
        configs = self._row_configs()
        if has_saved_import_parameters() or not configs:
            saved = self._saved_config()
            if saved[0] not in {input_sha for input_sha, _yaml in configs}:
                configs.append(saved)
        return configs

    def _run_updates(self, configs, skip_existing):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message("Choose or create a table first.", Qgis.Critical)
            return
        if not configs:
            self._show_message("No import parameters are available.", Qgis.Warning)
            return
        try:
            db = self._database(table_name, select_table=True)
            self._cancel_import_event.clear()
            self._set_import_active(True)
            wrote = False
            for _input_sha, yaml_text in configs:
                function_params = import_yaml_to_function_params(yaml_text)
                try:
                    batches = import_local_images(
                        **function_params,
                        max_workers=self.thread_count_input.value(),
                        batch_size=self.batch_size_input.value(),
                        return_as_yield=True,
                        progress_callback=self._update_progress,
                        skip_images_in_postgresql=db,
                        skip_existing=bool(skip_existing),
                        on_error=self.on_error_input.currentText(),
                        cancel_event=self._cancel_import_event,
                    )
                    for images in batches:
                        db.upsert_images(images, table_name, conflict="update")
                        wrote = True
                except ValueError as exc:
                    if skip_existing and "No new files match" in str(exc):
                        continue
                    raise
            if not wrote:
                self._show_message("No new images were found.", Qgis.Info)
            else:
                self._show_message("Import update completed.", Qgis.Info)
        except ImportCancelledError:
            self._show_message("Import cancelled.", Qgis.Warning)
        except Exception as exc:
            self._show_message("Import failed: {}".format(exc), Qgis.Critical)
        finally:
            self._set_import_active(False)
            self.refresh_table()

    def _run_drop_old(self, configs):
        deleted = 0
        db = self._database(self.current_table_name(), True)
        for input_sha, yaml_text in configs:
            parameters = import_yaml_to_function_params(yaml_text)
            paths = discover_image_paths(parameters["source_file_glob"])
            deleted += db.remove_unmatched_for_input(input_sha, paths)
        return deleted

    def _run_drop_all(self, configs):
        db = self._database(self.current_table_name(), True)
        return sum(db.remove_all_for_input(input_sha) for input_sha, _yaml in configs)

    def run_all_updates(self, skip_existing=False):
        self._run_updates(self._update_configs(), skip_existing)

    def run_row_updates(self, input_sha, yaml_text, skip_existing=False):
        if not yaml_text:
            yaml_text = self._fetch_first_import_params(input_sha)
        self._run_updates([(input_sha, yaml_text)], skip_existing)

    def run_all_drop_old(self):
        try:
            deleted = self._run_drop_old(self._row_configs())
        except Exception as exc:
            self._show_message("Drop Old failed: {}".format(exc), Qgis.Critical)
            return
        self._show_message("Removed {} stale row(s).".format(deleted), Qgis.Info)
        self.refresh_table()

    def run_row_drop_old(self, input_sha, yaml_text):
        try:
            if not yaml_text:
                yaml_text = self._fetch_first_import_params(input_sha)
            deleted = self._run_drop_old([(input_sha, yaml_text)])
        except Exception as exc:
            self._show_message("Drop Old failed: {}".format(exc), Qgis.Critical)
            return
        self._show_message("Removed {} stale row(s).".format(deleted), Qgis.Info)
        self.refresh_table()

    def run_all_drop_all(self):
        configs = self._row_configs()
        if not configs:
            return
        if (
            QtWidgets.QMessageBox.question(
                self, "Drop All Imports", "Delete every imported row in this table?"
            )
            != QtWidgets.QMessageBox.Yes
        ):
            return
        try:
            deleted = self._run_drop_all(configs)
        except Exception as exc:
            self._show_message("Drop All failed: {}".format(exc), Qgis.Critical)
            return
        self._show_message("Removed {} row(s).".format(deleted), Qgis.Info)
        self.refresh_table()

    def run_row_drop_all(self, input_sha, yaml_text):
        if (
            QtWidgets.QMessageBox.question(
                self, "Drop Import Group", "Delete every row in this import group?"
            )
            != QtWidgets.QMessageBox.Yes
        ):
            return
        try:
            deleted = self._run_drop_all([(input_sha, yaml_text)])
        except Exception as exc:
            self._show_message("Drop All failed: {}".format(exc), Qgis.Critical)
            return
        self._show_message("Removed {} row(s).".format(deleted), Qgis.Info)
        self.refresh_table()

    def run_all_sync(self):
        configs = self._update_configs()
        try:
            self._run_drop_old(configs)
        except Exception as exc:
            self._show_message("Sync failed: {}".format(exc), Qgis.Critical)
            return
        self._run_updates(configs, False)

    def run_row_sync(self, input_sha, yaml_text):
        if not yaml_text:
            yaml_text = self._fetch_first_import_params(input_sha)
        try:
            self._run_drop_old([(input_sha, yaml_text)])
        except Exception as exc:
            self._show_message("Sync failed: {}".format(exc), Qgis.Critical)
            return
        self._run_updates([(input_sha, yaml_text)], False)

    def fetch_metadata(self, input_sha):
        """Fetch one metadata row per input SHA and save the parameter tree."""
        table_name = self.current_table_name()
        if not table_name:
            self._show_message("Choose or create a table first.", Qgis.Critical)
            return
        schema_name = self.connection_values.get("schema", "public").strip() or "public"
        try:
            with psycopg2.connect(
                **connection_kwargs(self.connection_values)
            ) as connection:
                with connection.cursor() as cursor:
                    source_query = sql.SQL("SELECT * FROM {}.{}").format(
                        sql.Identifier(schema_name), sql.Identifier(table_name)
                    )
                    tree = fetch_metadata_tree(cursor, source_query, input_sha)
        except Exception as exc:
            self._show_message(
                "Could not fetch metadata structure: {}".format(exc), Qgis.Critical
            )
            return
        self._show_message("Metadata structure fetched.", Qgis.Info)
        return tree

    def _database(self, table_name, select_table=False):
        db = Postgres(self._build_database_url())
        db.engine = create_engine(
            self._build_database_url(), connect_args=self._engine_connect_args()
        )
        if select_table:
            db.table(table_name)
        return db

    def _build_database_url(self):
        values = self.connection_values
        database = quote_plus(values.get("database", "").strip())
        service = values.get("service", "").strip()
        user = quote_plus(values.get("user", "").strip())
        password = quote_plus(values.get("password", ""))
        auth = user
        if auth and values.get("password", ""):
            auth = "{}:{}".format(auth, password)
        if auth:
            auth += "@"
        if service:
            return "postgresql+psycopg2://{}/{database}".format(auth, database=database)
        return "postgresql+psycopg2://{}{}:{}/{}".format(
            auth,
            values.get("host", "").strip(),
            values.get("port", "").strip() or "5432",
            database,
        )

    def _engine_connect_args(self):
        schema_name = self.connection_values.get("schema", "public").strip() or "public"
        args = {"options": "-csearch_path={},public".format(schema_name)}
        service = self.connection_values.get("service", "").strip()
        if service:
            args["service"] = service
        if self.connection_values.get("user", "").strip():
            args["user"] = self.connection_values["user"].strip()
        if self.connection_values.get("password", ""):
            args["password"] = self.connection_values["password"]
        return args

    def _set_import_active(self, active):
        self._import_active = bool(active)
        self.cancel_button.setEnabled(self._import_active)
        self.open_yaml_button.setEnabled(not self._import_active)

    def _cancel_active_import(self):
        if self._import_active:
            self._cancel_import_event.set()

    def _update_progress(self, processed, total):
        self.progress_bar.setRange(0, max(total, 1))
        self.progress_bar.setValue(processed)
        self.progress_bar.setFormat("{}/{}".format(processed, total))
        application = QtWidgets.QApplication.instance()
        if application is not None:
            application.processEvents()

    def _show_message(self, message, level):
        if self.iface is not None:
            self.iface.messageBar().pushMessage(
                "Landlensdb", message, level=level, duration=6
            )
