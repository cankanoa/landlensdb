# -*- coding: utf-8 -*-

import threading
from urllib.parse import quote_plus

from qgis.PyQt import QtCore, QtWidgets
from qgis.core import Qgis

from ..shared.connection_dialog import ConnectionDialog
from ..shared.connection_utils import (
    connection_kwargs,
    load_connection_settings,
    save_connection_settings,
    test_connection_values,
    validate_connection_values,
)
from ..shared.import_params import unique_import_parameter_rows
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

from ..landlensdb import SearchLocalToGeoImageFrame, Postgres
from ..landlensdb.handlers.local import ImportCancelledError



class AddTableDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super(AddTableDialog, self).__init__(parent)
        self.setWindowTitle('Add Table')
        self.resize(360, 100)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QHBoxLayout()
        form.addWidget(QtWidgets.QLabel('Name'))
        self.name_input = QtWidgets.QLineEdit()
        form.addWidget(self.name_input, 1)
        layout.addLayout(form)

        buttons = QtWidgets.QHBoxLayout()
        self.create_button = QtWidgets.QPushButton('Create')
        self.cancel_button = QtWidgets.QPushButton('Cancel')
        buttons.addStretch()
        buttons.addWidget(self.create_button)
        buttons.addWidget(self.cancel_button)
        layout.addLayout(buttons)

        self.create_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)

    def table_name(self):
        return self.name_input.text().strip()


class ImportTab(QtWidgets.QWidget):
    STATUS_COLUMN = 0
    ACTIONS_COLUMN = 1
    IMPORT_TYPE_COLUMN = 2
    QUERY_FROM_COLUMN = 3
    SEARCH_RE_COLUMN = 4
    HEADERS = ['', 'Actions', 'import_type', 'query_from', 'search_re']
    ADD_TABLE_SENTINEL = '__add_table__'
    IMPORT_TYPES = ['', 'GeoTaggedImage', 'GeoTransformImage', 'WorldView3Image']

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

        top_row = QtWidgets.QHBoxLayout()
        top_row.addWidget(QtWidgets.QLabel('Table:'))
        self.table_button = QtWidgets.QToolButton()
        self.table_button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.table_button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
        self.table_button.setArrowType(QtCore.Qt.DownArrow)
        self.table_button.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        top_row.addWidget(self.table_button, 1)
        self.refresh_table_button = QtWidgets.QToolButton()
        self.refresh_table_button.setAutoRaise(True)
        self.refresh_table_button.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload)
        )
        self.refresh_table_button.setToolTip('Refresh import parameter rows from the selected table.')
        self.refresh_table_button.clicked.connect(self.refresh_table)
        top_row.addWidget(self.refresh_table_button)
        self.update_all_button = QtWidgets.QPushButton('Update')
        self.update_all_button.setToolTip('Update all saved trios in the selected table.')
        self.update_all_button.clicked.connect(self.run_all_updates)
        top_row.addWidget(self.update_all_button)
        self.update_all_new_button = QtWidgets.QPushButton('Update New')
        self.update_all_new_button.setToolTip('Update all saved trios, skipping images already in PostgreSQL.')
        self.update_all_new_button.clicked.connect(
            lambda: self.run_all_updates(skip_existing=True)
        )
        top_row.addWidget(self.update_all_new_button)
        self.drop_old_all_button = QtWidgets.QPushButton('Drop Old')
        self.drop_old_all_button.setToolTip('Remove rows whose source files no longer exist on disk.')
        self.drop_old_all_button.clicked.connect(self.run_all_drop_old)
        top_row.addWidget(self.drop_old_all_button)
        self.drop_all_button = QtWidgets.QPushButton('Drop All')
        self.drop_all_button.setToolTip('Remove all rows for all saved trios in this table.')
        self.drop_all_button.clicked.connect(self.run_all_drop_all)
        top_row.addWidget(self.drop_all_button)
        self.sync_all_button = QtWidgets.QPushButton('Sync')
        self.sync_all_button.setToolTip('Drop old rows, then update all saved trios.')
        self.sync_all_button.clicked.connect(self.run_all_sync)
        top_row.addWidget(self.sync_all_button)
        layout.addLayout(top_row)

        self.import_table = QtWidgets.QTableWidget(self)
        self.import_table.setColumnCount(len(self.HEADERS))
        self.import_table.setHorizontalHeaderLabels(self.HEADERS)
        self.import_table.verticalHeader().setVisible(False)
        self.import_table.setAlternatingRowColors(True)
        self.import_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectItems)
        self.import_table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.import_table.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_table.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.import_table)

        button_row = QtWidgets.QHBoxLayout()
        self.connection_button = QtWidgets.QPushButton('Connection')
        button_row.addWidget(self.connection_button)
        button_row.addWidget(QtWidgets.QLabel('Threads:'))
        self.thread_count_input = QtWidgets.QSpinBox(self)
        self.thread_count_input.setMinimum(1)
        self.thread_count_input.setMaximum(256)
        self.thread_count_input.setValue(1)
        self.thread_count_input.setFixedWidth(72)
        button_row.addWidget(self.thread_count_input)
        self.import_progress_bar = QtWidgets.QProgressBar(self)
        self.import_progress_bar.setTextVisible(False)
        self.import_progress_bar.setRange(0, 0)
        self.import_progress_bar.setValue(0)
        self.import_progress_bar.setSizePolicy(
            QtWidgets.QSizePolicy.Expanding,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.import_progress_label = QtWidgets.QLabel('0/0', self)
        self.import_progress_label.setAlignment(
            QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter
        )
        button_row.addWidget(self.import_progress_bar, 1)
        button_row.addWidget(self.import_progress_label)
        self.cancel_import_button = QtWidgets.QToolButton(self)
        self.cancel_import_button.setText('x')
        self.cancel_import_button.setAutoRaise(True)
        self.cancel_import_button.setToolTip('Cancel the active import.')
        self.cancel_import_button.clicked.connect(self._cancel_active_import)
        button_row.addWidget(self.cancel_import_button)
        layout.addLayout(button_row)

        self.connection_button.clicked.connect(self.open_connection_dialog)

        self._update_connection_button_text()
        self._refresh_table_choices()
        self.load_records([])
        self._reset_progress()
        self._set_import_active(False)

    def showEvent(self, event):
        super(ImportTab, self).showEvent(event)
        self._refresh_table_choices()
        if self.current_table_name():
            self._select_table(self.current_table_name())

    def reload_connection_settings(self, values=None):
        self.connection_values = dict(values or load_connection_settings())
        self._update_connection_button_text()
        self._refresh_table_choices()

    def open_connection_dialog(self):
        dialog = ConnectionDialog(self.connection_values, self._test_connection_values, self)
        if dialog.exec_():
            self.connection_values = dialog.values()
            save_connection_settings(self.connection_values)
            self._update_connection_button_text()
            self._refresh_table_choices()

    def _test_connection_values(self, values):
        success, message = test_connection_values(values)
        if success:
            self.connection_values = dict(values)
            save_connection_settings(self.connection_values)
            self._update_connection_button_text()
            self._refresh_table_choices()
        return success, message

    def _update_connection_button_text(self):
        label = self.connection_values.get('name') or self.connection_values.get('database') or 'Connection'
        self.connection_button.setText('Connection' if label == 'Connection' else 'Connection: {}'.format(label))

    def _refresh_table_choices(self, selected_table=None):
        current_table = selected_table or self.current_table_name()
        tables = self._fetch_tables()

        if current_table in tables:
            self._selected_table = current_table
        elif tables:
            self._selected_table = tables[0]
        else:
            self._selected_table = None

        menu = QtWidgets.QMenu(self.table_button)
        for table_name in tables:
            menu.addAction(table_name, lambda checked=False, name=table_name: self._select_table(name))
        if tables:
            menu.addSeparator()
            delete_menu = menu.addMenu('Delete Table')
            for table_name in tables:
                delete_menu.addAction(
                    table_name,
                    lambda checked=False, name=table_name: self.drop_selected_table(name),
                )
        menu.addAction('Add Table...', self.add_table)
        menu.setMinimumWidth(max(self.table_button.width(), 240))

        self.table_button.setMenu(menu)
        button_label = self._selected_table or 'Choose Table'
        self.table_button.setText(button_label)

    def current_table_name(self):
        return self._selected_table

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
        if not dialog.exec_():
            self._refresh_table_choices()
            return

        table_name = dialog.table_name()
        if not table_name:
            self._show_message('Table name is required.', Qgis.Critical)
            self._refresh_table_choices()
            return

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        try:
            with psycopg2.connect(**connection_kwargs(self.connection_values)) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL(
                            """
                            CREATE TABLE {}.{} (
                                image_url text NOT NULL,
                                name text NOT NULL,
                                geometry geometry(Geometry, 4326) NOT NULL,
                                metadata jsonb,
                                fingerprint text,
                                thumbnail raster
                            )
                            """
                        ).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )
                    cursor.execute(
                        sql.SQL(
                            'ALTER TABLE {}.{} ADD CONSTRAINT {} UNIQUE (image_url)'
                        ).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier('{}_image_url_key'.format(table_name)),
                        )
                    )
                    cursor.execute(
                        sql.SQL(
                            'ALTER TABLE {}.{} ADD CONSTRAINT {} UNIQUE (fingerprint)'
                        ).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier('{}_fingerprint_key'.format(table_name)),
                        )
                    )
                connection.commit()
        except Exception as exc:  # pragma: no cover
            self._show_message('Could not create table: {}'.format(exc), Qgis.Critical)
            self._refresh_table_choices()
            return

        self._refresh_table_choices(selected_table=table_name)
        self._select_table(table_name)
        self._show_message('Created table "{}".'.format(table_name), Qgis.Info)

    def drop_selected_table(self, table_name=None):
        table_name = table_name or self.current_table_name()
        if not table_name:
            return

        answer = QtWidgets.QMessageBox.question(
            self,
            'Drop Table',
            'Drop table "{}"?'.format(table_name),
        )
        if answer != QtWidgets.QMessageBox.Yes:
            return

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        try:
            with psycopg2.connect(**connection_kwargs(self.connection_values)) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql.SQL('DROP TABLE {}.{}').format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )
                connection.commit()
        except Exception as exc:  # pragma: no cover
            self._show_message('Could not drop table: {}'.format(exc), Qgis.Critical)
            return

        self._refresh_table_choices()
        self.load_records([])
        self._show_message('Dropped table "{}".'.format(table_name), Qgis.Info)

    def refresh_table(self):
        valid, message = validate_connection_values(self.connection_values)
        if not valid:
            self._show_message(message, Qgis.Critical)
            return

        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        records = []

        try:
            with psycopg2.connect(**connection_kwargs(self.connection_values)) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        cursor.execute(
                            sql.SQL('SET search_path TO {}, public').format(
                                sql.Identifier(schema_name),
                            )
                        )
                    cursor.execute(
                        sql.SQL(
                            """
                            SELECT
                                metadata::jsonb->'input_params'->>'query_from' AS query_from,
                                metadata::jsonb->'input_params'->>'import_type' AS import_type,
                                metadata::jsonb->'input_params'->>'search_re' AS search_re,
                                COUNT(*) AS row_count
                            FROM {}.{}
                            WHERE metadata::jsonb ? 'input_params'
                            GROUP BY 1, 2, 3
                            ORDER BY 1, 2, 3
                            """
                        ).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )
                    records = [
                        {
                            'metadata': {
                                'input_params': {
                                    'query_from': row[0],
                                    'import_type': row[1],
                                    'search_re': row[2],
                                    'row_count': row[3],
                                }
                            }
                        }
                        for row in cursor.fetchall()
                    ]
        except Exception as exc:  # pragma: no cover
            self._show_message('Could not load import parameters: {}'.format(exc), Qgis.Critical)
            return

        self.load_records(records)
        self._show_message('Loaded {} import parameter set(s).'.format(len(records)), Qgis.Info)

    def load_records(self, records):
        unique_rows = unique_import_parameter_rows(records)
        row_counts = {}
        for record in records:
            input_params = (record.get('metadata') or {}).get('input_params') or {}
            trio = (
                input_params.get('query_from') or '',
                input_params.get('import_type') or '',
                input_params.get('search_re') or '',
            )
            row_counts[trio] = input_params.get('row_count', row_counts.get(trio, 0))

        self.import_table.clearContents()
        self.import_table.setRowCount(len(unique_rows) + 1)
        for row_index, row_values in enumerate(unique_rows):
            self.import_table.setCellWidget(
                row_index,
                self.STATUS_COLUMN,
                self._build_status_widget(
                    row_index,
                    row_values,
                    row_counts.get(row_values, 0),
                    editable=True,
                ),
            )
            self.import_table.setCellWidget(
                row_index,
                self.ACTIONS_COLUMN,
                self._build_actions_widget(row_index, include_update_new=True),
            )
            self.import_table.setCellWidget(row_index, self.IMPORT_TYPE_COLUMN, self._build_import_type_widget(row_values[1], row_index))
            self.import_table.setCellWidget(row_index, self.QUERY_FROM_COLUMN, self._build_query_from_widget(row_values[0], row_index))
            self.import_table.setCellWidget(row_index, self.SEARCH_RE_COLUMN, self._build_search_re_widget(row_values[2], row_index))
            self._update_status_widget(row_index)

        add_row_index = len(unique_rows)
        self.import_table.setCellWidget(
            add_row_index,
            self.STATUS_COLUMN,
            self._build_status_widget(add_row_index, None, 0, editable=False),
        )
        self.import_table.setCellWidget(
            add_row_index,
            self.ACTIONS_COLUMN,
            self._build_actions_widget(add_row_index, include_update_new=False),
        )
        self.import_table.setCellWidget(add_row_index, self.IMPORT_TYPE_COLUMN, self._build_import_type_widget('', add_row_index))
        self.import_table.setCellWidget(add_row_index, self.QUERY_FROM_COLUMN, self._build_query_from_widget('', add_row_index))
        self.import_table.setCellWidget(add_row_index, self.SEARCH_RE_COLUMN, self._build_search_re_widget('', add_row_index))
        self._update_status_widget(add_row_index)

        header = self.import_table.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(4, QtWidgets.QHeaderView.Stretch)
        self.import_table.resizeColumnsToContents()

    def run_row_update(self, row_index, skip_existing=False):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            self._show_message('Import row is incomplete.', Qgis.Critical)
            return

        query_from = query_from_input.text().strip()
        import_type = import_type_input.currentText().strip()
        search_re = search_re_input.text().strip()
        if not query_from or not import_type or not search_re:
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            self._run_import_update(
                table_name,
                query_from,
                import_type,
                search_re,
                skip_existing=skip_existing,
            )
        except ImportCancelledError:
            self._show_message('Import cancelled.', Qgis.Warning)
            self.refresh_table()
            return
        except Exception as exc:  # pragma: no cover
            self._show_message('Import update failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message('Updated "{}" from {}.'.format(table_name, query_from), Qgis.Info)
        self.refresh_table()

    def run_all_updates(self, skip_existing=False):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        update_rows = []
        for row_index in range(max(self.import_table.rowCount() - 1, 0)):
            import_type_input = self._import_type_input(row_index)
            query_from_input = self._query_from_input(row_index)
            search_re_input = self._search_re_input(row_index)
            if query_from_input is None or import_type_input is None or search_re_input is None:
                continue

            query_from = query_from_input.text().strip()
            import_type = import_type_input.currentText().strip()
            search_re = search_re_input.text().strip()
            if not query_from or not import_type or not search_re:
                continue
            update_rows.append((query_from, import_type, search_re))

        if not update_rows:
            self._show_message('No saved import parameter rows to update.', Qgis.Critical)
            return

        for query_from, import_type, search_re in update_rows:
            try:
                self._run_import_update(
                    table_name,
                    query_from,
                    import_type,
                    search_re,
                    skip_existing=skip_existing,
                )
            except ImportCancelledError:
                self._show_message('Import cancelled.', Qgis.Warning)
                self.refresh_table()
                return
            except Exception as exc:  # pragma: no cover
                self._show_message('Import update failed: {}'.format(exc), Qgis.Critical)
                return

        self._show_message(
            'Updated {} import parameter set(s) for "{}".'.format(
                len(update_rows),
                table_name,
            ),
            Qgis.Info,
        )
        self.refresh_table()

    def run_all_drop_old(self):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        update_rows = []
        for row_index in range(max(self.import_table.rowCount() - 1, 0)):
            import_type_input = self._import_type_input(row_index)
            query_from_input = self._query_from_input(row_index)
            search_re_input = self._search_re_input(row_index)
            if query_from_input is None or import_type_input is None or search_re_input is None:
                continue

            query_from = query_from_input.text().strip()
            import_type = import_type_input.currentText().strip()
            search_re = search_re_input.text().strip()
            if not query_from or not import_type or not search_re:
                continue
            update_rows.append((query_from, import_type, search_re))

        if not update_rows:
            self._show_message('No saved import parameter rows to prune.', Qgis.Critical)
            return

        deleted_rows = 0
        for query_from, import_type, search_re in update_rows:
            try:
                deleted_rows += self._run_drop_old(
                    table_name,
                    query_from,
                    import_type,
                    search_re,
                )
            except Exception as exc:  # pragma: no cover
                self._show_message('Drop Old failed: {}'.format(exc), Qgis.Critical)
                return

        self._show_message(
            'Dropped {} old row(s) from "{}".'.format(deleted_rows, table_name),
            Qgis.Info,
        )
        self.refresh_table()

    def run_all_drop_all(self):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        update_rows = []
        for row_index in range(max(self.import_table.rowCount() - 1, 0)):
            import_type_input = self._import_type_input(row_index)
            query_from_input = self._query_from_input(row_index)
            search_re_input = self._search_re_input(row_index)
            if query_from_input is None or import_type_input is None or search_re_input is None:
                continue

            query_from = query_from_input.text().strip()
            import_type = import_type_input.currentText().strip()
            search_re = search_re_input.text().strip()
            if not query_from or not import_type or not search_re:
                continue
            update_rows.append((query_from, import_type, search_re))

        if not update_rows:
            self._show_message('No saved import parameter rows to drop.', Qgis.Critical)
            return

        deleted_rows = 0
        for query_from, import_type, search_re in update_rows:
            try:
                deleted_rows += self._run_drop_all(
                    table_name,
                    query_from,
                    import_type,
                    search_re,
                )
            except Exception as exc:  # pragma: no cover
                self._show_message('Drop All failed: {}'.format(exc), Qgis.Critical)
                return

        self._show_message(
            'Dropped {} row(s) from "{}".'.format(deleted_rows, table_name),
            Qgis.Info,
        )
        self.refresh_table()

    def run_all_sync(self):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        update_rows = []
        for row_index in range(max(self.import_table.rowCount() - 1, 0)):
            import_type_input = self._import_type_input(row_index)
            query_from_input = self._query_from_input(row_index)
            search_re_input = self._search_re_input(row_index)
            if query_from_input is None or import_type_input is None or search_re_input is None:
                continue

            query_from = query_from_input.text().strip()
            import_type = import_type_input.currentText().strip()
            search_re = search_re_input.text().strip()
            if not query_from or not import_type or not search_re:
                continue
            update_rows.append((query_from, import_type, search_re))

        if not update_rows:
            self._show_message('No saved import parameter rows to sync.', Qgis.Critical)
            return

        for query_from, import_type, search_re in update_rows:
            try:
                self._run_drop_old(table_name, query_from, import_type, search_re)
                self._run_import_update(
                    table_name,
                    query_from,
                    import_type,
                    search_re,
                    skip_existing=False,
                )
            except Exception as exc:  # pragma: no cover
                self._show_message('Sync failed: {}'.format(exc), Qgis.Critical)
                return

        self._show_message(
            'Synced {} import parameter set(s) for "{}".'.format(
                len(update_rows),
                table_name,
            ),
            Qgis.Info,
        )
        self.refresh_table()

    def run_row_drop_old(self, row_index):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            self._show_message('Import row is incomplete.', Qgis.Critical)
            return

        query_from = query_from_input.text().strip()
        import_type = import_type_input.currentText().strip()
        search_re = search_re_input.text().strip()
        if not query_from or not import_type or not search_re:
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            deleted_rows = self._run_drop_old(
                table_name,
                query_from,
                import_type,
                search_re,
            )
        except Exception as exc:  # pragma: no cover
            self._show_message('Drop Old failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message(
            'Dropped {} old row(s) from "{}".'.format(deleted_rows, table_name),
            Qgis.Info,
        )
        self.refresh_table()

    def run_row_drop_all(self, row_index):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            self._show_message('Import row is incomplete.', Qgis.Critical)
            return

        query_from = query_from_input.text().strip()
        import_type = import_type_input.currentText().strip()
        search_re = search_re_input.text().strip()
        if not query_from or not import_type or not search_re:
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            deleted_rows = self._run_drop_all(
                table_name,
                query_from,
                import_type,
                search_re,
            )
        except Exception as exc:  # pragma: no cover
            self._show_message('Drop All failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message(
            'Dropped {} row(s) from "{}".'.format(deleted_rows, table_name),
            Qgis.Info,
        )
        self.refresh_table()

    def run_row_sync(self, row_index):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            self._show_message('Import row is incomplete.', Qgis.Critical)
            return

        query_from = query_from_input.text().strip()
        import_type = import_type_input.currentText().strip()
        search_re = search_re_input.text().strip()
        if not query_from or not import_type or not search_re:
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            self._run_drop_old(table_name, query_from, import_type, search_re)
            self._run_import_update(
                table_name,
                query_from,
                import_type,
                search_re,
                skip_existing=False,
            )
        except ImportCancelledError:
            self._show_message('Sync cancelled.', Qgis.Warning)
            self.refresh_table()
            return
        except Exception as exc:  # pragma: no cover
            self._show_message('Sync failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message('Synced "{}" from {}.'.format(table_name, query_from), Qgis.Info)
        self.refresh_table()

    def _build_query_from_widget(self, value, row_index):
        widget = QtWidgets.QWidget(self.import_table)
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        line_edit = QtWidgets.QLineEdit(value)
        line_edit.setPlaceholderText('/folder/to/search')
        line_edit.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        line_edit.textChanged.connect(lambda _text, row=row_index: self._mark_row_dirty(row))
        browse_button = QtWidgets.QToolButton(widget)
        browse_button.setText('...')
        browse_button.clicked.connect(lambda: self._browse_query_from(line_edit))
        layout.addWidget(line_edit, 1)
        layout.addWidget(browse_button)
        widget.line_edit = line_edit
        min_width = line_edit.fontMetrics().horizontalAdvance(value or 'Select folder') + 48
        widget.setMinimumWidth(max(180, min_width))
        return widget

    def _build_import_type_widget(self, value, row_index):
        combo = QtWidgets.QComboBox(self.import_table)
        combo.addItems(self.IMPORT_TYPES)
        index = combo.findText(value)
        combo.setCurrentIndex(index if index >= 0 else 0)
        combo.currentTextChanged.connect(lambda _text, row=row_index: self._mark_row_dirty(row))
        return combo

    def _build_search_re_widget(self, value, row_index):
        widget = QtWidgets.QLineEdit(value, self.import_table)
        widget.setPlaceholderText(r'.*\.JPG$')
        widget.textChanged.connect(lambda _text, row=row_index: self._mark_row_dirty(row))
        min_width = widget.fontMetrics().horizontalAdvance(value or '.*') + 32
        widget.setMinimumWidth(max(140, min_width))
        return widget

    def _browse_query_from(self, line_edit):
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            'Select Folder',
            line_edit.text().strip() or '',
        )
        if directory:
            line_edit.setText(directory)

    def _query_from_input(self, row_index):
        widget = self.import_table.cellWidget(row_index, self.QUERY_FROM_COLUMN)
        return getattr(widget, 'line_edit', None)

    def _import_type_input(self, row_index):
        widget = self.import_table.cellWidget(row_index, self.IMPORT_TYPE_COLUMN)
        return widget if isinstance(widget, QtWidgets.QComboBox) else None

    def _search_re_input(self, row_index):
        widget = self.import_table.cellWidget(row_index, self.SEARCH_RE_COLUMN)
        return widget if isinstance(widget, QtWidgets.QLineEdit) else None

    def _fetch_tables(self):
        valid, _ = validate_connection_values(self.connection_values)
        if not valid:
            return []

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        try:
            with psycopg2.connect(**connection_kwargs(self.connection_values)) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                        """,
                        (schema_name,),
                    )
                    return [row[0] for row in cursor.fetchall() if row[0] != 'spatial_ref_sys']
        except Exception:  # pragma: no cover
            return []

    def _build_database_url(self):
        values = self.connection_values
        database = quote_plus(values.get('database', '').strip())
        service = values.get('service', '').strip()
        user = quote_plus(values.get('user', '').strip())
        password = quote_plus(values.get('password', ''))
        auth = ''
        if user:
            auth = user
            if values.get('password', ''):
                auth = '{}:{}'.format(auth, password)
            auth = '{}@'.format(auth)
        if service:
            return 'postgresql+psycopg2://{auth}/{database}'.format(
                auth=auth,
                database=database,
            )

        host = values.get('host', '').strip()
        port = values.get('port', '').strip() or '5432'
        return 'postgresql+psycopg2://{auth}{host}:{port}/{database}'.format(
            auth=auth,
            host=host,
            port=port,
            database=database,
        )

    def _engine_connect_args(self):
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        service = self.connection_values.get('service', '').strip()
        connect_args = {
            'options': '-csearch_path={},public'.format(schema_name),
        }
        if service:
            connect_args['service'] = service
        user = self.connection_values.get('user', '').strip()
        password = self.connection_values.get('password', '')
        if user:
            connect_args['user'] = user
        if password:
            connect_args['password'] = password
        return connect_args

    def _show_message(self, message, level):
        if self.iface is not None:
            self.iface.messageBar().pushMessage('Landlensdb', message, level=level, duration=6)

    def _build_actions_widget(self, row_index, include_update_new):
        widget = QtWidgets.QWidget(self.import_table)
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        primary_button = QtWidgets.QPushButton('Update' if include_update_new else 'Add')
        primary_button.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        primary_button.setToolTip(
            'Update this trio from disk into the selected table.'
            if include_update_new else
            'Add this trio to the selected table.'
        )
        primary_button.clicked.connect(
            lambda _=False, row=row_index: self.run_row_update(row)
        )
        layout.addWidget(primary_button)

        if include_update_new:
            update_new_button = QtWidgets.QPushButton('Update New')
            update_new_button.setSizePolicy(
                QtWidgets.QSizePolicy.Fixed,
                QtWidgets.QSizePolicy.Fixed,
            )
            update_new_button.setToolTip('Update this trio, skipping images already in PostgreSQL.')
            update_new_button.clicked.connect(
                lambda _=False, row=row_index: self.run_row_update(row, skip_existing=True)
            )
            layout.addWidget(update_new_button)

            drop_old_button = QtWidgets.QPushButton('Drop Old')
            drop_old_button.setSizePolicy(
                QtWidgets.QSizePolicy.Fixed,
                QtWidgets.QSizePolicy.Fixed,
            )
            drop_old_button.setToolTip('Remove rows for this trio whose files no longer exist on disk.')
            drop_old_button.clicked.connect(
                lambda _=False, row=row_index: self.run_row_drop_old(row)
            )
            layout.addWidget(drop_old_button)

            drop_all_button = QtWidgets.QPushButton('Drop All')
            drop_all_button.setSizePolicy(
                QtWidgets.QSizePolicy.Fixed,
                QtWidgets.QSizePolicy.Fixed,
            )
            drop_all_button.setToolTip('Remove all rows currently stored for this trio.')
            drop_all_button.clicked.connect(
                lambda _=False, row=row_index: self.run_row_drop_all(row)
            )
            layout.addWidget(drop_all_button)

            sync_button = QtWidgets.QPushButton('Sync')
            sync_button.setSizePolicy(
                QtWidgets.QSizePolicy.Fixed,
                QtWidgets.QSizePolicy.Fixed,
            )
            sync_button.setToolTip('Drop old rows, then update this trio from disk.')
            sync_button.clicked.connect(
                lambda _=False, row=row_index: self.run_row_sync(row)
            )
            layout.addWidget(sync_button)

        return widget

    def _build_status_widget(self, row_index, original_trio, row_count, editable):
        widget = QtWidgets.QWidget(self.import_table)
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(6)

        left_button = QtWidgets.QToolButton(widget)
        left_button.setAutoRaise(True)
        left_button.setFixedSize(18, 18)

        right_label = QtWidgets.QLabel(str(row_count) if editable else '', widget)
        right_label.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)

        sync_button = QtWidgets.QToolButton(widget)
        sync_button.setAutoRaise(True)
        sync_button.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload))
        sync_button.setIconSize(QtCore.QSize(16, 16))
        sync_button.setToolTip('Drop all rows for the old trio, then update using the edited values.')
        sync_button.clicked.connect(lambda _=False, row=row_index: self.run_row_edit_sync(row))
        sync_button.hide()

        layout.addWidget(left_button)
        layout.addWidget(right_label)
        layout.addWidget(sync_button)

        widget.original_trio = tuple(original_trio) if original_trio else None
        widget.row_count = int(row_count or 0)
        widget.left_button = left_button
        widget.right_label = right_label
        widget.sync_button = sync_button
        widget.editable = editable
        widget.dirty = False

        left_button.clicked.connect(lambda _=False, row=row_index: self._handle_status_left_click(row))
        return widget

    def _status_widget(self, row_index):
        return self.import_table.cellWidget(row_index, self.STATUS_COLUMN)

    def _current_trio(self, row_index):
        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            return None
        return (
            query_from_input.text().strip(),
            import_type_input.currentText().strip(),
            search_re_input.text().strip(),
        )

    def _mark_row_dirty(self, row_index):
        status_widget = self._status_widget(row_index)
        if status_widget is None or not getattr(status_widget, 'editable', False):
            return
        original_trio = getattr(status_widget, 'original_trio', None)
        current_trio = self._current_trio(row_index)
        status_widget.dirty = current_trio != original_trio
        self._update_status_widget(row_index)

    def _update_status_widget(self, row_index):
        status_widget = self._status_widget(row_index)
        if status_widget is None:
            return

        left_button = status_widget.left_button
        right_label = status_widget.right_label
        sync_button = status_widget.sync_button

        if not status_widget.editable:
            left_button.hide()
            right_label.setText('')
            sync_button.hide()
            return

        left_button.show()
        if status_widget.dirty:
            left_button.setText('x')
            left_button.setToolTip('Exit edit mode and revert this trio to its saved values.')
            left_button.setStyleSheet('QToolButton { color: #c53030; font-size: 16px; font-weight: bold; }')
            right_label.hide()
            sync_button.show()
        else:
            left_button.setText('●')
            left_button.setToolTip('This trio matches the saved values.')
            left_button.setStyleSheet('QToolButton { color: #1f9d55; font-size: 14px; }')
            right_label.setText(str(status_widget.row_count))
            right_label.show()
            sync_button.hide()

    def _handle_status_left_click(self, row_index):
        status_widget = self._status_widget(row_index)
        if status_widget is None or not status_widget.editable or not status_widget.dirty:
            return
        self._revert_row_to_original(row_index)

    def _revert_row_to_original(self, row_index):
        status_widget = self._status_widget(row_index)
        if status_widget is None or not status_widget.original_trio:
            return

        query_from, import_type, search_re = status_widget.original_trio
        import_type_input = self._import_type_input(row_index)
        query_from_input = self._query_from_input(row_index)
        search_re_input = self._search_re_input(row_index)
        if query_from_input is None or import_type_input is None or search_re_input is None:
            return

        query_from_input.blockSignals(True)
        import_type_input.blockSignals(True)
        search_re_input.blockSignals(True)
        query_from_input.setText(query_from)
        import_type_input.setCurrentIndex(max(import_type_input.findText(import_type), 0))
        search_re_input.setText(search_re)
        query_from_input.blockSignals(False)
        import_type_input.blockSignals(False)
        search_re_input.blockSignals(False)

        status_widget.dirty = False
        self._update_status_widget(row_index)

    def run_row_edit_sync(self, row_index):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        status_widget = self._status_widget(row_index)
        if status_widget is None or not status_widget.editable or not status_widget.dirty:
            return

        old_trio = getattr(status_widget, 'original_trio', None)
        new_trio = self._current_trio(row_index)
        if not old_trio:
            self._show_message('No saved trio exists for this row.', Qgis.Critical)
            return
        if not new_trio or not all(new_trio):
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            self._run_drop_all(table_name, old_trio[0], old_trio[1], old_trio[2])
            self._run_import_update(
                table_name,
                new_trio[0],
                new_trio[1],
                new_trio[2],
                skip_existing=False,
            )
        except ImportCancelledError:
            self._show_message('Row sync cancelled.', Qgis.Warning)
            self.refresh_table()
            return
        except Exception as exc:  # pragma: no cover
            self._show_message('Row sync failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message('Saved edited trio for "{}".'.format(table_name), Qgis.Info)
        self.refresh_table()

    def _run_import_update(self, table_name, query_from, import_type, search_re, skip_existing=False):
        self._cancel_import_event.clear()
        self._set_import_active(True)
        self._reset_progress()
        db = Postgres(self._build_database_url())
        db.engine = create_engine(
            self._build_database_url(),
            connect_args=self._engine_connect_args(),
        )
        if skip_existing:
            db.table(table_name)
        try:
            images = SearchLocalToGeoImageFrame(
                query_from,
                import_types={import_type: search_re},
                max_workers=self.thread_count_input.value(),
                progress_callback=self._update_progress,
                skip_images_in_postgresql=db if skip_existing else None,
                cancel_event=self._cancel_import_event,
            )
            db.upsert_images(images, table_name, conflict='update')
        finally:
            self._set_import_active(False)

    def _run_drop_old(self, table_name, query_from, import_type, search_re):
        db = Postgres(self._build_database_url())
        db.engine = create_engine(
            self._build_database_url(),
            connect_args=self._engine_connect_args(),
        )
        db.table(table_name)
        return db.remove_unmatched(
            query_from,
            import_types={import_type: search_re},
        )

    def _run_drop_all(self, table_name, query_from, import_type, search_re):
        db = Postgres(self._build_database_url())
        db.engine = create_engine(
            self._build_database_url(),
            connect_args=self._engine_connect_args(),
        )
        db.table(table_name)
        return db.remove_all(
            query_from,
            import_types={import_type: search_re},
        )

    def _reset_progress(self):
        self.import_progress_bar.setRange(0, 1)
        self.import_progress_bar.setValue(0)
        self.import_progress_label.setText('0/0')

    def _set_import_active(self, active):
        self._import_active = bool(active)
        if self._import_active:
            self.cancel_import_button.setStyleSheet(
                'QToolButton { color: #c53030; font-size: 16px; font-weight: bold; }'
            )
        else:
            self.cancel_import_button.setStyleSheet(
                'QToolButton { color: #a0aec0; font-size: 16px; font-weight: bold; }'
            )

    def _cancel_active_import(self):
        if not self._import_active:
            return
        self._cancel_import_event.set()

    def _update_progress(self, processed, total):
        total = max(int(total or 0), 0)
        processed = max(min(int(processed or 0), total if total else 0), 0)
        maximum = total if total > 0 else 1
        self.import_progress_bar.setRange(0, maximum)
        self.import_progress_bar.setValue(processed)
        self.import_progress_label.setText('{}/{}'.format(processed, total))
        app = QtWidgets.QApplication.instance()
        if app is not None:
            app.processEvents()
