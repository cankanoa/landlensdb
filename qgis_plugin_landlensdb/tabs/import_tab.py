# -*- coding: utf-8 -*-

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
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

from ..landlensdb import SearchLocalToGeoImageFrame, Postgres



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
    HEADERS = ['query_from', 'import_type', 'search_re', 'Actions']
    ADD_TABLE_SENTINEL = '__add_table__'

    def __init__(self, iface, parent=None):
        super(ImportTab, self).__init__(parent)
        self.iface = iface
        self.connection_values = load_connection_settings()

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        top_row = QtWidgets.QHBoxLayout()
        top_row.addWidget(QtWidgets.QLabel('Table:'))
        self.table_combo = QtWidgets.QComboBox()
        self.table_combo.setSizeAdjustPolicy(QtWidgets.QComboBox.AdjustToContents)
        top_row.addWidget(self.table_combo, 1)
        self.drop_table_button = QtWidgets.QToolButton()
        self.drop_table_button.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_TrashIcon))
        self.drop_table_button.setToolTip('Drop selected table')
        top_row.addWidget(self.drop_table_button)
        self.update_button = QtWidgets.QPushButton('Update')
        top_row.addWidget(self.update_button)
        layout.addLayout(top_row)

        self.import_table = QtWidgets.QTableWidget(self)
        self.import_table.setColumnCount(len(self.HEADERS))
        self.import_table.setHorizontalHeaderLabels(self.HEADERS)
        self.import_table.verticalHeader().setVisible(False)
        self.import_table.setAlternatingRowColors(True)
        self.import_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.import_table.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_table.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.import_table)

        button_row = QtWidgets.QHBoxLayout()
        self.connection_button = QtWidgets.QPushButton('Connection')
        button_row.addWidget(self.connection_button)
        button_row.addStretch()
        layout.addLayout(button_row)

        self.connection_button.clicked.connect(self.open_connection_dialog)
        self.update_button.clicked.connect(self.refresh_table)
        self.drop_table_button.clicked.connect(self.drop_selected_table)
        self.table_combo.activated.connect(self._handle_table_choice)

        self._update_connection_button_text()
        self._refresh_table_choices()
        self.load_records([])

    def showEvent(self, event):
        super(ImportTab, self).showEvent(event)
        self._refresh_table_choices()

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

    def _handle_table_choice(self, index):
        if self.table_combo.itemData(index) == self.ADD_TABLE_SENTINEL:
            self.add_table()

    def _refresh_table_choices(self, selected_table=None):
        current_table = selected_table or self.current_table_name()
        tables = self._fetch_tables()

        self.table_combo.blockSignals(True)
        self.table_combo.clear()
        for table_name in tables:
            self.table_combo.addItem(table_name, table_name)
        self.table_combo.addItem('Add Table...', self.ADD_TABLE_SENTINEL)

        if current_table and current_table in tables:
            self.table_combo.setCurrentIndex(tables.index(current_table))
        elif tables:
            self.table_combo.setCurrentIndex(0)
        else:
            self.table_combo.setCurrentIndex(self.table_combo.count() - 1)
        self.table_combo.blockSignals(False)

        has_real_table = bool(tables)
        self.drop_table_button.setEnabled(has_real_table and self.current_table_name() is not None)

    def current_table_name(self):
        data = self.table_combo.currentData()
        if data == self.ADD_TABLE_SENTINEL:
            return None
        return data

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
        self._show_message('Created table "{}".'.format(table_name), Qgis.Info)

    def drop_selected_table(self):
        table_name = self.current_table_name()
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
                            SELECT DISTINCT
                                metadata::jsonb->'input_params'->>'query_from' AS query_from,
                                metadata::jsonb->'input_params'->>'import_type' AS import_type,
                                metadata::jsonb->'input_params'->>'search_re' AS search_re
                            FROM {}.{}
                            WHERE metadata::jsonb ? 'input_params'
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
        unique_rows = []
        seen = set()

        for record in records or []:
            metadata = record.get('metadata') if isinstance(record, dict) else None
            input_params = metadata.get('input_params', {}) if isinstance(metadata, dict) else {}
            row = (
                str(input_params.get('query_from', '') or ''),
                str(input_params.get('import_type', '') or ''),
                str(input_params.get('search_re', '') or ''),
            )
            if row in seen or not any(row):
                continue
            seen.add(row)
            unique_rows.append(row)

        self.import_table.clearContents()
        self.import_table.setRowCount(len(unique_rows))
        for row_index, row_values in enumerate(unique_rows):
            for column_index, value in enumerate(row_values):
                item = QtWidgets.QTableWidgetItem(value)
                item.setFlags(item.flags() | QtCore.Qt.ItemIsEditable)
                self.import_table.setItem(row_index, column_index, item)

            button = QtWidgets.QPushButton('Update')
            button.clicked.connect(lambda _=False, row=row_index: self.run_row_update(row))
            self.import_table.setCellWidget(row_index, 3, button)

        if not unique_rows:
            self.import_table.setRowCount(1)
            empty_item = QtWidgets.QTableWidgetItem('No import parameter sets yet')
            empty_item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.import_table.setItem(0, 0, empty_item)
            for column_index in range(1, len(self.HEADERS)):
                spacer_item = QtWidgets.QTableWidgetItem('')
                spacer_item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
                self.import_table.setItem(0, column_index, spacer_item)

        self.import_table.resizeColumnsToContents()

    def run_row_update(self, row_index):
        table_name = self.current_table_name()
        if not table_name:
            self._show_message('Choose a table first.', Qgis.Critical)
            return

        query_from_item = self.import_table.item(row_index, 0)
        import_type_item = self.import_table.item(row_index, 1)
        search_re_item = self.import_table.item(row_index, 2)
        if query_from_item is None or import_type_item is None or search_re_item is None:
            self._show_message('Import row is incomplete.', Qgis.Critical)
            return

        query_from = query_from_item.text().strip()
        import_type = import_type_item.text().strip()
        search_re = search_re_item.text().strip()
        if not query_from or not import_type or not search_re:
            self._show_message('query_from, import_type, and search_re are required.', Qgis.Critical)
            return

        try:
            images = SearchLocalToGeoImageFrame(
                query_from,
                import_types={import_type: search_re},
            )
            db = Postgres(self._build_database_url())
            db.engine = create_engine(
                self._build_database_url(),
                connect_args=self._engine_connect_args(),
            )
            db.upsert_images(images, table_name, conflict='update')
        except Exception as exc:  # pragma: no cover
            self._show_message('Import update failed: {}'.format(exc), Qgis.Critical)
            return

        self._show_message('Updated "{}" from {}.'.format(table_name, query_from), Qgis.Info)
        self.refresh_table()

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
                    return [row[0] for row in cursor.fetchall()]
        except Exception:  # pragma: no cover
            return []

    def _build_database_url(self):
        values = self.connection_values
        database = quote_plus(values.get('database', '').strip())
        service = values.get('service', '').strip()
        if service:
            return 'postgresql+psycopg2:///{database}'.format(database=database)

        host = values.get('host', '').strip()
        port = values.get('port', '').strip() or '5432'
        return 'postgresql+psycopg2://{host}:{port}/{database}'.format(
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
        return connect_args

    def _show_message(self, message, level):
        if self.iface is not None:
            self.iface.messageBar().pushMessage('Landlensdb', message, level=level, duration=6)
