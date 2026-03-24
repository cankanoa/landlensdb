# -*- coding: utf-8 -*-
"""
/***************************************************************************
 QueryTab
                                 A QGIS plugin
 Query workflow for loading PostGIS layers into QGIS
 ***************************************************************************/
"""
import os
import re
from datetime import datetime

from qgis.PyQt import QtCore, QtWidgets, uic
from qgis.PyQt.QtWidgets import QAbstractItemView
from qgis.core import (
    Qgis,
    QgsDataSourceUri,
    QgsFeatureRequest,
    QgsLayerTreeGroup,
    QgsProject,
    QgsRasterLayer,
    QgsVectorLayer,
    QgsWkbTypes,
)

from ..shared.connection_dialog import ConnectionDialog
from ..shared.connection_utils import (
    connection_kwargs,
    load_connection_settings,
    save_connection_settings,
    test_connection_values,
    validate_connection_values,
)
from ..shared.import_params import import_parameter_label, normalize_import_parameter_row
from .query_components import QueryHistoryController, ResultsController, SqlBuilderController

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:  # pragma: no cover - depends on QGIS runtime
    psycopg2 = None
    sql = None


FORM_CLASS, _ = uic.loadUiType(
    os.path.join(os.path.dirname(__file__), '..', 'landlensdb_dialog_base.ui')
)


class QueryTab(QtWidgets.QWidget, FORM_CLASS):
    connectionSaved = QtCore.pyqtSignal(dict)

    HISTORY_KEY = 'Landlensdb/query_history'
    STAR_KEY = 'Landlensdb/starred_queries'
    NAME_KEY = 'Landlensdb/query_names'
    PREVIEW_LIMIT = 10
    HISTORY_LIMIT = 25
    KEY_COLUMN = '__lldb_rowid__'
    SIMPLE_SELECT_RE = re.compile(
        r'^\s*SELECT\s+\*\s+FROM\s+(?:"(?P<schema_q>[^"]+)"|"?(?P<schema_u>[\w]+)"?)\.(?:"(?P<table_q>[^"]+)"|"?(?P<table_u>[\w]+)"?)'
        r'(?:\s+(?:AS\s+)?(?:"?[\w]+"?))?'
        r'(?:\s+WHERE\s+(?P<where>.+?))?'
        r'\s*$',
        re.IGNORECASE | re.DOTALL,
    )
    STATIC_ROW_ONE = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'GROUP BY', 'ORDER BY', 'LIMIT']
    STATIC_ROW_TWO = ['*', 'DISTINCT', 'COUNT(*)', 'SUM()', 'AVG()', 'MIN()', 'MAX()', 'AND', 'OR', 'NOT', 'IN ()', 'LIKE', 'IS NULL', 'IS NOT NULL']
    STATIC_ROW_THREE = ['ST_Intersects()', 'ST_Within()', 'ST_Contains()', 'ST_DWithin()', 'ST_Touches()', 'ST_Crosses()', '::geometry', '::geography']
    QUERY_EXAMPLE = 'SELECT  *  FROM "<schema>"."<table>";'

    def __init__(self, iface, parent=None):
        super(QueryTab, self).__init__(parent)
        self.iface = iface
        self.setupUi(self)

        self.connection_values = {}
        self._metadata_loaded = False
        self._last_query_state = None

        self.connection_button.clicked.connect(self.open_connection_dialog)
        self.query_button.clicked.connect(self.run_query)
        self.add_button.clicked.connect(self.add_last_query_to_map)
        self.close_button.clicked.connect(self._close_parent_dialog)
        self.commands_toggle_button.toggled.connect(self._toggle_commands)
        self.history_menu_button.clicked.connect(self._show_history_menu)
        self.star_menu_button.clicked.connect(self._show_star_menu)

        self.results_table.setColumnCount(0)
        self.results_table.setRowCount(0)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.verticalHeader().setVisible(False)
        self.results_table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.results_table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.add_button.setEnabled(False)

        self.builder_controller = SqlBuilderController(
            self,
            self.sql_input,
            self.commands_frame,
            self.commands_toggle_button,
            self.commands_content_widget,
        )
        self.builder_controller.prepare_ui()

        self.history_controller = QueryHistoryController(
            self,
            self.sql_input,
            self.history_menu_button,
            self.star_menu_button,
            self.HISTORY_KEY,
            self.STAR_KEY,
            self.NAME_KEY,
            self.HISTORY_LIMIT,
        )
        self.results_controller = ResultsController(
            self,
            self.results_tab,
            self.results_tab_layout,
            self.results_label,
            self.results_table,
            self.PREVIEW_LIMIT,
            self._update_results_preview,
        )
        self.results_controller.setup()
        self._prepare_workbench_ui()
        self._add_builder_help_button()

        self._load_settings()
        self._populate_static_buttons()
        self._build_history_menu()
        self._build_star_menu()
        self._render_dynamic_buttons([], [])
        self._update_connection_button_text()
        self._set_results_label(0, 0)

    def showEvent(self, event):
        super(QueryTab, self).showEvent(event)
        self.sql_input.setFocus()
        if not self._metadata_loaded:
            self._refresh_schema_buttons(silent=True)
            self._metadata_loaded = True

    def reload_connection_settings(self, values=None):
        self.connection_values = dict(values or load_connection_settings())
        self._update_connection_button_text()
        self._refresh_schema_buttons(silent=False)

    def _load_settings(self):
        self.connection_values = load_connection_settings()
        self.history_controller.load()

    def _save_settings(self):
        save_connection_settings(self.connection_values)
        self.history_controller.save()

    def _append_status(self, message):
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.status_output.appendPlainText('[{}] {}'.format(timestamp, message))

    def _show_error(self, message):
        self._append_status(message)
        self.bottom_tabs.setCurrentWidget(self.logs_tab)
        if self.iface is not None:
            self.iface.messageBar().pushMessage('Landlensdb', message, level=Qgis.Critical, duration=8)

    def _show_info(self, message):
        self._append_status(message)
        if self.iface is not None:
            self.iface.messageBar().pushMessage('Landlensdb', message, level=Qgis.Info, duration=5)

    def _close_parent_dialog(self):
        window = self.window()
        if isinstance(window, QtWidgets.QDialog):
            window.close()

    def _prepare_workbench_ui(self):
        if hasattr(self, 'headerLayout'):
            while self.headerLayout.count():
                item = self.headerLayout.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
        if hasattr(self, 'verticalLayout'):
            self.verticalLayout.setContentsMargins(18, 18, 18, 18)
            self.verticalLayout.setSpacing(10)
        if hasattr(self, 'commands_frame'):
            self.commands_frame.setFrameShape(QtWidgets.QFrame.NoFrame)
            self.commands_frame.setLineWidth(0)
        if hasattr(self, 'commands_frame_layout'):
            self.commands_frame_layout.setContentsMargins(0, 0, 0, 0)
            self.commands_frame_layout.setSpacing(6)
        if hasattr(self, 'commandsHeaderLayout'):
            self.commandsHeaderLayout.setContentsMargins(0, 0, 0, 0)
            self.commandsHeaderLayout.setSpacing(6)
        if hasattr(self, 'commandsContentLayout'):
            self.commandsContentLayout.setContentsMargins(0, 0, 0, 0)
            self.commandsContentLayout.setSpacing(4)
        if hasattr(self, 'commands_scroll_layout'):
            self.commands_scroll_layout.setContentsMargins(0, 0, 0, 0)
            self.commands_scroll_layout.setSpacing(4)
        if hasattr(self, 'commands_scroll'):
            self.commands_scroll.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.sql_input.setPlaceholderText(self._sql_placeholder_text())
        self.commands_toggle_button.setStyleSheet('QToolButton { color: white; }')
        self.history_menu_button.setStyleSheet('QToolButton { color: white; }')
        self.star_menu_button.setStyleSheet('QToolButton { color: white; }')

    def _add_builder_help_button(self):
        if not hasattr(self, 'commandsHeaderLayout'):
            return
        self.builder_help_button = QtWidgets.QToolButton(self)
        self.builder_help_button.setText('?')
        self.builder_help_button.setAutoRaise(True)
        self.builder_help_button.setToolTip('How the builder works')
        self.builder_help_button.clicked.connect(self._show_builder_help)
        self.commandsHeaderLayout.insertWidget(0, self.builder_help_button)

    def _show_builder_help(self):
        QtWidgets.QMessageBox.information(self, 'Builder', self._builder_help_text())

    def _sql_placeholder_text(self):
        return self.QUERY_EXAMPLE

    def _builder_help_text(self):
        return (
            "Use the builder buttons to help write SQL, then click Query to preview "
            "the returned rows and Add to load them into QGIS.\n\n"
            "Example:\n{}".format(self._sql_placeholder_text())
        )

    def _update_connection_button_text(self):
        label = self.connection_values.get('name') or self.connection_values.get('database') or 'Connection'
        self.connection_button.setText('Connection' if label == 'Connection' else 'Connection: {}'.format(label))

    def _toggle_commands(self, checked):
        self.builder_controller.toggle_commands(checked)

    def _layout_for_name(self, name):
        return getattr(self, name)

    def _clear_layout(self, layout):
        self.builder_controller.clear_layout(layout)

    def _make_insert_button(self, label, insert_text=None):
        return self.builder_controller.make_insert_button(label, insert_text)

    def _set_row_buttons(self, layout_name, items):
        layout = self._layout_for_name(layout_name)
        self.builder_controller.set_row_buttons(layout, items)

    def _populate_static_buttons(self):
        self._set_row_buttons('row_one_layout', [(item, item) for item in self.STATIC_ROW_ONE])
        self._set_row_buttons('row_two_layout', [(item, item) for item in self.STATIC_ROW_TWO])
        self._set_row_buttons('row_three_layout', [(item, item) for item in self.STATIC_ROW_THREE])
        self._set_row_buttons('row_five_layout', [('Spatial Query', None)])
        spatial_button = self.row_five_layout.itemAt(0).widget()
        if spatial_button is not None:
            try:
                spatial_button.clicked.disconnect()
            except TypeError:
                pass
            spatial_button.clicked.connect(self._open_spatial_query_dialog)

    def _render_dynamic_buttons(self, tables, columns):
        table_items = [(table, table) for table in tables] or [('No tables', None)]
        column_items = [(column, column) for column in columns] or [('No columns', None)]
        self._set_row_buttons('row_four_layout', table_items + column_items)
        layout = self._layout_for_name('row_four_layout')
        if layout.count() > 1 and layout.itemAt(0).widget() and layout.itemAt(0).widget().text().startswith('No '):
            layout.itemAt(0).widget().setEnabled(False)

    def _insert_sql(self, token):
        self.builder_controller.insert_sql(token)

    def _build_history_menu(self):
        self.history_controller.build_history_menu()

    def _show_history_menu(self):
        self.history_controller.show_history_menu()

    def _build_star_menu(self):
        self.history_controller.build_star_menu()

    def _show_star_menu(self):
        self.history_controller.show_star_menu()

    def _query_title(self, query):
        return self.history_controller.query_title(query)

    def _rename_query(self, query):
        self.history_controller.rename_query(query)

    def _unname_query(self, query):
        self.history_controller.unname_query(query)

    def _add_history_item(self, sql_text):
        self.history_controller.add_history_item(sql_text)

    def _remove_history_item(self, index):
        self.history_controller.remove_history_item(index)

    def clear_history(self):
        self.history_controller.clear_history()

    def _star_history_item(self, index):
        self.history_controller.star_history_item(index)

    def _unstar_item(self, index):
        self.history_controller.unstar_item(index)

    def _remove_star_item(self, index):
        self.history_controller.remove_star_item(index)

    def clear_starred(self):
        self.history_controller.clear_starred()

    def open_connection_dialog(self):
        dialog = ConnectionDialog(self.connection_values, self._test_connection_values, self)
        if dialog.exec_():
            self.connection_values = dialog.values()
            self._save_settings()
            self._update_connection_button_text()
            self._refresh_schema_buttons(silent=False)
            self.connectionSaved.emit(dict(self.connection_values))

    def _validate_connection_values(self, values):
        return validate_connection_values(values)

    def _connection_kwargs(self, values=None):
        return connection_kwargs(values or self.connection_values)

    def _test_connection_values(self, values):
        success, message = test_connection_values(values)
        if success:
            self.connection_values = dict(values)
            self._save_settings()
            self._update_connection_button_text()
            self._show_info('Connection successful')
            self._refresh_schema_buttons(silent=True)
            self.connectionSaved.emit(dict(self.connection_values))
        return success, message

    def _refresh_schema_buttons(self, silent):
        valid, message = self._validate_connection_values(self.connection_values)
        if not valid:
            if not silent:
                self._show_error(message)
            self._render_dynamic_buttons([], [])
            return

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    cursor.execute(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                        """,
                        (schema_name,),
                    )
                    tables = ['"{}"."{}"'.format(schema_name, row[0]) for row in cursor.fetchall()]
                    cursor.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = %s
                        ORDER BY table_name, ordinal_position
                        """,
                        (schema_name,),
                    )
                    columns = ['"{}"'.format(row[0]) for row in cursor.fetchall()]
        except Exception as exc:  # pragma: no cover - depends on external DB
            if not silent:
                self._show_error('Could not load schema metadata: {}'.format(exc))
            return

        self._render_dynamic_buttons(tables, columns)

    def run_query(self):
        self._run_query_preview(add_to_history=True)

    def _run_query_preview(self, add_to_history):
        start_row, end_row = self._validated_preview_range()
        if end_row == start_row:
            self._show_error('Results range must span at least one row.')
            return

        valid, message = self._validate_connection_values(self.connection_values)
        if not valid:
            self._show_error(message)
            return

        sql_text = self.sql_input.toPlainText().strip().rstrip(';')
        if not sql_text:
            self._show_error('Missing required fields: SQL')
            return

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        query_name = 'Query {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        live_query = self._build_live_query(sql_text)
        raster_source = self._parse_simple_raster_source(sql_text)

        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    column_info = self._get_column_info(cursor, live_query)
                    row_count = self._get_row_count(cursor, live_query)
                    preview_rows = self._get_preview_rows(
                        cursor,
                        live_query,
                        column_info,
                        start_row,
                        end_row,
                    )
                    raster_key_columns = self._get_raster_key_columns(cursor, raster_source)
                    raster_columns = [column['name'] for column in column_info if column['udt_name'] == 'raster']
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Query failed: {}'.format(exc))
            return

        vector_column = self._find_first_column(column_info, {'geometry', 'geography'})
        import_groups = []
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    import_groups = self._get_query_import_groups(cursor, live_query, column_info)
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Could not inspect import groups: {}'.format(exc))
            return
        self._populate_preview(column_info, preview_rows, row_count)
        if add_to_history:
            self._add_history_item(sql_text)
        self._last_query_state = {
            'query_name': query_name,
            'live_query': live_query,
            'column_info': column_info,
            'column_names': [column['name'] for column in column_info],
            'raster_source': raster_source,
            'vector_column': vector_column,
            'import_groups': import_groups,
            'raster_columns': raster_columns,
            'raster_key_columns': raster_key_columns,
            'row_count': row_count,
        }
        self.add_button.setEnabled(True)
        self.bottom_tabs.setCurrentWidget(self.results_tab)
        self._show_info('Previewed {} row(s). Click Add to load layers into the map.'.format(row_count))

    def add_last_query_to_map(self):
        if not self._last_query_state:
            self._show_error('Run Query first to preview results before adding to the map.')
            return

        query_name = self._last_query_state['query_name']
        root_group = self._ensure_query_group(query_name)
        added_layers = []
        import_groups = self._last_query_state.get('import_groups') or [None]
        vector_column = self._last_query_state['vector_column']
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'

        for import_group in import_groups:
            group_name = import_parameter_label(import_group) if import_group else query_name
            group = root_group.addGroup(group_name)
            filtered_query = self._build_import_group_query(
                self._last_query_state['live_query'],
                import_group,
            )

            geometry_types = []
            if vector_column:
                try:
                    with psycopg2.connect(**self._connection_kwargs()) as connection:
                        with connection.cursor() as cursor:
                            if schema_name:
                                self._set_search_path(cursor, schema_name)
                            geometry_types = self._get_geometry_types(cursor, filtered_query, vector_column)
                except Exception as exc:  # pragma: no cover - depends on external DB
                    self._show_error('Could not inspect geometry types: {}'.format(exc))
                    continue

                for vector_layer in self._create_geometry_layers(
                    filtered_query,
                    vector_column,
                    group_name,
                    geometry_types,
                ):
                    added_layers.append(vector_layer.name())
                    self._add_layer_to_group(group, vector_layer)

            if import_group and import_group[1] in ('GeoTransformImage', 'WorldView3Image'):
                try:
                    with psycopg2.connect(**self._connection_kwargs()) as connection:
                        with connection.cursor() as cursor:
                            if schema_name:
                                self._set_search_path(cursor, schema_name)
                            raster_row_map = self._get_raster_row_map(
                                cursor,
                                self._last_query_state['raster_source'],
                                self._last_query_state['raster_key_columns'],
                                self._last_query_state['raster_columns'],
                                import_group=import_group,
                            )
                except Exception as exc:  # pragma: no cover - depends on external DB
                    self._show_error('Could not inspect raster rows: {}'.format(exc))
                    continue

                for raster_column, row_filters in raster_row_map.items():
                    for row_filter, row_label in row_filters:
                        raster_layer = self._create_raster_layer(
                            self._last_query_state['raster_source'],
                            raster_column,
                            row_filter,
                            '{} {} {}'.format(group_name, raster_column, row_label),
                        )
                        if raster_layer is not None:
                            added_layers.append(raster_layer.name())
                            self._add_layer_to_group(group, raster_layer, insert_at_top=True)

        if added_layers:
            self._show_info(
                'Loaded {} rows into {} layer(s) under "{}".'.format(
                    self._last_query_state['row_count'],
                    len(added_layers),
                    query_name,
                )
            )
        else:
            self._show_error('Nothing was added to the map from the current preview.')

    def _build_live_query(self, sql_text):
        return 'SELECT row_number() OVER () AS {key}, src.* FROM ({sql}) AS src'.format(
            key=self.KEY_COLUMN,
            sql=sql_text,
        )

    def _parse_simple_raster_source(self, sql_text):
        match = self.SIMPLE_SELECT_RE.match(sql_text.strip())
        if not match:
            return None
        return {
            'schema': match.group('schema_q') or match.group('schema_u') or 'public',
            'table': match.group('table_q') or match.group('table_u'),
            'where': (match.group('where') or '').strip(),
        }

    def _open_spatial_query_dialog(self):
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            'Select Vector File',
            '',
            'Vector files (*.gpkg *.geojson *.json *.shp *.kml);;All files (*)',
        )
        if not file_path:
            return

        layer = QgsVectorLayer(file_path, 'spatial_query_source', 'ogr')
        if not layer.isValid():
            self._show_error('Could not open the selected vector file.')
            return

        feature = next(layer.getFeatures(QgsFeatureRequest().setLimit(1)), None)
        if feature is None or not feature.hasGeometry():
            self._show_error('The selected vector file has no geometry.')
            return

        geometry = feature.geometry()
        if geometry is None or geometry.isEmpty():
            self._show_error('The selected vector geometry is empty.')
            return

        wkt = geometry.asWkt()
        srid = layer.crs().postgisSrid()
        if srid <= 0:
            srid = 4326

        spatial_function = 'ST_Intersects'
        if QgsWkbTypes.geometryType(layer.wkbType()) == QgsWkbTypes.PointGeometry:
            spatial_function = 'ST_DWithin'
            snippet = "{}(geometry, ST_GeomFromText('{}', {}), 0)".format(
                spatial_function,
                wkt.replace("'", "''"),
                srid,
            )
        else:
            snippet = "{}(geometry, ST_GeomFromText('{}', {}))".format(
                spatial_function,
                wkt.replace("'", "''"),
                srid,
            )
        self._insert_sql(snippet)
        self._show_info('Spatial query text inserted. Adjust it if needed.')

    def _set_search_path(self, cursor, schema_name):
        cursor.execute(
            sql.SQL('SET search_path TO {}, public').format(sql.Identifier(schema_name))
        )

    def _get_column_info(self, cursor, query_text):
        cursor.execute(sql.SQL('SELECT * FROM ({}) AS q LIMIT 0').format(sql.SQL(query_text)))
        descriptions = list(cursor.description or [])
        type_oids = [description.type_code for description in descriptions]
        type_names = {}
        if type_oids:
            cursor.execute('SELECT oid, typname FROM pg_type WHERE oid = ANY(%s)', (type_oids,))
            type_names = {oid: name for oid, name in cursor.fetchall()}
        return [{'name': description.name, 'udt_name': type_names.get(description.type_code, '')} for description in descriptions]

    def _get_preview_rows(self, cursor, query_text, column_info, start_row, end_row):
        select_items = []
        for column in column_info:
            column_name = sql.Identifier(column['name'])
            if column['udt_name'] in ('geometry', 'geography'):
                select_items.append(sql.SQL('ST_AsText(q.{}) AS {}').format(column_name, column_name))
            elif column['udt_name'] == 'raster':
                select_items.append(sql.SQL("'[raster]' AS {}").format(column_name))
            else:
                select_items.append(sql.SQL('q.{}').format(column_name))
        limit_value = max(0, end_row - start_row)
        cursor.execute(
            sql.SQL('SELECT {} FROM ({}) AS q OFFSET {} LIMIT {}').format(
                sql.SQL(', ').join(select_items),
                sql.SQL(query_text),
                sql.Literal(start_row),
                sql.Literal(limit_value),
            )
        )
        return cursor.fetchall()

    def _get_row_count(self, cursor, query_text):
        cursor.execute(sql.SQL('SELECT COUNT(*) FROM ({}) AS q').format(sql.SQL(query_text)))
        return cursor.fetchone()[0]

    def _get_raster_key_columns(self, cursor, raster_source):
        if not raster_source:
            return []
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'image_url'
            """,
            (raster_source['schema'], raster_source['table']),
        )
        if cursor.fetchone():
            self._show_info(
                'Using "image_url" as the raster row key for "{}"."{}".'.format(
                    raster_source['schema'],
                    raster_source['table'],
                )
            )
            return ['image_url']
        cursor.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (raster_source['schema'], raster_source['table']),
        )
        return [row[0] for row in cursor.fetchall()]

    def _table_has_column(self, cursor, raster_source, column_name):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (raster_source['schema'], raster_source['table'], column_name),
        )
        return cursor.fetchone() is not None

    def _get_raster_row_map(self, cursor, raster_source, key_columns, raster_columns, import_group=None):
        raster_row_map = {}
        if not raster_source or not key_columns:
            if raster_source and raster_columns:
                self._show_error(
                    'Raster loading requires a primary key or image_url on "{}"."{}".'.format(
                        raster_source['schema'],
                        raster_source['table'],
                    )
                )
            return raster_row_map

        has_metadata_column = self._table_has_column(cursor, raster_source, 'metadata')

        for raster_column in raster_columns:
            filter_parts = []
            if raster_source['where']:
                filter_parts.append(sql.SQL('({})').format(sql.SQL(raster_source['where'])))
            filter_parts.append(sql.SQL('{} IS NOT NULL').format(sql.Identifier(raster_column)))
            if has_metadata_column:
                if import_group:
                    filter_parts.append(
                        sql.SQL(
                            "coalesce(metadata::jsonb->'input_params'->>'query_from', '') = {}"
                        ).format(sql.Literal(import_group[0]))
                    )
                    filter_parts.append(
                        sql.SQL(
                            "coalesce(metadata::jsonb->'input_params'->>'import_type', '') = {}"
                        ).format(sql.Literal(import_group[1]))
                    )
                    filter_parts.append(
                        sql.SQL(
                            "coalesce(metadata::jsonb->'input_params'->>'search_re', '') = {}"
                        ).format(sql.Literal(import_group[2]))
                    )
                else:
                    filter_parts.append(
                        sql.SQL(
                            "coalesce(metadata::jsonb->'input_params'->>'import_type', '') IN ('GeoTransformImage', 'WorldView3Image')"
                        )
                    )

            key_filter_exprs = []
            key_label_exprs = []
            for key_column in key_columns:
                key_identifier = sql.Identifier(key_column)
                if key_column == 'image_url':
                    key_filter_exprs.append(
                        sql.SQL("""'"{}" = $lldb$' || {}::text || '$lldb$'""").format(
                            sql.SQL(key_column.replace('"', '""')),
                            key_identifier,
                        )
                    )
                else:
                    key_filter_exprs.append(
                        sql.SQL("""'"{}" = ' || quote_nullable({})""").format(
                            sql.SQL(key_column.replace('"', '""')),
                            key_identifier,
                        )
                    )
                key_label_exprs.append(sql.SQL("""coalesce({}::text, 'NULL')""").format(key_identifier))

            cursor.execute(
                sql.SQL('SELECT {}, {} FROM {}.{} WHERE {}').format(
                    sql.SQL(" || ' AND ' || ").join(key_filter_exprs),
                    sql.SQL(" || ',' || ").join(key_label_exprs),
                    sql.Identifier(raster_source['schema']),
                    sql.Identifier(raster_source['table']),
                    sql.SQL(' AND ').join(filter_parts),
                )
            )
            raster_row_map[raster_column] = list(cursor.fetchall())
        return raster_row_map

    def _find_first_column(self, column_info, udt_names):
        for column in column_info:
            if column['udt_name'] in udt_names:
                return column['name']
        return None

    def _get_query_import_groups(self, cursor, query_text, column_info):
        column_names = {column['name'] for column in column_info}
        if 'metadata' not in column_names:
            return []

        cursor.execute(
            sql.SQL(
                """
                SELECT DISTINCT
                    q.metadata::jsonb->'input_params'->>'query_from' AS query_from,
                    q.metadata::jsonb->'input_params'->>'import_type' AS import_type,
                    q.metadata::jsonb->'input_params'->>'search_re' AS search_re
                FROM ({}) AS q
                WHERE q.metadata::jsonb ? 'input_params'
                ORDER BY 1, 2, 3
                """
            ).format(sql.SQL(query_text))
        )
        return [
            normalize_import_parameter_row(row[0], row[1], row[2])
            for row in cursor.fetchall()
        ]

    def _get_geometry_types(self, cursor, query_text, geometry_column):
        cursor.execute(
            sql.SQL(
                """
                SELECT DISTINCT ST_GeometryType(q.{geometry_column})
                FROM ({query_text}) AS q
                WHERE q.{geometry_column} IS NOT NULL
                """
            ).format(
                geometry_column=sql.Identifier(geometry_column),
                query_text=sql.SQL(query_text),
            )
        )
        return [row[0] for row in cursor.fetchall() if row and row[0]]

    def _preview_start_value(self):
        return self.results_controller.preview_range()[0]

    def _preview_end_value(self):
        return self.results_controller.preview_range()[1]

    def _validated_preview_range(self):
        return self.results_controller.preview_range()

    def _update_results_preview(self):
        self._run_query_preview(add_to_history=False)

    def _set_results_label(self, preview_count, total_count):
        _ = preview_count
        self.results_controller.set_label(total_count)

    def _populate_preview(self, column_info, rows, total_count):
        self.results_controller.populate_preview(column_info, rows, total_count)

    def _create_uri(self):
        uri = QgsDataSourceUri()
        kwargs = self._connection_kwargs()
        service = kwargs.get('service', '')
        user = kwargs.get('user', '')
        password = kwargs.get('password', '')
        if service:
            uri.setConnection(service, kwargs['dbname'], user, password)
        else:
            uri.setConnection(
                kwargs.get('host', ''),
                kwargs.get('port', ''),
                kwargs['dbname'],
                user,
                password,
            )
        schema_name = self.connection_values.get('schema', '').strip()
        if schema_name and hasattr(uri, 'setParam'):
            uri.setParam('options', '-c search_path={},public'.format(schema_name))
        return uri

    def _build_postgres_raster_uri(self, raster_source, raster_column, row_filter):
        base_uri = self._create_uri()
        connection_info = base_uri.connectionInfo(False)
        where_parts = []
        if raster_source['where']:
            where_parts.append('({})'.format(raster_source['where']))
        where_parts.append(row_filter)
        where_clause = ' AND '.join(where_parts).replace("'", "''")
        return "PG: {} schema='{}' table='{}' column='{}' mode=1 where='{}'".format(
            connection_info,
            raster_source['schema'].replace("'", "''"),
            raster_source['table'].replace("'", "''"),
            raster_column.replace("'", "''"),
            where_clause,
        )

    def _create_vector_layer(self, query_text, geometry_column, layer_name):
        uri = self._create_uri()
        uri.setDataSource('', '({})'.format(query_text), geometry_column, '', self.KEY_COLUMN)
        layer = QgsVectorLayer(uri.uri(False), layer_name, 'postgres')
        return layer if layer.isValid() else None

    def _create_geometry_layers(self, query_text, geometry_column, layer_name, geometry_types):
        if not geometry_types:
            layer = self._create_vector_layer(query_text, geometry_column, layer_name)
            return [layer] if layer is not None else []

        if len(geometry_types) == 1:
            layer = self._create_vector_layer(query_text, geometry_column, layer_name)
            return [layer] if layer is not None else []

        layers = []
        for geometry_type in geometry_types:
            suffix = geometry_type.replace('ST_', '').lower()
            filtered_query = (
                "SELECT * FROM ({}) AS q WHERE ST_GeometryType(q.{}) = '{}'".format(
                    query_text,
                    self._quote_identifier(geometry_column),
                    geometry_type.replace("'", "''"),
                )
            )
            layer = self._create_vector_layer(
                filtered_query,
                geometry_column,
                '{} {}'.format(layer_name, suffix),
            )
            if layer is not None:
                layers.append(layer)
        return layers

    def _quote_identifier(self, value):
        return '"{}"'.format(value.replace('"', '""'))

    def _build_import_group_query(self, query_text, import_group):
        if not import_group:
            return query_text

        query_from, import_type, search_re = import_group
        return (
            "SELECT * FROM ({}) AS q "
            "WHERE coalesce(q.metadata::jsonb->'input_params'->>'query_from', '') = '{}' "
            "AND coalesce(q.metadata::jsonb->'input_params'->>'import_type', '') = '{}' "
            "AND coalesce(q.metadata::jsonb->'input_params'->>'search_re', '') = '{}'"
        ).format(
            query_text,
            query_from.replace("'", "''"),
            import_type.replace("'", "''"),
            search_re.replace("'", "''"),
        )

    def _create_raster_layer(self, raster_source, raster_column, row_filter, layer_name):
        layer = QgsRasterLayer(self._build_postgres_raster_uri(raster_source, raster_column, row_filter), layer_name, 'gdal')
        if not layer.isValid():
            error_summary = layer.error().summary() if layer.error() else 'unknown raster provider error'
            self._show_error('Raster layer could not be created from "{}" filter {}: {}'.format(raster_column, row_filter, error_summary))
            return None
        return layer

    def _ensure_query_group(self, query_name):
        root = QgsProject.instance().layerTreeRoot()
        group = root.addGroup(query_name)
        return group if isinstance(group, QgsLayerTreeGroup) else root

    def _add_layer_to_group(self, group, layer, insert_at_top=False):
        project = QgsProject.instance()
        project.addMapLayer(layer, False)
        if insert_at_top:
            group.insertLayer(0, layer)
        else:
            group.addLayer(layer)
