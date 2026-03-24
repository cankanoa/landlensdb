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
from ..landlensdb.handlers.local import (
    GeoTaggedImage,
    GeoTransformImage,
    SearchLocalToGeoImageFrame,
    WorldView3Image,
)
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
    THUMBNAIL_IMPORT_TYPE_NAMES = (
        'GeoTransformImage',
        'WorldView3Image',
    )
    SIMPLE_SELECT_RE = re.compile(
        r'^\s*SELECT\s+\*\s+FROM\s+(?:"(?P<schema_q>[^"]+)"|"?(?P<schema_u>[\w]+)"?)\.(?:"(?P<table_q>[^"]+)"|"?(?P<table_u>[\w]+)"?)'
        r'(?:\s+(?:AS\s+)?(?:"?[\w]+"?))?'
        r'(?:\s+WHERE\s+(?P<where>.+?))?'
        r'\s*$',
        re.IGNORECASE | re.DOTALL,
    )
    SOURCE_TABLE_RE = re.compile(
        r'\bFROM\s+(?:"(?P<schema_q>[^"]+)"|"?(?P<schema_u>[\w]+)"?)\.(?:"(?P<table_q>[^"]+)"|"?(?P<table_u>[\w]+)"?)'
        r'(?:\s+(?:AS\s+)?(?:"?[\w]+"?))?'
        r'(?P<tail>.*)$',
        re.IGNORECASE | re.DOTALL,
    )
    STATIC_ROW_ONE = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'GROUP BY', 'ORDER BY', 'LIMIT']
    STATIC_ROW_TWO = ['*', 'DISTINCT', 'COUNT(*)', 'SUM()', 'AVG()', 'MIN()', 'MAX()', 'AND', 'OR', 'NOT', 'IN ()', 'LIKE', 'IS NULL', 'IS NOT NULL']
    STATIC_ROW_THREE = ['ST_Intersects()', 'ST_Within()', 'ST_Contains()', 'ST_DWithin()', 'ST_Touches()', 'ST_Crosses()', '::geometry', '::geography']
    QUERY_EXAMPLE = (
        "image_url is the unique identifier per GeoImage. This sql statement must return the following:\n"
        'No grouping (image_url column must be one string per row):  SELECT  *  FROM  "<shema>"."<table>"\n'
        "Grouping (image_url must be a list of strings):\n"
        "SELECT   metadata::jsonb->'captured_at'->>'year' ,  array_agg(image_url) AS image_url  FROM  "
        '"<shema>"."<table>"   GROUP BY metadata::jsonb->\'captured_at\'->>\'year\' '
    )

    def __init__(self, iface, parent=None):
        super(QueryTab, self).__init__(parent)
        self.iface = iface
        self.setupUi(self)

        self.connection_values = {}
        self._metadata_loaded = False
        self._last_query_state = None
        self._thumbnail_support_cache = {}

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
        self._setup_add_menu()

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

    def _setup_add_menu(self):
        add_menu = QtWidgets.QMenu(self.add_button)
        add_thumbnail_action = add_menu.addAction('Add Thumbnail')
        add_thumbnail_action.triggered.connect(
            lambda: self.add_last_query_to_map(add_thumbnail=True, add_geometry=False)
        )
        add_geometry_action = add_menu.addAction('Add Geometry')
        add_geometry_action.triggered.connect(
            lambda: self.add_last_query_to_map(add_thumbnail=False, add_geometry=True)
        )

        self.add_menu_button = QtWidgets.QToolButton(self)
        self.add_menu_button.setText('')
        self.add_menu_button.setArrowType(QtCore.Qt.DownArrow)
        self.add_menu_button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.add_menu_button.setMenu(add_menu)
        self.add_menu_button.setStyleSheet('QToolButton::menu-indicator { image: none; width: 0px; }')
        self.add_menu_button.setEnabled(False)

        if hasattr(self, 'buttonLayout'):
            index = self.buttonLayout.indexOf(self.add_button)
            if index >= 0:
                self.buttonLayout.insertWidget(index + 1, self.add_menu_button)

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
        self.commands_toggle_button.setStyleSheet('')
        self.history_menu_button.setStyleSheet('')
        self.star_menu_button.setStyleSheet('')

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
            "the returned rows. Add requires an image_url column. If image_url is a "
            "string, each row is added directly. If image_url is a list, each item is "
            "treated as a grouped GeoImage entry.\n\n"
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
        self._set_row_buttons('row_five_layout', [('Spatial Query', None), ('Metadata Query', None)])
        spatial_button = self.row_five_layout.itemAt(0).widget()
        if spatial_button is not None:
            try:
                spatial_button.clicked.disconnect()
            except TypeError:
                pass
            spatial_button.clicked.connect(self._open_spatial_query_dialog)
        metadata_button = self.row_five_layout.itemAt(1).widget()
        if metadata_button is not None:
            try:
                metadata_button.clicked.disconnect()
            except TypeError:
                pass
            metadata_button.clicked.connect(self._open_metadata_query_menu)

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
        query_source = self._parse_query_source(sql_text)
        source_column_info = []
        effective_raster_source = raster_source
        effective_raster_columns = []

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
                    effective_raster_columns = [
                        column['name'] for column in column_info if column['udt_name'] == 'raster'
                    ]
                    if query_source:
                        source_query = self._build_source_query_from_source(query_source)
                        source_column_info = self._get_column_info(cursor, source_query)
                        if effective_raster_source is None:
                            effective_raster_source = query_source
                        if not effective_raster_columns:
                            effective_raster_columns = [
                                column['name']
                                for column in source_column_info
                                if column['udt_name'] == 'raster'
                            ]
                    raster_key_columns = self._get_raster_key_columns(cursor, effective_raster_source)
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Query failed: {}'.format(exc))
            return

        vector_column = self._find_first_column(column_info, {'geometry', 'geography'})
        source_vector_column = self._find_first_column(source_column_info, {'geometry', 'geography'})
        self._populate_preview(column_info, preview_rows, row_count)
        if add_to_history:
            self._add_history_item(sql_text)
        self._last_query_state = {
            'query_name': query_name,
            'live_query': live_query,
            'query_source': query_source,
            'column_info': column_info,
            'column_names': [column['name'] for column in column_info],
            'raster_source': effective_raster_source,
            'vector_column': vector_column,
            'source_vector_column': source_vector_column,
            'raster_columns': effective_raster_columns,
            'raster_key_columns': raster_key_columns,
            'row_count': row_count,
        }
        self.add_button.setEnabled(True)
        self.add_menu_button.setEnabled(True)
        self.bottom_tabs.setCurrentWidget(self.results_tab)
        self._show_info('Previewed {} row(s). Click Add to load layers into the map.'.format(row_count))

    def add_last_query_to_map(self, add_thumbnail=True, add_geometry=True):
        if not self._last_query_state:
            self._show_error('Run Query first to preview results before adding to the map.')
            return
        if 'image_url' not in self._last_query_state.get('column_names', []):
            self._show_error('Add requires an "image_url" column in the query results.')
            return
        if not add_thumbnail and not add_geometry:
            self._show_error('Select at least one layer type to add.')
            return

        query_name = self._last_query_state['query_name']
        root_group = self._ensure_query_group(query_name)
        query_rows = self._fetch_query_rows()
        if not query_rows:
            self._show_error('The query returned no rows to add.')
            return

        added_layers = []
        grouped_urls = self._build_grouped_image_urls(query_rows)
        for group_path, image_urls in grouped_urls.items():
            if not image_urls:
                continue
            parent_group = root_group
            for part in group_path:
                parent_group = self._ensure_child_group(parent_group, part)
            added_layers.extend(
                self._add_group_layers(
                    parent_group,
                    image_urls,
                    add_thumbnail=add_thumbnail,
                    add_geometry=add_geometry,
                )
            )

        if added_layers:
            self._show_info(
                'Loaded {} layer(s) for "{}".'.format(
                    len(added_layers),
                    query_name,
                )
            )
        else:
            self._show_error('Nothing was added to the map from the current preview.')

    def _fetch_query_rows(self):
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        rows = []
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    select_items = sql.SQL(', ').join(
                        sql.SQL('q.{}').format(sql.Identifier(column_name))
                        for column_name in self._last_query_state['column_names']
                    )
                    cursor.execute(
                        sql.SQL('SELECT {} FROM ({}) AS q').format(
                            select_items,
                            sql.SQL(self._last_query_state['live_query']),
                        )
                    )
                    rows = cursor.fetchall()
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Could not load query rows: {}'.format(exc))
        return rows

    def _build_grouped_image_urls(self, query_rows):
        grouped_urls = {}
        column_names = self._last_query_state['column_names']
        image_url_index = column_names.index('image_url')

        for row in query_rows:
            image_url_value = row[image_url_index]
            image_urls = self._normalize_image_urls(image_url_value)
            if not image_urls:
                continue

            is_grouped_row = isinstance(image_url_value, (list, tuple)) or (
                isinstance(image_url_value, str)
                and image_url_value.startswith('{')
                and image_url_value.endswith('}')
            )
            if is_grouped_row:
                group_path = tuple(
                    '{}'.format(value)
                    for column_name, value in zip(column_names, row)
                    if column_name not in ('image_url', self.KEY_COLUMN)
                )
            else:
                group_path = tuple()

            grouped_urls.setdefault(group_path, [])
            for image_url in image_urls:
                if image_url not in grouped_urls[group_path]:
                    grouped_urls[group_path].append(image_url)

        return grouped_urls

    def _normalize_image_urls(self, value):
        if isinstance(value, (list, tuple)):
            return [item for item in value if item]
        if not value:
            return []
        if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
            stripped = value[1:-1].strip()
            if not stripped:
                return []
            return [item.strip().strip('"') for item in stripped.split(',') if item.strip()]
        return [str(value)]

    def _ensure_child_group(self, parent_group, label):
        for child in parent_group.children():
            if isinstance(child, QgsLayerTreeGroup) and child.name() == label:
                return child
        return parent_group.addGroup(label)

    def _add_group_layers(self, parent_group, image_urls, add_thumbnail=True, add_geometry=True):
        added_layers = []
        if add_thumbnail:
            thumbnail_group = self._ensure_child_group(parent_group, 'thumbnail')
            added_layers.extend(self._add_thumbnail_layers(thumbnail_group, image_urls))
        if add_geometry:
            geometry_group = self._ensure_child_group(parent_group, 'geometry')
            added_layers.extend(self._add_geometry_layers(geometry_group, image_urls))
        return added_layers

    def _add_thumbnail_layers(self, group, image_urls):
        added_layers = []
        raster_source = self._last_query_state.get('raster_source')
        raster_columns = self._last_query_state.get('raster_columns') or []
        if not raster_source or not raster_columns:
            return added_layers

        supported_image_urls = self._get_thumbnail_image_urls(image_urls)
        for image_url in supported_image_urls:
            row_filter = self._build_thumbnail_row_filter(image_url)
            for raster_column in raster_columns:
                raster_layer = self._create_raster_layer(
                    raster_source,
                    raster_column,
                    row_filter,
                    os.path.basename(image_url) or image_url,
                )
                if raster_layer is not None:
                    added_layers.append(raster_layer.name())
                    self._add_layer_to_group(group, raster_layer, insert_at_top=True)
        return added_layers

    def _build_thumbnail_row_filter(self, image_url):
        safe_image_url = image_url.replace('$lldb$', '')
        return "\"image_url\" = $lldb${}$lldb$".format(safe_image_url)

    def _add_geometry_layers(self, group, image_urls):
        added_layers = []
        vector_column = self._last_query_state.get('source_vector_column') or self._last_query_state.get('vector_column')
        if not vector_column or not image_urls:
            return added_layers

        filtered_query = self._build_image_url_query(image_urls)
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        geometry_types = []
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    geometry_types = self._get_geometry_types(cursor, filtered_query, vector_column)
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Could not inspect geometry types: {}'.format(exc))
            return added_layers

        geometry_families = self._geometry_families(geometry_types)
        if len(geometry_families) <= 1:
            layer = self._create_vector_layer(filtered_query, vector_column, 'geometry')
            if layer is not None:
                added_layers.append(layer.name())
                self._add_layer_to_group(group, layer)
            return added_layers

        for family in geometry_families:
            family_query = self._build_geometry_family_query(filtered_query, vector_column, family)
            layer = self._create_vector_layer(family_query, vector_column, family)
            if layer is not None:
                added_layers.append(layer.name())
                self._add_layer_to_group(group, layer)
        return added_layers

    def _build_image_url_query(self, image_urls):
        filters = [
            "q.image_url = $lldb${}$lldb$".format(image_url.replace('$lldb$', ''))
            for image_url in image_urls
        ]
        return "SELECT * FROM ({}) AS q WHERE {}".format(
            self._build_source_query(),
            " OR ".join(filters),
        )

    def _geometry_families(self, geometry_types):
        families = []
        for geometry_type in geometry_types:
            normalized = geometry_type.upper()
            if 'POINT' in normalized:
                family = 'points'
            elif 'POLYGON' in normalized:
                family = 'polygons'
            elif 'LINE' in normalized:
                family = 'lines'
            else:
                family = normalized.replace('ST_', '').lower()
            if family not in families:
                families.append(family)
        return families

    def _build_geometry_family_query(self, filtered_query, geometry_column, family):
        if family == 'points':
            match = "LIKE 'ST_%Point'"
        elif family == 'polygons':
            match = "LIKE 'ST_%Polygon'"
        elif family == 'lines':
            match = "LIKE 'ST_%LineString'"
        else:
            match = "= '{}'".format(family)
        return (
            "SELECT * FROM ({}) AS q WHERE ST_GeometryType(q.{}) {}".format(
                filtered_query,
                self._quote_identifier(geometry_column),
                match,
            )
        )

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

    def _parse_query_source(self, sql_text):
        match = self.SOURCE_TABLE_RE.search(sql_text.strip())
        if not match:
            return None

        tail = match.group('tail') or ''
        where_clause = ''
        where_match = re.search(
            r'\bWHERE\b(?P<where>.*?)(?=\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b|$)',
            tail,
            re.IGNORECASE | re.DOTALL,
        )
        if where_match:
            where_clause = where_match.group('where').strip()

        return {
            'schema': match.group('schema_q') or match.group('schema_u') or 'public',
            'table': match.group('table_q') or match.group('table_u'),
            'where': where_clause,
        }

    def _build_source_query_from_source(self, query_source):
        base_query = 'SELECT * FROM "{}"."{}"'.format(
            query_source['schema'].replace('"', '""'),
            query_source['table'].replace('"', '""'),
        )
        if query_source.get('where'):
            base_query = '{} WHERE {}'.format(base_query, query_source['where'])
        return self._build_live_query(base_query)

    def _build_source_query(self):
        query_source = self._last_query_state.get('query_source')
        if not query_source:
            return self._last_query_state['live_query']
        return self._build_source_query_from_source(query_source)

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

    def _open_metadata_query_menu(self):
        button = self.row_five_layout.itemAt(1).widget()
        if button is None:
            return

        menu = QtWidgets.QMenu(button)
        metadata_classes = [
            ('GeoTaggedImage', GeoTaggedImage),
            ('GeoTransformImage', GeoTransformImage),
            ('WorldView3Image', WorldView3Image),
        ]
        metadata_schemas = {
            label: importer_cls._get_metadata()
            for label, importer_cls in metadata_classes
        }
        base_schema = self._metadata_schema_intersection(list(metadata_schemas.values()))
        if base_schema:
            submenu = menu.addMenu('Base')
            self._populate_metadata_submenu(submenu, base_schema, [])

        for label, _importer_cls in metadata_classes:
            unique_schema = self._metadata_schema_difference(
                metadata_schemas[label],
                base_schema,
            )
            if not unique_schema:
                continue
            submenu = menu.addMenu(label)
            self._populate_metadata_submenu(
                submenu,
                unique_schema,
                [],
            )

        menu.exec_(button.mapToGlobal(button.rect().bottomLeft()))

    def _metadata_schema_intersection(self, schemas):
        if not schemas:
            return {}

        first_schema = schemas[0]
        shared = {}
        for key, value in first_schema.items():
            if not all(isinstance(schema, dict) and key in schema for schema in schemas[1:]):
                continue
            other_values = [schema[key] for schema in schemas[1:]]
            if isinstance(value, dict) and all(isinstance(other, dict) for other in other_values):
                nested_shared = self._metadata_schema_intersection(
                    [value] + other_values
                )
                if nested_shared:
                    shared[key] = nested_shared
            else:
                shared[key] = value
        return shared

    def _metadata_schema_difference(self, schema, base_schema):
        if not isinstance(schema, dict):
            return schema

        difference = {}
        for key, value in schema.items():
            if key not in base_schema:
                difference[key] = value
                continue
            base_value = base_schema[key]
            if isinstance(value, dict) and isinstance(base_value, dict):
                nested_difference = self._metadata_schema_difference(value, base_value)
                if nested_difference:
                    difference[key] = nested_difference
        return difference

    def _populate_metadata_submenu(self, menu, metadata, path_parts):
        for key, value in metadata.items():
            current_path = path_parts + [key]
            if isinstance(value, dict):
                submenu = menu.addMenu(key)
                self._populate_metadata_submenu(submenu, value, current_path)
                continue
            action = menu.addAction(key)
            action.triggered.connect(
                lambda _checked=False, path=current_path: self._insert_sql(
                    self._metadata_sql_expression(path)
                )
            )

    def _metadata_sql_expression(self, path_parts):
        if not path_parts:
            return 'metadata'
        sql_parts = ["metadata::jsonb"]
        for key in path_parts[:-1]:
            sql_parts.append("->'{}'".format(key.replace("'", "''")))
        sql_parts.append("->>'{}'".format(path_parts[-1].replace("'", "''")))
        return ''.join(sql_parts)

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

    def _find_first_column(self, column_info, udt_names):
        for column in column_info:
            if column['udt_name'] in udt_names:
                return column['name']
        return None

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

    def _get_thumbnail_image_urls(self, image_urls):
        raster_source = self._last_query_state.get('raster_source')
        if not raster_source or not image_urls:
            return []

        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    if not self._table_has_column(cursor, raster_source, 'metadata'):
                        return list(image_urls)
                    cursor.execute(
                        sql.SQL(
                            """
                            SELECT image_url
                            FROM {}.{}
                            WHERE image_url = ANY(%s)
                              AND coalesce(metadata::jsonb->'input_params'->>'import_type', '') = ANY(%s)
                            """
                        ).format(
                            sql.Identifier(raster_source['schema']),
                            sql.Identifier(raster_source['table']),
                        ),
                        (list(image_urls), list(self.THUMBNAIL_IMPORT_TYPE_NAMES)),
                    )
                    return [row[0] for row in cursor.fetchall() if row and row[0]]
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Could not inspect thumbnail rows: {}'.format(exc))
            return []

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

    def _quote_identifier(self, value):
        return '"{}"'.format(value.replace('"', '""'))

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
