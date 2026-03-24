# -*- coding: utf-8 -*-
"""Group query tab built on the shared query workbench."""

import os
from datetime import datetime

from qgis.core import QgsLayerTreeGroup

from .query_tab import QueryTab, psycopg2, sql


class GroupTab(QueryTab):
    HISTORY_KEY = 'Landlensdb/group_history'
    STAR_KEY = 'Landlensdb/group_starred'
    NAME_KEY = 'Landlensdb/group_names'

    def __init__(self, iface, parent=None):
        super(GroupTab, self).__init__(iface, parent)
        self._last_group_state = None
        self.query_button.setText('Group')

    def _sql_placeholder_text(self):
        return (
            'SELECT  landuse,  array_agg(image_url) AS image_url  '
            'FROM "<schema>"."<table>"  GROUP BY landuse;'
        )

    def _builder_help_text(self):
        return (
            "Use the builder to write grouping SQL. The result must include an "
            "\"image_url\" column containing an array of image URLs. Click Group "
            "to preview the grouped rows, then Add to build nested QGIS groups.\n\n"
            "Example:\n{}".format(self._sql_placeholder_text())
        )

    def run_query(self):
        self._run_group_preview(add_to_history=True)

    def _run_group_preview(self, add_to_history):
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
        live_query = self._build_live_query(sql_text)

        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    column_info = self._get_column_info(cursor, live_query)
                    column_names = [column['name'] for column in column_info]
                    if 'image_url' not in column_names:
                        self._show_error('Group SQL must return an "image_url" column.')
                        return
                    row_count = self._get_row_count(cursor, live_query)
                    preview_rows = self._get_preview_rows(
                        cursor,
                        live_query,
                        column_info,
                        start_row,
                        end_row,
                    )
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Group query failed: {}'.format(exc))
            return

        self._populate_preview(column_info, preview_rows, row_count)
        if add_to_history:
            self._add_history_item(sql_text)
        self._last_group_state = {
            'query_name': 'Group {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            'live_query': live_query,
            'column_info': column_info,
            'column_names': column_names,
            'row_count': row_count,
        }
        self.add_button.setEnabled(True)
        self.bottom_tabs.setCurrentWidget(self.results_tab)
        self._show_info('Previewed {} group row(s). Click Add to load grouped layers into the map.'.format(row_count))

    def add_last_query_to_map(self):
        if not self._last_group_state:
            self._show_error('Run Group first to preview grouped rows before adding to the map.')
            return

        query_tab = self._linked_query_tab()
        if query_tab is None or not query_tab._last_query_state:
            self._show_error('Run a Query preview first so grouped rows can resolve to map layers.')
            return

        if 'image_url' not in (query_tab._last_query_state.get('column_names') or []):
            self._show_error('The active Query result must include an "image_url" column for grouping.')
            return

        group_rows = self._fetch_group_rows()
        if not group_rows:
            self._show_error('The grouped query returned no rows to add.')
            return

        root_group = self._ensure_query_group(self._last_group_state['query_name'])
        added_layers = []
        group_columns = [
            name for name in self._last_group_state['column_names'] if name != 'image_url'
        ]
        image_index = self._last_group_state['column_names'].index('image_url')

        for row in group_rows:
            parent_group = root_group
            for column_name in group_columns:
                value = row[self._last_group_state['column_names'].index(column_name)]
                parent_group = self._ensure_child_group(parent_group, '{}'.format(value))

            image_urls = self._normalize_image_urls(row[image_index])
            for image_url in image_urls:
                item_group = self._ensure_child_group(
                    parent_group,
                    os.path.basename(image_url) or image_url,
                )
                added_layers.extend(self._add_group_item_layers(query_tab, item_group, image_url))

        if added_layers:
            self._show_info(
                'Loaded {} grouped layer(s) under "{}".'.format(
                    len(added_layers),
                    self._last_group_state['query_name'],
                )
            )
        else:
            self._show_error('Nothing was added to the map from the current group preview.')

    def _linked_query_tab(self):
        window = self.window()
        return getattr(window, 'query_tab', None)

    def _fetch_group_rows(self):
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        rows = []
        try:
            with psycopg2.connect(**self._connection_kwargs()) as connection:
                with connection.cursor() as cursor:
                    if schema_name:
                        self._set_search_path(cursor, schema_name)
                    select_items = sql.SQL(', ').join(
                        sql.SQL('q.{}').format(sql.Identifier(column_name))
                        for column_name in self._last_group_state['column_names']
                    )
                    cursor.execute(
                        sql.SQL('SELECT {} FROM ({}) AS q').format(
                            select_items,
                            sql.SQL(self._last_group_state['live_query']),
                        )
                    )
                    rows = cursor.fetchall()
        except Exception as exc:  # pragma: no cover - depends on external DB
            self._show_error('Could not load grouped rows: {}'.format(exc))
        return rows

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

    def _add_group_item_layers(self, query_tab, group, image_url):
        added_layers = []
        filtered_query = (
            "SELECT * FROM ({}) AS q WHERE q.image_url = $lldb${}$lldb$".format(
                query_tab._last_query_state['live_query'],
                image_url.replace('$lldb$', ''),
            )
        )
        schema_name = self.connection_values.get('schema', 'public').strip() or 'public'
        vector_column = query_tab._last_query_state.get('vector_column')

        if vector_column:
            geometry_types = []
            try:
                with psycopg2.connect(**self._connection_kwargs()) as connection:
                    with connection.cursor() as cursor:
                        if schema_name:
                            self._set_search_path(cursor, schema_name)
                        geometry_types = self._get_geometry_types(cursor, filtered_query, vector_column)
            except Exception as exc:  # pragma: no cover - depends on external DB
                self._show_error('Could not inspect grouped geometry types: {}'.format(exc))

            for vector_layer in self._create_geometry_layers(
                filtered_query,
                vector_column,
                'geometry',
                geometry_types,
            ):
                added_layers.append(vector_layer.name())
                self._add_layer_to_group(group, vector_layer)

        raster_source = query_tab._last_query_state.get('raster_source')
        raster_columns = query_tab._last_query_state.get('raster_columns') or []
        if raster_source and raster_columns:
            row_filter = "\"image_url\" = $lldb${}$lldb$".format(image_url.replace('$lldb$', ''))
            for raster_column in raster_columns:
                raster_layer = self._create_raster_layer(
                    raster_source,
                    raster_column,
                    row_filter,
                    'thumbnail',
                )
                if raster_layer is not None:
                    added_layers.append(raster_layer.name())
                    self._add_layer_to_group(group, raster_layer, insert_at_top=True)

        return added_layers
