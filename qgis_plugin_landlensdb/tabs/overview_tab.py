# -*- coding: utf-8 -*-

from qgis.PyQt import QtWidgets


class OverviewTab(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super(OverviewTab, self).__init__(parent)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        title = QtWidgets.QLabel('Landlensdb QGIS Plugin')
        font = title.font()
        font.setBold(True)
        font.setPointSize(font.pointSize() + 1)
        title.setFont(font)
        layout.addWidget(title)

        content = QtWidgets.QTextBrowser(self)
        content.setOpenExternalLinks(True)
        content.setReadOnly(True)
        content.setHtml(
            """
            <p>Landlensdb is a geospatial image workflow for storing image records, thumbnails, footprints, and metadata in PostgreSQL/PostGIS and then working with them directly in QGIS. It is useful for organizing local and remote imagery, keeping metadata queryable, and turning SQL results into QGIS layers without manual export steps.</p>

            <h3>Setup</h3>
            <p>Use <b>Setup</b> to create PostGIS extension SQL and save the PostgreSQL connection used by Import and Query. The plugin depends on QPIP for Python package installation, which should have already installed the required packages.</p>

            <h3>Import</h3>
            <p>Use <b>Import</b> to save input parameters: <code>import_type</code>, <code>query_from</code>, <code>search_glob</code>, and optional <code>additional_files_and_metadata_glob</code>.</p>
            <p>Supported importers are <code>GeoTaggedImage</code>, <code>GeoTransformImage</code>, and <code>WorldView3Image</code>. <code>search_glob</code> is recursive, so patterns like <code>**/*.tif</code> work.</p>
            <p>Actions let you update rows from disk, skip existing rows, drop missing rows, drop all rows for saved input parameters, sync, and resolve additional metadata files.</p>

            <h3>Additional Metadata</h3>
            <p><code>additional_files_and_metadata_glob</code> can match extra files relative to each image path or by absolute path. Every matched file path is saved into the row metadata. If a matched file path ends with <code>.yml</code> or <code>.yaml</code>, the file is parsed and its YAML structure is merged into the row metadata under additional files and metadata, so those values can be queried later.</p>

            <h3>Query</h3>
            <p>Use <b>Query</b> to preview SQL results and build spatial and metadata expressions. The query text is the source for previewing, viewing, grouping, and metadata copy actions.</p>
            <p>The SQL must return <code>image_url</code>. If <code>image_url</code> is a string, each row is added directly. If it is a list of strings, the other columns define the QGIS group hierarchy from left to right.</p>

            <h3>View And Copy</h3>
            <p>Query results can be viewed in QGIS as geometry and, where available, thumbnails. The <b>View</b> tab can display multiple selected Landlensdb images in a zoomable grid, switch between preview and path sources, navigate through the selected layer by an organized metadata field, and optionally rotate images to north-up. Staged metadata fields can also be copied from the current query into CSV output for the matching rows. If the query is grouped, each group is separated by a heading line before its CSV block, using a format like <code>Year=2024.Month=05</code> based on the grouping columns and values.</p>
            """
        )
        layout.addWidget(content, 1)
