# -*- coding: utf-8 -*-

from qgis.PyQt import QtWidgets


class OverviewTab(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super(OverviewTab, self).__init__(parent)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        title = QtWidgets.QLabel("Landlensdb QGIS Plugin")
        font = title.font()
        font.setBold(True)
        font.setPointSize(font.pointSize() + 1)
        title.setFont(font)
        layout.addWidget(title)

        content = QtWidgets.QTextBrowser(self)
        content.setOpenExternalLinks(True)
        content.setReadOnly(True)
        content.setHtml("""
            <p>Landlensdb is a geospatial image workflow for storing image records, thumbnails, footprints, and metadata in PostgreSQL/PostGIS and then working with them directly in QGIS. It is useful for organizing local and remote imagery, keeping metadata queryable, and turning SQL results into QGIS layers without manual export steps.</p>

            <h3>Setup</h3>
            <p>Use <b>Setup</b> to create PostGIS extension SQL and save the PostgreSQL connection used by Import and Query. The plugin depends on QPIP for Python package installation, which should have already installed the required packages.</p>

            <h3>Import</h3>
            <p>Use <b>Import Parameters</b> to configure and save an import in a colorized YAML editor. The saved document is restored when the editor reopens. Presets cover the defaults, EXIF-geotagged photos, georeferenced rasters, and WorldView-3 TIL + IMD products. Imports are grouped by the SHA-256 digest of their normalized parameters. Each row stores that digest and the canonical YAML used to create it.</p>
            <p>The YAML controls one full-path wcmatch file glob, field sources, geometry extraction, user-defined metadata, thumbnails, and fingerprints. Threads, batch size, and error handling remain QGIS controls and are not stored in the YAML. Use <b>Update New</b> when existing image paths should be skipped.</p>
            <p>Actions let you update a selected import group, remove rows whose files no longer match, or remove the complete group.</p>

            <h3>Additional Metadata</h3>
            <p>An optional sidecar glob must resolve exactly one supported file per image when configured. JSON, GeoJSON, YAML, and WorldView IMD files are converted to a JSON-like mapping before metadata expressions are evaluated. Metadata mappings explicitly select values from EXIF, raster, sidecar, file, geometry, constants, or automatic EXIF time parsing.</p>

            <h3>Query</h3>
            <p>Use <b>Query</b> to preview SQL results and build spatial and metadata expressions. The query text is the source for previewing, viewing, grouping, and metadata copy actions.</p>
            <p>The SQL must return <code>image_url</code>. If <code>image_url</code> is a string, each row is added directly. If it is a list of strings, the other columns define the QGIS group hierarchy from left to right.</p>

            <h3>View And Copy</h3>
            <p>Query results can be viewed in QGIS as geometry and, where available, thumbnails. The <b>View</b> tab can display multiple selected Landlensdb images in a zoomable grid, switch between preview and path sources, navigate through the selected layer by an organized metadata field, and optionally rotate images to north-up. Staged metadata fields can also be copied from the current query into CSV output for the matching rows. If the query is grouped, each group is separated by a heading line before its CSV block, using a format like <code>Year=2024.Month=05</code> based on the grouping columns and values.</p>
            """)
        layout.addWidget(content, 1)
