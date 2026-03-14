# -*- coding: utf-8 -*-

from qgis.PyQt import QtWidgets


class ImportTab(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super(ImportTab, self).__init__(parent)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)

        title = QtWidgets.QLabel('Import')
        font = title.font()
        font.setPointSize(font.pointSize() + 3)
        font.setBold(True)
        title.setFont(font)
        layout.addWidget(title)

        message = QtWidgets.QLabel(
            'Import workflow will live here next. The query and setup pages are wired first.'
        )
        message.setWordWrap(True)
        layout.addWidget(message)
        layout.addStretch()
