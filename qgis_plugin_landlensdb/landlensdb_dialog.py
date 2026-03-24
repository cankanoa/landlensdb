# -*- coding: utf-8 -*-
"""
/***************************************************************************
 LandlensdbDialog
                                 A QGIS plugin
 Graphical interface to add Postgis layers to QGIS
 ***************************************************************************/
"""

from qgis.PyQt import QtWidgets
from qgis.PyQt.QtGui import QIcon

import os

from .tabs.group_tab import GroupTab
from .tabs.import_tab import ImportTab
from .tabs.query_tab import QueryTab
from .tabs.setup_tab import SetupTab


class LandlensdbDialog(QtWidgets.QDialog):
    def __init__(self, iface, parent=None):
        super(LandlensdbDialog, self).__init__(parent)
        self.iface = iface
        self.setWindowTitle('Landlensdb')
        self.setWindowIcon(
            QIcon(os.path.join(os.path.dirname(__file__), 'landlensdb.png'))
        )
        self.resize(1120, 780)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        self.tab_widget = QtWidgets.QTabWidget(self)
        layout.addWidget(self.tab_widget)

        self.setup_tab = SetupTab(iface, self)
        self.import_tab = ImportTab(iface, self)
        self.query_tab = QueryTab(iface, self)
        self.group_tab = GroupTab(iface, self)

        self.tab_widget.addTab(self.setup_tab, 'Setup')
        self.tab_widget.addTab(self.import_tab, 'Import')
        self.tab_widget.addTab(self.query_tab, 'Query')
        self.tab_widget.addTab(self.group_tab, 'Group')

        self.setup_tab.connectionSaved.connect(self.query_tab.reload_connection_settings)
        self.query_tab.connectionSaved.connect(self.setup_tab.set_connection_values)
        self.setup_tab.connectionSaved.connect(self.import_tab.reload_connection_settings)
        self.setup_tab.connectionSaved.connect(self.group_tab.reload_connection_settings)
        self.group_tab.connectionSaved.connect(self.setup_tab.set_connection_values)
        self.query_tab.connectionSaved.connect(self.group_tab.reload_connection_settings)
        self.group_tab.connectionSaved.connect(self.query_tab.reload_connection_settings)
        self.query_tab.connectionSaved.connect(self.import_tab.reload_connection_settings)
        self.group_tab.connectionSaved.connect(self.import_tab.reload_connection_settings)
