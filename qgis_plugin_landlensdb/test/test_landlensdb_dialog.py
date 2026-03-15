# coding=utf-8
"""Dialog test."""

__author__ = 'cankanoa@gmail.com'
__date__ = '2026-03-13'
__copyright__ = 'Copyright 2026, Kanoa Lindiwe LLC'

import unittest

from landlensdb_dialog import LandlensdbDialog

from utilities import get_qgis_app
QGIS_APP = get_qgis_app()


class LandlensdbDialogTest(unittest.TestCase):
    """Test top-level tabs and query widgets exist."""

    def setUp(self):
        """Runs before each test."""
        self.dialog = LandlensdbDialog(None)

    def tearDown(self):
        """Runs after each test."""
        self.dialog = None

    def test_dialog_has_tabs_and_query_controls(self):
        """The tabbed dialog and query workflow widgets should be available."""
        self.assertEqual(self.dialog.tab_widget.count(), 3)
        self.assertEqual(self.dialog.tab_widget.tabText(0), 'Setup')
        self.assertEqual(self.dialog.tab_widget.tabText(1), 'Import')
        self.assertEqual(self.dialog.tab_widget.tabText(2), 'Query')

        query_tab = self.dialog.query_tab
        self.assertTrue(query_tab.connection_button.text().startswith('Connection'))
        self.assertEqual(query_tab.commands_toggle_button.text(), 'Commands')
        self.assertEqual(query_tab.history_menu_button.text(), 'History')
        self.assertEqual(query_tab.star_menu_button.text(), 'Star')
        self.assertEqual(query_tab.results_label.text(), 'Results (0/0)')
        self.assertEqual(query_tab.query_button.text(), 'Query')
        self.assertEqual(query_tab.add_button.text(), 'Add')
        self.assertEqual(query_tab.close_button.text(), 'Close')


if __name__ == "__main__":
    suite = unittest.makeSuite(LandlensdbDialogTest)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
