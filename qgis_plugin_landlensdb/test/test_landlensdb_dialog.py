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
    """Test dialog widgets exist."""

    def setUp(self):
        """Runs before each test."""
        self.dialog = LandlensdbDialog(None)

    def tearDown(self):
        """Runs after each test."""
        self.dialog = None

    def test_dialog_has_query_controls(self):
        """The query workflow widgets should be available."""
        self.assertTrue(self.dialog.connection_button.text().startswith('Connection'))
        self.assertEqual(self.dialog.commands_toggle_button.text(), 'Commands')
        self.assertEqual(self.dialog.history_menu_button.text(), 'History')
        self.assertEqual(self.dialog.star_menu_button.text(), 'Star')
        self.assertEqual(self.dialog.results_label.text(), 'Results (0/0)')
        self.assertEqual(self.dialog.query_button.text(), 'Query')
        self.assertEqual(self.dialog.add_button.text(), 'Add')
        self.assertEqual(self.dialog.close_button.text(), 'Close')


if __name__ == "__main__":
    suite = unittest.makeSuite(LandlensdbDialogTest)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
