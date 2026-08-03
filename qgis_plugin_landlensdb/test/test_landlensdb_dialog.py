# coding=utf-8
"""Dialog test."""

__author__ = 'cankanoa@gmail.com'
__date__ = '2026-03-13'
__copyright__ = 'Copyright 2026, Kanoa Lindiwe LLC'

import unittest
from unittest import mock

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
        self.assertEqual(self.dialog.tab_widget.count(), 5)
        self.assertEqual(self.dialog.tab_widget.tabText(0), 'Overview')
        self.assertEqual(self.dialog.tab_widget.tabText(1), 'Setup')
        self.assertEqual(self.dialog.tab_widget.tabText(2), 'Import')
        self.assertEqual(self.dialog.tab_widget.tabText(3), 'Query')
        self.assertEqual(self.dialog.tab_widget.tabText(4), 'View')

        query_tab = self.dialog.query_tab
        self.assertTrue(query_tab.connection_button.text().startswith('Connection'))
        self.assertEqual(query_tab.commands_toggle_button.text(), 'Commands')
        self.assertEqual(query_tab.history_menu_button.text(), 'History')
        self.assertEqual(query_tab.star_menu_button.text(), 'Star')
        self.assertEqual(query_tab.query_button.text(), 'Query')
        self.assertEqual(query_tab.add_button.text(), 'Add')
        self.assertEqual(query_tab.close_button.text(), 'Close')

    def test_import_row_action_keeps_clicked_row_index(self):
        """QAction's checked signal must not replace the captured table row."""
        import_tab = self.dialog.import_tab
        import_tab.load_records([
            {
                'metadata': {
                    'input_params': {
                        'query_from': '/images/first',
                        'import_type': 'GeoTaggedImage',
                        'search_glob': '**/*.JPG',
                        'additional_files_and_metadata_glob': '',
                    }
                }
            },
            {
                'metadata': {
                    'input_params': {
                        'query_from': '/images/second',
                        'import_type': 'GeoTaggedImage',
                        'search_glob': '**/*.JPG',
                        'additional_files_and_metadata_glob': '',
                    }
                }
            },
        ])
        import_tab.run_row_drop_all = mock.Mock()

        actions_widget = import_tab.import_table.cellWidget(
            1,
            import_tab.ACTIONS_COLUMN,
        )
        actions_button = actions_widget.layout().itemAt(0).widget()
        drop_all_action = next(
            action for action in actions_button.menu().actions()
            if action.text() == 'Drop All'
        )

        drop_all_action.trigger()

        import_tab.run_row_drop_all.assert_called_once_with(1)


if __name__ == "__main__":
    suite = unittest.makeSuite(LandlensdbDialogTest)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
