# coding=utf-8
"""Dialog test."""

__author__ = "cankanoa@gmail.com"
__date__ = "2026-03-13"
__copyright__ = "Copyright 2026, Kanoa Lindiwe LLC"

import unittest

from qgis.PyQt import QtCore, QtGui, QtWidgets

from ..landlensdb_dialog import LandlensdbDialog
from ..landlensdb.import_config import load_example_import_yaml, load_import_presets
from ..shared.import_settings import (
    IMPORT_PARAMETERS_KEY,
    load_import_parameters,
    save_import_parameters,
)
from ..shared.yaml_editor import ImportYamlDialog
from ..tabs.view_tab import ImageCanvas, ImageScrollArea
from .utilities import get_qgis_app

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
        self.assertEqual(self.dialog.tab_widget.tabText(0), "Overview")
        self.assertEqual(self.dialog.tab_widget.tabText(1), "Setup")
        self.assertEqual(self.dialog.tab_widget.tabText(2), "Import")
        self.assertEqual(self.dialog.tab_widget.tabText(3), "Query")
        self.assertEqual(self.dialog.tab_widget.tabText(4), "View")

        query_tab = self.dialog.query_tab
        self.assertFalse(hasattr(query_tab, "connection_button"))
        self.assertEqual(query_tab.commands_toggle_button.text(), "Commands")
        self.assertEqual(query_tab.history_menu_button.text(), "History")
        self.assertEqual(query_tab.star_menu_button.text(), "Star")
        self.assertEqual(query_tab.query_button.text(), "Query")
        self.assertEqual(query_tab.add_button.text(), "Add")
        self.assertEqual(query_tab.close_button.text(), "Close")
        query_tab._populate_copy_menu()
        self.assertNotIn(
            "Fetch Metadata Structure",
            [action.text() for action in query_tab.copy_menu.actions()],
        )
        self.assertFalse(hasattr(query_tab, "additional_metadata_refresh_button"))

        setup_tab = self.dialog.setup_tab
        self.assertTrue(setup_tab.server_conda_radio.isChecked())
        self.assertEqual(setup_tab.server_stack.currentIndex(), 3)
        requirements = setup_tab.server_requirements_label.text()
        self.assertIn("PostgreSQL 14+", requirements)
        self.assertIn("postgis_raster", requirements)
        self.assertIn("GDAL raster drivers", requirements)

    def test_view_outer_area_ignores_wheel_but_keeps_scrollbar(self):
        class WheelEvent:
            def __init__(self):
                self.accepted = False

            def accept(self):
                self.accepted = True

        scroll_area = ImageScrollArea()
        scrollbar = scroll_area.verticalScrollBar()
        scrollbar.setRange(0, 100)
        scrollbar.setValue(40)

        event = WheelEvent()
        scroll_area.wheelEvent(event)

        self.assertTrue(event.accepted)
        self.assertEqual(scrollbar.value(), 40)
        scrollbar.setValue(60)
        self.assertEqual(scrollbar.value(), 60)

    def test_image_canvas_consumes_wheel_and_zooms(self):
        class WheelEvent:
            def __init__(self):
                self.accepted = False

            def angleDelta(self):
                return QtCore.QPoint(0, 120)

            def accept(self):
                self.accepted = True

        canvas = ImageCanvas()
        canvas.resize(300, 220)
        pixmap = QtGui.QPixmap(1200, 800)
        pixmap.fill(QtGui.QColor("black"))
        canvas.set_pixmap(pixmap)
        initial_scale = canvas._current_scale

        event = WheelEvent()
        canvas.wheelEvent(event)

        self.assertTrue(event.accepted)
        self.assertGreater(canvas._current_scale, initial_scale)

    def test_import_rows_are_grouped_by_input_sha(self):
        """The import tab displays one row for each canonical input SHA."""
        import_tab = self.dialog.import_tab
        first_yaml = "source:\n  file_glob: /photos/first/**/*.jpg\n"
        second_yaml = "source:\n  file_glob: /photos/second/**/*.tif\n"
        import_tab.load_records(
            [
                {
                    "input_sha": "a" * 64,
                    "row_count": 2,
                    "import_params": first_yaml,
                },
                {
                    "input_sha": "b" * 64,
                    "row_count": 3,
                    "import_params": second_yaml,
                },
            ]
        )

        self.assertEqual(import_tab.import_table.rowCount(), 2)
        self.assertEqual(
            import_tab.import_table.item(0, import_tab.COUNT_COLUMN).text(), "2"
        )
        self.assertEqual(
            import_tab.import_table.item(1, import_tab.COUNT_COLUMN).text(), "3"
        )
        self.assertEqual(
            import_tab.import_table.item(0, import_tab.COUNT_COLUMN).data(
                QtCore.Qt.UserRole
            ),
            "a" * 64,
        )
        self.assertEqual(
            import_tab.import_table.item(0, import_tab.FILE_GLOB_COLUMN).text(),
            "/photos/first/**/*.jpg",
        )
        self.assertEqual(
            [
                import_tab.import_table.horizontalHeaderItem(column).text()
                for column in range(import_tab.import_table.columnCount())
            ],
            ["Rows", "file_glob", "import_params", "Actions"],
        )
        params_button = import_tab.import_table.cellWidget(
            0, import_tab.IMPORT_PARAMS_COLUMN
        )
        self.assertEqual(params_button.text(), "View YAML")
        opened_yaml = []
        import_tab._fetch_first_import_params = lambda input_sha: (
            first_yaml if input_sha == "a" * 64 else None
        )
        import_tab._show_import_parameters = opened_yaml.append
        params_button.click()
        self.assertEqual(opened_yaml, [first_yaml])
        self.assertEqual(import_tab.open_yaml_button.text(), "Import Parameters…")
        self.assertFalse(hasattr(import_tab, "connection_button"))
        self.assertFalse(hasattr(import_tab, "skip_existing_input"))
        expected_actions = [
            "Update",
            "Update New",
            "Drop Old",
            "Drop All",
            "Sync (Drop Old/Update)",
            "Fetch Metadata Structure",
        ]
        self.assertEqual(
            [action.text() for action in import_tab.actions_button.menu().actions()],
            expected_actions,
        )
        row_actions = import_tab.import_table.cellWidget(0, import_tab.ACTIONS_COLUMN)
        self.assertEqual(
            [action.text() for action in row_actions.menu().actions()],
            expected_actions,
        )
        fetched = []
        import_tab.fetch_metadata = fetched.append
        import_tab.actions_button.menu().actions()[-1].trigger()
        row_actions.menu().actions()[-1].trigger()
        self.assertEqual(fetched, ["all", "a" * 64])

    def test_import_editor_saves_one_settings_document(self):
        settings = QtCore.QSettings()
        had_original = settings.contains(IMPORT_PARAMETERS_KEY)
        original = settings.value(IMPORT_PARAMETERS_KEY) if had_original else None
        try:
            save_import_parameters("source:\n  file_glob: /tmp/*.jpg\n")
            self.assertEqual(
                load_import_parameters("default"),
                "source:\n  file_glob: /tmp/*.jpg\n",
            )
            dialog = ImportYamlDialog(
                load_example_import_yaml(),
                lambda value: value,
                presets=load_import_presets(),
            )
            self.assertEqual(dialog.save_button.text(), "Save")
            self.assertEqual(dialog.copy_button.text(), "Copy")
            dialog.copy_button.click()
            self.assertEqual(
                QtWidgets.QApplication.clipboard().text(), dialog.yaml_text()
            )
            self.assertFalse(hasattr(dialog, "reset_button"))
            self.assertFalse(hasattr(dialog, "normalize_button"))
            self.assertEqual(dialog.preset_input.itemText(0), "Defaults")
        finally:
            if had_original:
                settings.setValue(IMPORT_PARAMETERS_KEY, original)
            else:
                settings.remove(IMPORT_PARAMETERS_KEY)

if __name__ == "__main__":
    suite = unittest.makeSuite(LandlensdbDialogTest)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
