"""Verify import action scope and runtime controls without database writes."""

import json
from unittest.mock import Mock

import pytest
from geoalchemy2 import Geometry
from qgis.PyQt import QtWidgets
from sqlalchemy import Column, MetaData, Table

from ..landlensdb import calculate_input_sha
from ..tabs import import_tab as module
from .utilities import get_qgis_app

QGIS_APP = get_qgis_app()


def configuration(folder):
    text = json.dumps(
        {
            "file_glob": "/{}/**/*.jpg".format(folder),
            "name": "file.name",
            "image_url": "file.path",
            "geometry": "point_from_exif",
        }
    )
    return calculate_input_sha(text), text


@pytest.fixture
def tab(monkeypatch):
    monkeypatch.setattr(module, "fetch_base_tables", lambda values: ["images"])
    widget = module.ImportTab(None)
    widget.refresh_table = Mock()
    widget._show_message = Mock()
    widget.load_records(
        [
            {"input_sha": sha, "row_count": 2, "import_params": text}
            for sha, text in (configuration("first"), configuration("second"))
        ]
    )
    yield widget
    widget.close()
    widget.deleteLater()


def test_action_layout_and_empty_table(tab):
    assert isinstance(tab.table_button, QtWidgets.QPushButton)
    assert tab.table_button.menu() is not None
    assert (
        tab.table_button.sizeHint().height() == tab.refresh_button.sizeHint().height()
    )
    assert isinstance(tab.actions_button, QtWidgets.QPushButton)
    assert tab.actions_button.menu() is not None
    assert (
        tab.actions_button.sizeHint().height() == tab.refresh_button.sizeHint().height()
    )
    top = tab.layout().itemAt(0).layout()
    assert top.indexOf(tab.actions_button) == top.indexOf(tab.refresh_button) + 1
    bottom = tab.layout().itemAt(tab.layout().count() - 1).layout()
    assert bottom.indexOf(tab.add_button) == bottom.indexOf(tab.open_json_button) + 1
    assert bottom.indexOf(tab.actions_button) == -1
    assert tab.output_crs_input.text() == "EPSG:4326"
    tab.load_records([])
    assert not tab.actions_button.isEnabled()
    assert tab.add_button.isEnabled()


@pytest.mark.parametrize("scope", ["table", "row"])
@pytest.mark.parametrize(
    "action",
    [
        "Update",
        "Update New",
        "Drop Old",
        "Drop All",
        "Sync (Drop Old/Update)",
        "Fetch Metadata Structure",
    ],
)
def test_menus_use_stored_groups_without_saved_import_parameters(
    tab, monkeypatch, scope, action
):
    tab._saved_config = Mock(
        side_effect=AssertionError("Only Add uses saved parameters")
    )
    tab._run_updates = Mock()
    tab._run_drop_old = Mock(return_value=0)
    tab._run_drop_all = Mock(return_value=0)
    tab.fetch_metadata = Mock()
    monkeypatch.setattr(
        QtWidgets.QMessageBox, "question", lambda *args: QtWidgets.QMessageBox.Yes
    )
    # Highlighting one group does not narrow the table-wide menu.
    tab.import_table.selectRow(1)
    configs = [configuration("first"), configuration("second")]
    if scope == "table":
        button = tab.actions_button
    else:
        button = tab.import_table.cellWidget(1, tab.ACTIONS_COLUMN)
        configs = configs[1:]
    next(item for item in button.menu().actions() if item.text() == action).trigger()
    if action in ("Update", "Update New", "Sync (Drop Old/Update)"):
        tab._run_updates.assert_called_once_with(configs, action == "Update New")
    if action in ("Drop Old", "Sync (Drop Old/Update)"):
        tab._run_drop_old.assert_called_once_with(configs)
    if action == "Drop All":
        tab._run_drop_all.assert_called_once_with(configs)
    if action == "Fetch Metadata Structure":
        tab.fetch_metadata.assert_called_once_with(
            "all" if scope == "table" else configs[0][0]
        )
    tab._saved_config.assert_not_called()


@pytest.fixture
def database(tab):
    db = Mock()
    db.selected_table = Table(
        "images", MetaData(), Column("geometry", Geometry(srid=3857))
    )
    tab._database = Mock(return_value=db)
    tab.output_crs_input.setText("EPSG:3857")
    return db


@pytest.mark.parametrize("add_only", [True, False])
def test_add_and_update_pass_runtime_controls_and_protect_other_groups(
    tab, database, monkeypatch, add_only
):
    saved = configuration("new")
    tab._saved_config = Mock(return_value=saved)
    tab.thread_count_input.setValue(3)
    tab.batch_size_input.setValue(25)
    tab.on_error_input.setCurrentText("error")
    batch = object()

    def import_images(config, **kwargs):
        assert not tab.add_button.isEnabled()
        assert not tab.actions_button.isEnabled()
        assert not tab.import_table.isEnabled()
        assert not tab.table_button.isEnabled()
        assert not tab.output_crs_input.isEnabled()
        assert tab.cancel_button.isEnabled()
        assert kwargs["output_crs"] == "EPSG:3857"
        assert kwargs["max_workers"] == 3
        assert kwargs["batch_size"] == 25
        assert kwargs["on_error"] == "error"
        assert kwargs["skip_images_in_postgresql"] is database
        assert kwargs["skip_existing"] == add_only
        assert kwargs["return_as_yield"] is True
        assert kwargs["cancel_event"] is tab._cancel_import_event
        kwargs["progress_callback"](1, 1)
        return iter([batch])

    importer = Mock(side_effect=import_images)
    monkeypatch.setattr(module, "import_local_images", importer)
    if add_only:
        tab.add_button.click()
        configs = [saved]
    else:
        tab.run_all_updates()
        configs = [configuration("first"), configuration("second")]
        tab._saved_config.assert_not_called()
    assert [call.args[0] for call in importer.call_args_list] == [
        json.loads(text) for _, text in configs
    ]
    assert database.upsert_images.call_count == len(configs)
    for call, (sha, _) in zip(database.upsert_images.call_args_list, configs):
        assert call.args == (batch, "images")
        assert call.kwargs == {
            "conflict": "nothing" if add_only else "update",
            "input_sha": None if add_only else sha,
        }
    assert tab.add_button.isEnabled()
    assert tab.actions_button.isEnabled()
    assert tab.output_crs_input.isEnabled()
    assert not tab.cancel_button.isEnabled()


@pytest.mark.parametrize("crs", ["invalid", "EPSG:4326"])
def test_invalid_or_mismatched_crs_cannot_write(tab, database, monkeypatch, crs):
    importer = Mock()
    monkeypatch.setattr(module, "import_local_images", importer)
    tab.output_crs_input.setText(crs)
    tab.run_all_updates()
    importer.assert_not_called()
    database.upsert_images.assert_not_called()
    assert "CRS" in tab._show_message.call_args.args[0]


def test_invalid_saved_parameters_do_not_start_add(tab):
    tab._saved_config = Mock(side_effect=ValueError("Invalid JSON"))
    tab._run_updates = Mock()
    tab.add_button.click()
    tab._run_updates.assert_not_called()
    assert "Invalid Import Parameters" in tab._show_message.call_args.args[0]
