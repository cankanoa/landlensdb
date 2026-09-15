"""Verify import action scope and runtime controls without database writes."""

import json
from unittest.mock import MagicMock, Mock

import pytest
from geoalchemy2 import Geometry
from qgis.PyQt import QtCore, QtGui, QtWidgets
from sqlalchemy import Column, MetaData, Table, Text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from ..landlensdb import calculate_input_sha
from ..shared import import_settings
from ..tabs import import_tab as module
from ..shared import import_task as tasks
from .utilities import get_qgis_app

QGIS_APP = get_qgis_app()


def wait_for_operation(tab):
    loop = QtCore.QEventLoop()
    timer = QtCore.QTimer()
    timer.timeout.connect(lambda: loop.quit() if not tab._import_active else None)
    timer.start(5)
    QtCore.QTimer.singleShot(5000, loop.quit)
    if tab._import_active:
        loop.exec_()
    timer.stop()
    assert not tab._import_active, "Background operation did not finish"


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
def table_cursor(monkeypatch):
    monkeypatch.setattr(module, "fetch_base_tables", lambda values: ["images"])
    connect = MagicMock()
    monkeypatch.setattr(module.psycopg2, "connect", connect)
    connection = connect.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.table_columns = dict(module.IMPORT_TABLE_COLUMNS)
    cursor.import_groups = []

    def execute(statement, parameters=None):
        cursor.fetchall.return_value = (
            list(cursor.table_columns.items())
            if isinstance(statement, str) and "information_schema.columns" in statement
            else cursor.import_groups
        )

    cursor.execute.side_effect = execute
    return cursor


@pytest.fixture
def tab(table_cursor):
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
    if widget._import_active:
        widget._cancel_active_import()
        wait_for_operation(widget)
    widget.close()
    widget.deleteLater()


def test_initial_selection_checks_schema_before_loading_groups(tab, table_cursor):
    calls = table_cursor.execute.call_args_list
    assert "information_schema.columns" in calls[0].args[0]
    assert calls[0].args[1] == ("public", "images")
    assert "SELECT input_sha" in str(calls[1].args[0])
    assert "MIN(metadata ->> 'import_params')" in str(calls[1].args[0])
    assert tab._table_valid


def test_new_table_stores_import_parameters_only_in_metadata(
    tab, table_cursor, monkeypatch
):
    dialog = Mock()
    dialog.exec_.return_value = True
    dialog.table_name.return_value = "new_images"
    monkeypatch.setattr(module, "AddTableDialog", lambda *args: dialog)
    tab.add_table()
    statement = next(
        str(call.args[0])
        for call in table_cursor.execute.call_args_list
        if "CREATE TABLE" in str(call.args[0])
    )
    assert "metadata jsonb" in statement
    assert "input_sha text NOT NULL" in statement
    assert "import_params" not in statement


def test_background_group_reads_extract_json_text_from_metadata():
    database = Mock()
    database.selected_table = Table(
        "images", MetaData(), Column("input_sha", Text), Column("metadata", JSONB)
    )
    database.engine = MagicMock()
    connection = database.engine.connect.return_value.__enter__.return_value
    sha, config = configuration("stored")
    connection.execute.return_value = [(sha, 2, config)]
    assert tasks.read_import_groups(database) == [
        {"input_sha": sha, "row_count": 2, "import_params": config}
    ]
    compiled = connection.execute.call_args.args[0].compile(
        dialect=postgresql.dialect()
    )
    assert "min(images.metadata ->>" in str(compiled)
    assert "images.import_params" not in str(compiled)
    assert "import_params" in compiled.params.values()


def test_background_missing_config_is_loaded_from_metadata(monkeypatch):
    sha, config = configuration("stored")
    database = Mock()
    database.selected_table = Table(
        "images", MetaData(), Column("input_sha", Text), Column("metadata", JSONB)
    )
    database.engine = MagicMock()
    connection = database.engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar.return_value = config
    database.remove_unmatched_for_input.return_value = 0
    monkeypatch.setattr(tasks, "open_database", lambda *args: database)
    monkeypatch.setattr(tasks, "discover_image_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(tasks, "read_import_groups", lambda *args: [])
    task = tasks.ImportTask(
        "drop_old",
        [(sha, None)],
        database_url="unused",
        connect_args={},
        table_name="images",
    )
    assert task.run(), task.error
    compiled = connection.execute.call_args.args[0].compile(
        dialect=postgresql.dialect()
    )
    assert "images.metadata ->>" in str(compiled)
    assert "images.import_params" not in str(compiled)
    database.remove_unmatched_for_input.assert_called_once_with(sha, [])


def test_selecting_invalid_table_clears_old_groups_and_blocks_imports(
    tab, table_cursor, monkeypatch
):
    tab.refresh_table = module.ImportTab.refresh_table.__get__(tab)
    monkeypatch.setattr(
        module, "fetch_base_tables", lambda values: ["images", "old_images"]
    )
    tab._refresh_table_choices()
    tab.load_records(
        [
            {"input_sha": sha, "row_count": 2, "import_params": text}
            for sha, text in [configuration("old")]
        ]
    )
    del table_cursor.table_columns["input_sha"]
    del table_cursor.table_columns["metadata"]
    table_cursor.execute.reset_mock()
    tab.table_button.menu().actions()[1].trigger()
    assert tab.current_table_name() == "old_images"
    assert table_cursor.execute.call_count == 1
    assert table_cursor.execute.call_args.args[1] == ("public", "old_images")
    assert "missing input_sha" in tab._show_message.call_args.args[0]
    assert "missing metadata" in tab._show_message.call_args.args[0]
    assert tab.import_table.rowCount() == 0
    assert not tab._table_valid
    assert not tab.actions_button.isEnabled()
    assert not tab.add_button.isEnabled()
    tab._set_import_active(False)
    assert not tab.add_button.isEnabled()
    task_factory = Mock()
    monkeypatch.setattr(module, "ImportTask", task_factory)
    tab._run_updates([configuration("new")], skip_existing=True, add_only=True)
    task_factory.assert_not_called()


def test_refresh_reports_wrong_type_and_allows_retry_after_correction(
    tab, table_cursor
):
    table_cursor.table_columns["metadata"] = "text"
    tab.refresh_button.click()
    assert "metadata is text; expected jsonb" in tab._show_message.call_args.args[0]
    assert not tab.add_button.isEnabled()
    table_cursor.table_columns["metadata"] = "jsonb"
    sha, config = configuration("fixed")
    table_cursor.import_groups = [(sha, 3, config)]
    tab.refresh_button.click()
    assert tab._table_valid
    assert tab.add_button.isEnabled()
    assert tab.actions_button.isEnabled()
    assert tab.import_table.item(0, tab.COUNT_COLUMN).text() == "3"


def test_changing_connection_revalidates_same_table_in_new_schema(tab, table_cursor):
    tab.refresh_table = module.ImportTab.refresh_table.__get__(tab)
    table_cursor.execute.reset_mock()
    values = dict(tab.connection_values, schema="another_schema")
    tab.reload_connection_settings(values)
    assert table_cursor.execute.call_args_list[0].args[1] == (
        "another_schema",
        "images",
    )


def test_no_tables_clears_previous_rows_and_disables_import(tab, monkeypatch):
    tab.refresh_table = module.ImportTab.refresh_table.__get__(tab)
    monkeypatch.setattr(module, "fetch_base_tables", lambda values: [])
    tab._refresh_table_choices()
    assert tab.current_table_name() is None
    assert tab.table_button.text() == "Choose Table"
    assert tab.import_table.rowCount() == 0
    assert not tab.add_button.isEnabled()
    assert not tab.actions_button.isEnabled()


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


@pytest.mark.parametrize(
    "button_name", ["close_button", "copy_button", "update_button"]
)
def test_view_json_buttons_only_update_qgis_settings_on_update(
    tab, table_cursor, monkeypatch, tmp_path, button_name
):
    settings = QtCore.QSettings(
        str(tmp_path / "imports.ini"), QtCore.QSettings.IniFormat
    )
    monkeypatch.setattr(import_settings.QtCore, "QSettings", lambda: settings)
    original = configuration("current")[1]
    stored = configuration("stored")[1]
    edited = configuration("edited")[1]
    import_settings.save_import_parameters(original)
    table_cursor.fetchone.return_value = (stored,)
    table_cursor.execute.reset_mock()

    def use_viewer(dialog):
        assert dialog.windowTitle() == "View JSON"
        assert dialog.json_text() == stored
        assert dialog.editor.lineWrapMode() == QtWidgets.QPlainTextEdit.WidgetWidth
        assert (
            dialog.editor.wordWrapMode()
            == QtGui.QTextOption.WrapAtWordBoundaryOrAnywhere
        )
        buttons = dialog.layout().itemAt(dialog.layout().count() - 1).layout()
        assert [buttons.itemAt(i).widget().text() for i in range(1, 4)] == [
            "Close",
            "Copy",
            "Update Import Text",
        ]
        dialog.editor.setPlainText(edited)
        getattr(dialog, button_name).click()
        if button_name == "copy_button":
            assert QtWidgets.QApplication.clipboard().text() == edited
            dialog.close_button.click()
        return dialog.result()

    monkeypatch.setattr(module.ImportGroupJsonDialog, "exec_", use_viewer)
    tab.open_group_import_parameters(configuration("stored")[0])
    expected = edited if button_name == "update_button" else original
    assert import_settings.load_import_parameters("default") == expected
    table_cursor.execute.assert_called_once()
    assert "SELECT metadata ->> 'import_params'" in str(
        table_cursor.execute.call_args.args[0]
    )
    reopened = module.ImportJsonDialog(expected, module.normalize_import_json)
    assert reopened.json_text() == expected
    reopened.close()


def test_view_json_invalid_text_and_save_failure_keep_viewer_open(tab, monkeypatch):
    save = Mock(side_effect=OSError("Settings unavailable"))
    monkeypatch.setattr(module, "save_import_parameters", save)

    def use_viewer(dialog):
        dialog.editor.setPlainText("invalid JSON")
        dialog.update_button.click()
        save.assert_not_called()
        assert dialog.validation_label.text()
        assert dialog.result() == QtWidgets.QDialog.Rejected
        dialog.editor.setPlainText(configuration("edited")[1])
        dialog.update_button.click()
        save.assert_called_once_with(dialog.json_text())
        assert "Settings unavailable" in dialog.validation_label.text()
        assert dialog.result() == QtWidgets.QDialog.Rejected
        dialog.close_button.click()
        return dialog.result()

    monkeypatch.setattr(module.ImportGroupJsonDialog, "exec_", use_viewer)
    tab._show_group_import_parameters(configuration("stored")[1])
    tab._show_message.assert_not_called()


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
    tab._start_operation = Mock()
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
    if action in ("Update", "Update New"):
        tab._run_updates.assert_called_once_with(configs, action == "Update New")
    if action == "Drop Old":
        tab._run_drop_old.assert_called_once_with(configs)
    if action == "Sync (Drop Old/Update)":
        tab._start_operation.assert_called_once_with("sync", configs)
    if action == "Drop All":
        tab._run_drop_all.assert_called_once_with(configs)
    if action == "Fetch Metadata Structure":
        tab.fetch_metadata.assert_called_once_with(
            "all" if scope == "table" else configs[0][0]
        )
    tab._saved_config.assert_not_called()


@pytest.fixture
def database(tab, monkeypatch):
    db = Mock()
    db.selected_table = Table(
        "images", MetaData(), Column("geometry", Geometry(srid=3857))
    )
    monkeypatch.setattr(tasks, "open_database", Mock(return_value=db))
    monkeypatch.setattr(
        tasks,
        "read_import_groups",
        Mock(
            return_value=[
                {"input_sha": sha, "row_count": 2, "import_params": text}
                for sha, text in (configuration("first"), configuration("second"))
            ]
        ),
    )
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
    monkeypatch.setattr(tasks, "import_local_images", importer)
    if add_only:
        tab.add_button.click()
        configs = [saved]
    else:
        tab.run_all_updates()
        configs = [configuration("first"), configuration("second")]
        tab._saved_config.assert_not_called()
    assert not tab.add_button.isEnabled()
    assert not tab.actions_button.isEnabled()
    assert not tab.import_table.isEnabled()
    assert not tab.table_button.isEnabled()
    assert not tab.output_crs_input.isEnabled()
    assert tab.cancel_button.isEnabled()
    wait_for_operation(tab)
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
    monkeypatch.setattr(tasks, "import_local_images", importer)
    tab.output_crs_input.setText(crs)
    tab.run_all_updates()
    wait_for_operation(tab)
    importer.assert_not_called()
    database.upsert_images.assert_not_called()
    assert "CRS" in tab._show_message.call_args.args[0]


def test_invalid_saved_parameters_do_not_start_add(tab):
    tab._saved_config = Mock(side_effect=ValueError("Invalid JSON"))
    tab._run_updates = Mock()
    tab.add_button.click()
    tab._run_updates.assert_not_called()
    assert "Invalid Import Parameters" in tab._show_message.call_args.args[0]


@pytest.mark.parametrize(
    "error, message",
    [
        (
            "No files match `file_glob`: C:/Photos/*.jpg",
            "Import failed: No files match",
        ),
        ("No new files match `file_glob`.", "No new images were found."),
    ],
)
def test_missing_files_are_reported_separately_from_already_imported_files(
    tab, database, monkeypatch, error, message
):
    monkeypatch.setattr(
        tasks, "import_local_images", Mock(side_effect=ValueError(error))
    )
    tab._run_updates([configuration("photos")], skip_existing=True, add_only=True)
    wait_for_operation(tab)
    assert message in tab._show_message.call_args.args[0]
    database.upsert_images.assert_not_called()


def wait_until(condition):
    loop = QtCore.QEventLoop()
    timer = QtCore.QTimer()
    timer.timeout.connect(lambda: loop.quit() if condition() else None)
    timer.start(5)
    QtCore.QTimer.singleShot(5000, loop.quit)
    if not condition():
        loop.exec_()
    timer.stop()
    assert condition(), "Timed out waiting for background task"


def test_worker_batch_counts_remain_visible_during_database_writes(
    tab, database, tmp_path
):
    import threading

    for index in range(3):
        (tmp_path / f"{index}.jpg").touch()
    text = json.dumps(
        {
            "file_glob": str(tmp_path / "*.jpg"),
            "name": "file.name",
            "image_url": "file.path",
            "geometry": {
                "upper_left": [0, 1],
                "upper_right": [1, 1],
                "lower_right": [1, 0],
                "lower_left": [0, 0],
            },
            "thumbnail": {"enabled": False},
        }
    )
    tab.batch_size_input.setValue(1)
    tab.thread_count_input.setValue(2)
    writing = [threading.Event() for _ in range(3)]
    release = [threading.Event() for _ in range(3)]
    writes = []

    def write(images, *args, **kwargs):
        index = len(writes)
        writes.append(list(images["name"]))
        writing[index].set()
        assert release[index].wait(5)

    database.upsert_images.side_effect = write
    tab.run_row_updates(calculate_input_sha(text), text)
    try:
        for index in range(3):
            wait_until(writing[index].is_set)
            # Drain queued task signals while this database write is blocked.
            QtWidgets.QApplication.processEvents()
            assert tab._import_active
            assert tab.progress_bar.maximum() == 3
            assert tab.progress_bar.value() == index + 1
            assert tab.progress_bar.text() == f"{index + 1}/3"
            release[index].set()
    finally:
        for event in release:
            event.set()
        wait_for_operation(tab)
    assert sorted(name for batch in writes for name in batch) == [
        "0.jpg",
        "1.jpg",
        "2.jpg",
    ]
    assert tab.progress_bar.text() == "Completed"


@pytest.mark.parametrize("operation", ["update", "drop_old", "sync"])
@pytest.mark.parametrize("cancel", [False, True])
def test_glob_work_keeps_qt_responsive_and_cancellation_stops_writes(
    tab, database, monkeypatch, operation, cancel
):
    import threading
    from pathlib import Path

    main_thread = threading.get_ident()
    entered = threading.Event()
    release = threading.Event()
    threads = []
    paths = [Path("scene.jpg")]

    def discover(*args, **kwargs):
        threads.append(threading.get_ident())
        entered.set()
        assert release.wait(5)
        return paths

    def load(config, **kwargs):
        if kwargs["discovered_paths"] is None:
            discover(config["file_glob"])
        threads.append(threading.get_ident())
        kwargs["progress_callback"](1, 1)
        return iter([object()])

    def refresh(db):
        threads.append(threading.get_ident())
        return []

    monkeypatch.setattr(tasks, "discover_image_paths", Mock(side_effect=discover))
    monkeypatch.setattr(tasks, "import_local_images", Mock(side_effect=load))
    monkeypatch.setattr(tasks, "read_import_groups", refresh)
    database.remove_unmatched_for_input.return_value = 0
    tab._start_operation(operation, [configuration("photos")])
    try:
        assert tab._import_active
        wait_until(entered.is_set)
        ticks = []
        QtCore.QTimer.singleShot(0, lambda: ticks.append(threading.get_ident()))
        wait_until(lambda: bool(ticks))
        assert ticks == [main_thread]
        assert tab._import_active and not release.is_set()
        # Starting again must not create a second writer while this task runs.
        tab._start_operation(operation, [configuration("other")])
        assert tasks.open_database.call_count == 1
        if cancel:
            tab.cancel_button.click()
            assert tab._cancel_import_event.is_set()
    finally:
        release.set()
        wait_for_operation(tab)
    assert threads and all(thread != main_thread for thread in threads)
    database.engine.dispose.assert_called_once()
    if cancel:
        database.upsert_images.assert_not_called()
        database.remove_unmatched_for_input.assert_not_called()
        assert "cancelled" in tab._show_message.call_args.args[0]
    else:
        if operation == "sync":
            tasks.discover_image_paths.assert_called_once()
            assert (
                tasks.import_local_images.call_args.kwargs["discovered_paths"] is paths
            )
        assert database.upsert_images.call_count == int(operation != "drop_old")


def test_task_cancellation_before_run_never_opens_database(monkeypatch):
    open_database = Mock()
    monkeypatch.setattr(tasks, "open_database", open_database)
    task = tasks.ImportTask(
        "update",
        [configuration("photos")],
        database_url="unused",
        connect_args={},
        table_name="images",
    )
    task.cancel()
    assert not task.run()
    assert isinstance(task.error, tasks.ImportCancelledError)
    open_database.assert_not_called()
