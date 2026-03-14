# -*- coding: utf-8 -*-

import os
import sys

from qgis.PyQt import QtCore, QtGui, QtWidgets

try:
    from qgis.gui import QgsCodeEditorShell
except ImportError:  # pragma: no cover
    QgsCodeEditorShell = None

from ..shared.connection_utils import load_connection_settings, save_connection_settings, test_connection_values


class SetupTab(QtWidgets.QWidget):
    connectionSaved = QtCore.pyqtSignal(dict)

    RUNTIME_PREFIX = 'Landlensdb/runtime'

    def __init__(self, iface, parent=None):
        super(SetupTab, self).__init__(parent)
        self.iface = iface
        self.connection_values = load_connection_settings()
        self.runtime_values = self._load_runtime_settings()
        self._process = None
        self._process_target = None

        self._build_ui()
        self._wire_signals()
        self._apply_dependency_mode()
        self._apply_server_mode()
        self._apply_runtime_mode()
        self._refresh_connection_summary()

    def _build_ui(self):
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal, self)
        root_layout.addWidget(self.main_splitter)

        left_scroll = QtWidgets.QScrollArea(self)
        left_scroll.setWidgetResizable(True)
        self.main_splitter.addWidget(left_scroll)

        left_content = QtWidgets.QWidget()
        left_scroll.setWidget(left_content)

        layout = QtWidgets.QVBoxLayout(left_content)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(18)

        dependencies_section = QtWidgets.QWidget()
        dependencies_section_layout = QtWidgets.QVBoxLayout(dependencies_section)
        dependencies_section_layout.setContentsMargins(0, 0, 0, 0)
        dependencies_section_layout.setSpacing(6)

        dependencies_header = QtWidgets.QHBoxLayout()
        dependencies_title = QtWidgets.QLabel('Python Dependencies')
        dependencies_header.addWidget(dependencies_title)
        dependencies_header.addStretch()
        self.reset_dependencies_button = QtWidgets.QPushButton('Reset')
        dependencies_header.addWidget(self.reset_dependencies_button)
        dependencies_section_layout.addLayout(dependencies_header)

        self.dependencies_group = QtWidgets.QFrame()
        self.dependencies_group.setFrameShape(QtWidgets.QFrame.StyledPanel)
        dependencies_layout = QtWidgets.QVBoxLayout(self.dependencies_group)
        dependencies_layout.setSpacing(10)

        self.pip_radio = QtWidgets.QRadioButton('Python Exacutable')
        self.conda_radio = QtWidgets.QRadioButton('Conda Environment')
        self.docker_radio = QtWidgets.QRadioButton('Docker Environment')
        self.pip_radio.setChecked(True)

        radio_row = QtWidgets.QHBoxLayout()
        radio_row.addWidget(self.pip_radio)
        radio_row.addWidget(self.conda_radio)
        radio_row.addWidget(self.docker_radio)
        radio_row.addStretch()
        dependencies_layout.addLayout(radio_row)

        self.dependency_stack = QtWidgets.QStackedWidget()
        self.dependency_stack.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        dependencies_layout.addWidget(self.dependency_stack)
        self.dependency_stack.addWidget(self._build_pip_panel())
        self.dependency_stack.addWidget(self._build_conda_panel())
        self.dependency_stack.addWidget(self._build_docker_panel())
        dependencies_section_layout.addWidget(self.dependencies_group)
        layout.addWidget(dependencies_section)

        server_section = QtWidgets.QWidget()
        server_section_layout = QtWidgets.QVBoxLayout(server_section)
        server_section_layout.setContentsMargins(0, 0, 0, 0)
        server_section_layout.setSpacing(6)

        server_header = QtWidgets.QHBoxLayout()
        server_title = QtWidgets.QLabel('Create PostgreSQL Server')
        server_header.addWidget(server_title)
        server_header.addStretch()
        self.reset_server_button = QtWidgets.QPushButton('Reset')
        server_header.addWidget(self.reset_server_button)
        server_section_layout.addLayout(server_header)

        self.server_group = QtWidgets.QFrame()
        self.server_group.setFrameShape(QtWidgets.QFrame.StyledPanel)
        server_layout = QtWidgets.QVBoxLayout(self.server_group)
        server_layout.setSpacing(10)

        self.server_installer_radio = QtWidgets.QRadioButton('Installer Package')
        self.server_homebrew_radio = QtWidgets.QRadioButton('Homebrew')
        self.server_windows_radio = QtWidgets.QRadioButton('Windows')
        self.server_conda_radio = QtWidgets.QRadioButton('Conda')
        self.server_homebrew_radio.setChecked(True)

        server_radio_row = QtWidgets.QHBoxLayout()
        server_radio_row.addWidget(self.server_installer_radio)
        server_radio_row.addWidget(self.server_homebrew_radio)
        server_radio_row.addWidget(self.server_windows_radio)
        server_radio_row.addWidget(self.server_conda_radio)
        server_radio_row.addStretch()
        server_layout.addLayout(server_radio_row)

        self.server_stack = QtWidgets.QStackedWidget()
        self.server_stack.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        server_layout.addWidget(self.server_stack)
        self.server_stack.addWidget(
            self._build_single_command_panel(
                'PostGIS must be installed after PostgreSQL, and GDAL raster drivers must be enabled for the database before raster queries will work.',
                'https://www.postgresql.org/download/',
                'Open',
                'server_installer',
            )
        )
        self.server_stack.addWidget(
            self._build_single_command_panel(
                'Install PostgreSQL/PostGIS with Homebrew and enable GDAL raster drivers for landlens_test.',
                self._homebrew_server_command(),
                'Run',
                'server_homebrew',
            )
        )
        self.server_stack.addWidget(
            self._build_single_command_panel(
                'Install PostgreSQL from Windows package manager. PostGIS must still be installed separately and GDAL drivers enabled in the database.',
                'winget install PostgreSQL.PostgreSQL',
                'Run',
                'server_windows',
            )
        )
        self.server_stack.addWidget(
            self._build_single_command_panel(
                'Install PostgreSQL/PostGIS into a conda environment and enable GDAL raster drivers for landlens_test.',
                self._conda_server_command(),
                'Run',
                'server_conda',
            )
        )
        server_section_layout.addWidget(self.server_group)
        layout.addWidget(server_section)

        runtime_section = QtWidgets.QWidget()
        runtime_section_layout = QtWidgets.QVBoxLayout(runtime_section)
        runtime_section_layout.setContentsMargins(0, 0, 0, 0)
        runtime_section_layout.setSpacing(6)

        runtime_header = QtWidgets.QHBoxLayout()
        runtime_title = QtWidgets.QLabel('Choose Python Environment')
        runtime_header.addWidget(runtime_title)
        runtime_header.addStretch()
        self.reset_runtime_button = QtWidgets.QPushButton('Reset')
        runtime_header.addWidget(self.reset_runtime_button)
        runtime_section_layout.addLayout(runtime_header)

        self.runtime_group = QtWidgets.QFrame()
        self.runtime_group.setFrameShape(QtWidgets.QFrame.StyledPanel)
        runtime_layout = QtWidgets.QVBoxLayout(self.runtime_group)
        runtime_layout.setSpacing(10)

        self.runtime_python_radio = QtWidgets.QRadioButton('Python Exacutable')
        self.runtime_conda_radio = QtWidgets.QRadioButton('Conda Environment')
        self.runtime_docker_radio = QtWidgets.QRadioButton('Docker Environment')

        runtime_radio_row = QtWidgets.QHBoxLayout()
        runtime_radio_row.addWidget(self.runtime_python_radio)
        runtime_radio_row.addWidget(self.runtime_conda_radio)
        runtime_radio_row.addWidget(self.runtime_docker_radio)
        runtime_radio_row.addStretch()
        runtime_layout.addLayout(runtime_radio_row)

        self.runtime_stack = QtWidgets.QStackedWidget()
        self.runtime_stack.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        runtime_layout.addWidget(self.runtime_stack)
        self.runtime_stack.addWidget(self._build_runtime_python_panel())
        self.runtime_stack.addWidget(self._build_runtime_conda_panel())
        self.runtime_stack.addWidget(self._build_runtime_docker_panel())

        runtime_test_row = QtWidgets.QHBoxLayout()
        self.runtime_test_button = QtWidgets.QPushButton('Test Installation')
        self.runtime_test_feedback = QtWidgets.QLabel('')
        runtime_test_row.addWidget(self.runtime_test_button)
        runtime_test_row.addWidget(self.runtime_test_feedback)
        runtime_test_row.addStretch()
        runtime_layout.addLayout(runtime_test_row)
        runtime_section_layout.addWidget(self.runtime_group)
        layout.addWidget(runtime_section)

        connection_section = QtWidgets.QWidget()
        connection_section_layout = QtWidgets.QVBoxLayout(connection_section)
        connection_section_layout.setContentsMargins(0, 0, 0, 0)
        connection_section_layout.setSpacing(6)

        connection_header = QtWidgets.QHBoxLayout()
        connection_title = QtWidgets.QLabel('Connect to PostgreSQL')
        connection_header.addWidget(connection_title)
        connection_header.addStretch()
        self.reset_connection_button = QtWidgets.QPushButton('Reset')
        connection_header.addWidget(self.reset_connection_button)
        connection_section_layout.addLayout(connection_header)

        self.connection_group = QtWidgets.QFrame()
        self.connection_group.setFrameShape(QtWidgets.QFrame.StyledPanel)
        connection_layout = QtWidgets.QVBoxLayout(self.connection_group)
        connection_layout.setSpacing(10)

        connection_grid = QtWidgets.QGridLayout()
        self.connection_name_input = QtWidgets.QLineEdit()
        self.connection_service_input = QtWidgets.QLineEdit()
        self.connection_host_input = QtWidgets.QLineEdit()
        self.connection_port_input = QtWidgets.QLineEdit()
        self.connection_database_input = QtWidgets.QLineEdit()
        self.connection_schema_input = QtWidgets.QLineEdit()
        connection_fields = [
            ('Name', self.connection_name_input, 0, 0),
            ('Service', self.connection_service_input, 0, 1),
            ('Host', self.connection_host_input, 0, 2),
            ('Port', self.connection_port_input, 1, 0),
            ('Database', self.connection_database_input, 1, 1),
            ('Schema', self.connection_schema_input, 1, 2),
        ]
        for label_text, widget, row, column in connection_fields:
            field_layout = QtWidgets.QVBoxLayout()
            field_layout.addWidget(QtWidgets.QLabel(label_text))
            field_layout.addWidget(widget)
            connection_grid.addLayout(field_layout, row, column)
        connection_layout.addLayout(connection_grid)

        connection_test_row = QtWidgets.QHBoxLayout()
        self.connection_test_button = QtWidgets.QPushButton('Test Connection')
        self.connection_feedback = QtWidgets.QLabel('')
        connection_test_row.addWidget(self.connection_test_button)
        connection_test_row.addWidget(self.connection_feedback)
        connection_test_row.addStretch()
        connection_layout.addLayout(connection_test_row)
        connection_section_layout.addWidget(self.connection_group)
        layout.addWidget(connection_section)
        layout.addStretch()

        log_panel = QtWidgets.QWidget()
        log_layout = QtWidgets.QVBoxLayout(log_panel)
        log_layout.setContentsMargins(12, 12, 12, 12)
        log_layout.setSpacing(10)

        log_header = QtWidgets.QHBoxLayout()
        log_label = QtWidgets.QLabel('Setup Log')
        log_header.addWidget(log_label)
        log_header.addStretch()
        self.run_manual_button = QtWidgets.QPushButton('Run')
        log_header.addWidget(self.run_manual_button)
        log_layout.addLayout(log_header)

        self.setup_log = QtWidgets.QPlainTextEdit()
        self.setup_log.setReadOnly(True)
        self.setup_log.setMaximumBlockCount(500)
        log_layout.addWidget(self.setup_log, 1)

        self.shell_input = self._create_shell_input()
        log_layout.addWidget(self.shell_input)

        self.main_splitter.addWidget(log_panel)
        self.main_splitter.setStretchFactor(0, 3)
        self.main_splitter.setStretchFactor(1, 2)
        self.main_splitter.setSizes([760, 360])

    def _build_pip_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setSpacing(8)

        path_row = QtWidgets.QHBoxLayout()
        path_row.addWidget(QtWidgets.QLabel('Python interpreter'))
        self.python_path_input = QtWidgets.QLineEdit(self._default_python_executable())
        path_row.addWidget(self.python_path_input, 1)
        self.python_browse_button = QtWidgets.QPushButton('Browse')
        path_row.addWidget(self.python_browse_button)
        layout.addLayout(path_row)

        command_row = QtWidgets.QHBoxLayout()
        self.pip_command_preview = QtWidgets.QLineEdit()
        command_row.addWidget(self.pip_command_preview, 1)
        self.pip_run_button = QtWidgets.QPushButton('Run')
        command_row.addWidget(self.pip_run_button)
        layout.addLayout(command_row)

        self._update_pip_preview()
        return widget

    def _build_conda_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setSpacing(10)

        self.conda_commands_edit = QtWidgets.QPlainTextEdit()
        self.conda_commands_edit.setPlainText(self._default_conda_commands())
        self.conda_commands_edit.setMinimumHeight(92)
        self.conda_commands_edit.setMaximumHeight(92)
        layout.addWidget(self.conda_commands_edit, 1)

        button_column = QtWidgets.QVBoxLayout()
        self.conda_run_button = QtWidgets.QPushButton('Run')
        button_column.addWidget(self.conda_run_button)
        button_column.addStretch()
        layout.addLayout(button_column)
        return widget

    def _build_docker_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setSpacing(10)

        self.docker_commands_edit = QtWidgets.QPlainTextEdit()
        self.docker_commands_edit.setPlainText(self._default_docker_commands())
        self.docker_commands_edit.setMinimumHeight(92)
        self.docker_commands_edit.setMaximumHeight(92)
        layout.addWidget(self.docker_commands_edit, 1)

        button_column = QtWidgets.QVBoxLayout()
        self.docker_run_button = QtWidgets.QPushButton('Run')
        button_column.addWidget(self.docker_run_button)
        button_column.addStretch()
        layout.addLayout(button_column)
        return widget

    def _build_single_command_panel(self, description, command, button_text, prefix):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setSpacing(8)

        label = QtWidgets.QLabel(description)
        label.setWordWrap(True)
        layout.addWidget(label)

        command_row = QtWidgets.QHBoxLayout()
        command_input = QtWidgets.QLineEdit(command)
        button = QtWidgets.QPushButton(button_text)
        command_row.addWidget(command_input, 1)
        command_row.addWidget(button)
        layout.addLayout(command_row)

        setattr(self, '{}_command_input'.format(prefix), command_input)
        setattr(self, '{}_button'.format(prefix), button)
        return widget

    def _build_runtime_python_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setSpacing(10)

        self.runtime_python_path_input = QtWidgets.QLineEdit(
            self._normalize_python_executable(
                self.runtime_values.get('python_path', self._default_python_executable())
            )
        )
        layout.addWidget(self.runtime_python_path_input, 1)
        self.runtime_python_browse_button = QtWidgets.QPushButton('Browse')
        layout.addWidget(self.runtime_python_browse_button)
        return widget

    def _build_runtime_conda_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setSpacing(10)

        self.runtime_conda_name_input = QtWidgets.QLineEdit(self.runtime_values.get('conda_env', 'landlensdb_env'))
        self.runtime_conda_name_input.setPlaceholderText('Conda environment name')
        layout.addWidget(self.runtime_conda_name_input)
        return widget

    def _build_runtime_docker_panel(self):
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(widget)
        layout.setSpacing(10)

        self.runtime_docker_name_input = QtWidgets.QLineEdit(self.runtime_values.get('docker_container', 'landlensdb-local'))
        self.runtime_docker_name_input.setPlaceholderText('Docker container name')
        layout.addWidget(self.runtime_docker_name_input)
        return widget

    def _create_shell_input(self):
        if QgsCodeEditorShell is not None:
            shell = QgsCodeEditorShell(self)
            shell.setMinimumHeight(42)
            shell.setMaximumHeight(72)
            if hasattr(shell, 'setPlaceholderText'):
                shell.setPlaceholderText('Enter a command to run')
            return shell
        fallback = QtWidgets.QLineEdit(self)
        fallback.setPlaceholderText('Enter a command to run')
        return fallback

    def _wire_signals(self):
        self.reset_dependencies_button.clicked.connect(self._reset_dependency_defaults)
        self.reset_server_button.clicked.connect(self._reset_server_defaults)
        self.reset_runtime_button.clicked.connect(self._reset_runtime_defaults)
        self.reset_connection_button.clicked.connect(self._reset_connection_defaults)

        self.python_browse_button.clicked.connect(lambda: self._browse_into(self.python_path_input))
        self.python_path_input.textChanged.connect(self._update_pip_preview)
        self.pip_run_button.clicked.connect(self._run_pip_preview)

        self.conda_run_button.clicked.connect(
            lambda: self._run_multiline_commands(self.conda_commands_edit.toPlainText(), 'Conda dependencies')
        )
        self.docker_run_button.clicked.connect(
            lambda: self._run_multiline_commands(self.docker_commands_edit.toPlainText(), 'Docker environment')
        )

        self.server_installer_button.clicked.connect(
            lambda: QtGui.QDesktopServices.openUrl(QtCore.QUrl(self.server_installer_command_input.text().strip()))
        )
        self.server_homebrew_button.clicked.connect(
            lambda: self._run_single_command(self.server_homebrew_command_input.text().strip(), 'Homebrew PostgreSQL install')
        )
        self.server_windows_button.clicked.connect(
            lambda: self._run_single_command(self.server_windows_command_input.text().strip(), 'Windows PostgreSQL install')
        )
        self.server_conda_button.clicked.connect(
            lambda: self._run_single_command(self.server_conda_command_input.text().strip(), 'Conda PostgreSQL install')
        )

        self.runtime_python_browse_button.clicked.connect(
            lambda: self._browse_into(self.runtime_python_path_input, save_runtime=True)
        )
        self.runtime_python_path_input.textChanged.connect(self._save_runtime_settings)
        self.runtime_conda_name_input.textChanged.connect(self._save_runtime_settings)
        self.runtime_docker_name_input.textChanged.connect(self._save_runtime_settings)
        self.runtime_test_button.clicked.connect(self._test_runtime_installation)

        self.connection_name_input.textChanged.connect(self._store_connection_form)
        self.connection_service_input.textChanged.connect(self._store_connection_form)
        self.connection_host_input.textChanged.connect(self._store_connection_form)
        self.connection_port_input.textChanged.connect(self._store_connection_form)
        self.connection_database_input.textChanged.connect(self._store_connection_form)
        self.connection_schema_input.textChanged.connect(self._store_connection_form)
        self.connection_test_button.clicked.connect(self._test_connection_form)
        self.run_manual_button.clicked.connect(self._run_manual_command)

        self.pip_radio.toggled.connect(self._apply_dependency_mode)
        self.conda_radio.toggled.connect(self._apply_dependency_mode)
        self.docker_radio.toggled.connect(self._apply_dependency_mode)

        self.server_installer_radio.toggled.connect(self._apply_server_mode)
        self.server_homebrew_radio.toggled.connect(self._apply_server_mode)
        self.server_windows_radio.toggled.connect(self._apply_server_mode)
        self.server_conda_radio.toggled.connect(self._apply_server_mode)

        self.runtime_python_radio.toggled.connect(self._apply_runtime_mode)
        self.runtime_conda_radio.toggled.connect(self._apply_runtime_mode)
        self.runtime_docker_radio.toggled.connect(self._apply_runtime_mode)

    def set_connection_values(self, values=None):
        self.connection_values = dict(values or load_connection_settings())
        self._refresh_connection_summary()

    def _test_connection_values(self, values):
        success, message = test_connection_values(values)
        if success:
            self.connection_values = dict(values)
            save_connection_settings(self.connection_values)
            self._refresh_connection_summary()
            self._log(message)
            self.connectionSaved.emit(dict(self.connection_values))
        return success, message

    def _refresh_connection_summary(self):
        self.connection_name_input.blockSignals(True)
        self.connection_service_input.blockSignals(True)
        self.connection_host_input.blockSignals(True)
        self.connection_port_input.blockSignals(True)
        self.connection_database_input.blockSignals(True)
        self.connection_schema_input.blockSignals(True)
        self.connection_name_input.setText(self.connection_values.get('name', ''))
        self.connection_service_input.setText(self.connection_values.get('service', ''))
        self.connection_host_input.setText(self.connection_values.get('host', ''))
        self.connection_port_input.setText(self.connection_values.get('port', '5432'))
        self.connection_database_input.setText(self.connection_values.get('database', 'landlensdb'))
        self.connection_schema_input.setText(self.connection_values.get('schema', 'public'))
        self.connection_name_input.blockSignals(False)
        self.connection_service_input.blockSignals(False)
        self.connection_host_input.blockSignals(False)
        self.connection_port_input.blockSignals(False)
        self.connection_database_input.blockSignals(False)
        self.connection_schema_input.blockSignals(False)

    def _apply_dependency_mode(self):
        if self.pip_radio.isChecked():
            self.dependency_stack.setCurrentIndex(0)
        elif self.conda_radio.isChecked():
            self.dependency_stack.setCurrentIndex(1)
        else:
            self.dependency_stack.setCurrentIndex(2)
        self.server_group.setVisible(not self.docker_radio.isChecked())
        self._shrink_stack(self.dependency_stack)

    def _apply_server_mode(self):
        if self.server_installer_radio.isChecked():
            self.server_stack.setCurrentIndex(0)
        elif self.server_homebrew_radio.isChecked():
            self.server_stack.setCurrentIndex(1)
        elif self.server_windows_radio.isChecked():
            self.server_stack.setCurrentIndex(2)
        else:
            self.server_stack.setCurrentIndex(3)
        self._shrink_stack(self.server_stack)

    def _apply_runtime_mode(self):
        mode = self.runtime_values.get('mode', 'python')
        if self.runtime_python_radio.isChecked():
            self.runtime_stack.setCurrentIndex(0)
            mode = 'python'
        elif self.runtime_conda_radio.isChecked():
            self.runtime_stack.setCurrentIndex(1)
            mode = 'conda'
        elif self.runtime_docker_radio.isChecked():
            self.runtime_stack.setCurrentIndex(2)
            mode = 'docker'
        else:
            if mode == 'conda':
                self.runtime_conda_radio.setChecked(True)
            elif mode == 'docker':
                self.runtime_docker_radio.setChecked(True)
            else:
                self.runtime_python_radio.setChecked(True)
                mode = 'python'
        self.runtime_values['mode'] = mode
        self._save_runtime_settings()
        self._shrink_stack(self.runtime_stack)

    def _shrink_stack(self, stack):
        current = stack.currentWidget()
        if current is None:
            return
        height = current.sizeHint().height()
        stack.setMinimumHeight(height)
        stack.setMaximumHeight(height)

    def _browse_into(self, line_edit, save_runtime=False):
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            'Select Python Interpreter',
            line_edit.text(),
        )
        if file_path:
            line_edit.setText(self._normalize_python_executable(file_path))
            if save_runtime:
                self._save_runtime_settings()

    def _update_pip_preview(self):
        python_path = self._normalize_python_executable(
            self.python_path_input.text().strip() or self._default_python_executable()
        )
        if self.python_path_input.text().strip() != python_path:
            self.python_path_input.blockSignals(True)
            self.python_path_input.setText(python_path)
            self.python_path_input.blockSignals(False)
        self.pip_command_preview.setText(
            '"{}" -m pip install --upgrade pip && "{}" -m pip install landlensdb'.format(
                python_path,
                python_path,
            )
        )

    def _run_pip_preview(self):
        command = self.pip_command_preview.text().strip()
        self._run_single_command(command, 'Python executable dependencies')

    def _test_runtime_installation(self):
        if self.runtime_python_radio.isChecked():
            label = 'Python environment test'
            command = '"{}" -c "import importlib.util; raise SystemExit(0 if importlib.util.find_spec(\'landlensdb\') else 1)"'.format(
                self._normalize_python_executable(
                    self.runtime_python_path_input.text().strip() or self._default_python_executable()
                )
            )
        elif self.runtime_conda_radio.isChecked():
            env_name = self.runtime_conda_name_input.text().strip() or 'landlensdb_env'
            label = 'Conda environment test'
            command = 'conda run -n "{}" python -c "import importlib.util; raise SystemExit(0 if importlib.util.find_spec(\'landlensdb\') else 1)"'.format(
                env_name
            )
        else:
            container_name = self.runtime_docker_name_input.text().strip() or 'landlensdb-local'
            label = 'Docker environment test'
            command = 'docker exec "{}" python -c "import importlib.util; raise SystemExit(0 if importlib.util.find_spec(\'landlensdb\') else 1)"'.format(
                container_name
            )
        self._run_probe_command(command, label, self.runtime_test_feedback, 'landlensdb is installed.', 'landlensdb is not installed.')

    def _store_connection_form(self):
        self.connection_values = {
            'name': self.connection_name_input.text().strip(),
            'service': self.connection_service_input.text().strip(),
            'host': self.connection_host_input.text().strip() or 'localhost',
            'port': self.connection_port_input.text().strip() or '5432',
            'database': self.connection_database_input.text().strip() or 'landlensdb',
            'schema': self.connection_schema_input.text().strip() or 'public',
        }
        save_connection_settings(self.connection_values)
        self.connectionSaved.emit(dict(self.connection_values))

    def _test_connection_form(self):
        self._store_connection_form()
        success, message = self._test_connection_values(self.connection_values)
        color = '#1b8a3a' if success else '#b42318'
        self.connection_feedback.setText(message)
        self.connection_feedback.setStyleSheet('color: {}; font-weight: 600;'.format(color))

    def _run_manual_command(self):
        command = self._shell_text().strip()
        if not command:
            self._log('Manual command: nothing to run.')
            return
        self._run_single_command(command, 'Manual command')
        self._set_shell_text('')

    def _shell_text(self):
        if hasattr(self.shell_input, 'text'):
            return self.shell_input.text()
        return ''

    def _set_shell_text(self, value):
        if hasattr(self.shell_input, 'setText'):
            self.shell_input.setText(value)

    def _run_multiline_commands(self, text, label):
        commands = [line.strip() for line in text.splitlines() if line.strip()]
        if not commands:
            self._log('{}: no command to run.'.format(label))
            return
        self._run_single_command(' && '.join(commands), label)

    def _run_single_command(self, command, label):
        if not command:
            self._log('{}: no command to run.'.format(label))
            return
        if self._process is not None:
            self._log('Another setup command is still running.')
            return

        self._log('{} started.'.format(label))
        self._log(command)

        process = QtCore.QProcess(self)
        process.setProcessChannelMode(QtCore.QProcess.MergedChannels)
        process.readyReadStandardOutput.connect(self._drain_process_output)
        process.finished.connect(self._on_process_finished)

        self._process = process
        self._process_target = label
        process.start('/bin/sh', ['-lc', command])

    def _run_probe_command(self, command, label, feedback_label, success_message, failure_message):
        process = QtCore.QProcess(self)
        process.setProcessChannelMode(QtCore.QProcess.MergedChannels)

        def finish(exit_code, _exit_status):
            output = bytes(process.readAllStandardOutput()).decode('utf-8', 'replace').strip()
            success = exit_code == 0
            feedback_label.setText(success_message if success else failure_message)
            feedback_label.setStyleSheet(
                'color: {}; font-weight: 600;'.format('#1b8a3a' if success else '#b42318')
            )
            self._log('{}: {}'.format(label, 'passed' if success else 'failed'))
            if output:
                for line in output.splitlines():
                    self._log(line)
            process.deleteLater()

        process.finished.connect(finish)
        process.start('/bin/sh', ['-lc', command])

    def _drain_process_output(self):
        if self._process is None:
            return
        text = bytes(self._process.readAllStandardOutput()).decode('utf-8', 'replace').strip()
        if text:
            for line in text.splitlines():
                self._log(line)

    def _on_process_finished(self, exit_code, _exit_status):
        if self._process is None:
            return
        self._drain_process_output()
        target = self._process_target or 'Command'
        if exit_code == 0:
            self._log('{} finished successfully.'.format(target))
        else:
            self._log('{} failed with exit code {}.'.format(target, exit_code))
        self._process.deleteLater()
        self._process = None
        self._process_target = None

    def _load_runtime_settings(self):
        settings = QtCore.QSettings()
        return {
            'mode': settings.value('{}/mode'.format(self.RUNTIME_PREFIX), 'python'),
            'python_path': settings.value('{}/python_path'.format(self.RUNTIME_PREFIX), self._default_python_executable()),
            'conda_env': settings.value('{}/conda_env'.format(self.RUNTIME_PREFIX), 'landlensdb_env'),
            'docker_container': settings.value('{}/docker_container'.format(self.RUNTIME_PREFIX), 'landlensdb-local'),
        }

    def _save_runtime_settings(self):
        if hasattr(self, 'runtime_python_path_input'):
            self.runtime_values['python_path'] = self._normalize_python_executable(
                self.runtime_python_path_input.text().strip() or self._default_python_executable()
            )
        if hasattr(self, 'runtime_conda_name_input'):
            self.runtime_values['conda_env'] = self.runtime_conda_name_input.text().strip() or 'landlensdb_env'
        if hasattr(self, 'runtime_docker_name_input'):
            self.runtime_values['docker_container'] = self.runtime_docker_name_input.text().strip() or 'landlensdb-local'

        settings = QtCore.QSettings()
        for key, value in self.runtime_values.items():
            settings.setValue('{}/{}'.format(self.RUNTIME_PREFIX, key), value)

    def _reset_dependency_defaults(self):
        self.python_path_input.setText(self._default_python_executable())
        self.conda_commands_edit.setPlainText(self._default_conda_commands())
        self.docker_commands_edit.setPlainText(self._default_docker_commands())
        self.pip_radio.setChecked(True)
        self._log('Python dependency fields reset to defaults.')

    def _reset_server_defaults(self):
        self.server_installer_command_input.setText('https://www.postgresql.org/download/')
        self.server_homebrew_command_input.setText(self._homebrew_server_command())
        self.server_windows_command_input.setText('winget install PostgreSQL.PostgreSQL')
        self.server_conda_command_input.setText(self._conda_server_command())
        self.server_homebrew_radio.setChecked(True)
        self._log('PostgreSQL setup fields reset to defaults.')

    def _reset_runtime_defaults(self):
        self.runtime_values = {
            'mode': 'python',
            'python_path': self._default_python_executable(),
            'conda_env': 'landlensdb_env',
            'docker_container': 'landlensdb-local',
        }
        self.runtime_python_radio.setChecked(True)
        self.runtime_python_path_input.setText(self._default_python_executable())
        self.runtime_conda_name_input.setText('landlensdb_env')
        self.runtime_docker_name_input.setText('landlensdb-local')
        self._save_runtime_settings()
        self.runtime_test_feedback.setText('')
        self._log('Python environment fields reset to defaults.')

    def _reset_connection_defaults(self):
        self.connection_values = {
            'name': '',
            'service': '',
            'host': 'localhost',
            'port': '5432',
            'database': 'landlensdb',
            'schema': 'public',
        }
        save_connection_settings(self.connection_values)
        self._refresh_connection_summary()
        self.connection_feedback.setText('')
        self.connectionSaved.emit(dict(self.connection_values))
        self._log('PostgreSQL connection fields reset to defaults.')

    def _default_conda_commands(self):
        return '\n'.join([
            'conda create -n landlensdb_env -c conda-forge "gdal>=3.5"',
            'conda activate landlensdb_env',
            'pip install landlensdb',
        ])

    def _default_docker_commands(self):
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        return '\n'.join([
            'docker build -t landlensdb-local -f "{}" "{}"'.format(
                os.path.join(root_dir, 'Dockerfile'),
                root_dir,
            ),
            'docker run --rm landlensdb-local python -m pip install landlensdb',
        ])

    def _homebrew_server_command(self):
        return (
            "brew install postgresql postgis && "
            "export POSTGIS_GDAL_ENABLED_DRIVERS=ENABLE_ALL && "
            "psql -d landlens_test -c \"ALTER DATABASE landlens_test SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';\""
        )

    def _conda_server_command(self):
        return (
            "conda create -n landlensdb_pg -c conda-forge \"postgresql>=14\" \"postgis>=3.5\" && "
            "export POSTGIS_GDAL_ENABLED_DRIVERS=ENABLE_ALL && "
            "psql -d landlens_test -c \"ALTER DATABASE landlens_test SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';\""
        )

    def _log(self, message):
        timestamp = QtCore.QDateTime.currentDateTime().toString('HH:mm:ss')
        self.setup_log.appendPlainText('[{}] {}'.format(timestamp, message))

    def _default_python_executable(self):
        return self._normalize_python_executable(sys.executable)

    def _normalize_python_executable(self, path):
        cleaned = (path or '').strip()
        if not cleaned:
            return sys.executable
        basename = os.path.basename(cleaned).lower()
        directory = os.path.dirname(cleaned)
        if basename in ('qgis', 'qgis-bin', 'qgis-ltr'):
            sibling_python = os.path.join(directory, 'python')
            if os.path.exists(sibling_python):
                return sibling_python
        if cleaned.endswith('.app'):
            app_python = os.path.join(cleaned, 'Contents', 'MacOS', 'python')
            if os.path.exists(app_python):
                return app_python
        return cleaned
