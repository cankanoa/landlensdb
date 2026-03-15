# -*- coding: utf-8 -*-

import os
import shlex
import sys

from qgis.PyQt import QtCore, QtGui, QtWidgets

try:
    from qgis.gui import QgsCodeEditorShell
except ImportError:  # pragma: no cover
    QgsCodeEditorShell = None

from ..shared.connection_utils import load_connection_settings, save_connection_settings, test_connection_values


class SetupTab(QtWidgets.QWidget):
    connectionSaved = QtCore.pyqtSignal(dict)

    def __init__(self, iface, parent=None):
        super(SetupTab, self).__init__(parent)
        self.iface = iface
        self.connection_values = load_connection_settings()
        self._process = None
        self._process_target = None

        self._build_ui()
        self._wire_signals()
        self._apply_server_mode()
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
        dependencies_layout.addWidget(self._build_pip_panel())
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

        self.python_runtime_label = QtWidgets.QLabel()
        self.python_runtime_label.setWordWrap(True)
        layout.addWidget(self.python_runtime_label)

        command_row = QtWidgets.QHBoxLayout()
        self.pip_command_preview = QtWidgets.QLineEdit()
        self.pip_command_preview.setReadOnly(True)
        command_row.addWidget(self.pip_command_preview, 1)
        self.pip_run_button = QtWidgets.QPushButton('Run')
        command_row.addWidget(self.pip_run_button)
        layout.addLayout(command_row)

        self._update_pip_preview()
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
        self.reset_connection_button.clicked.connect(self._reset_connection_defaults)

        self.pip_run_button.clicked.connect(self._run_pip_preview)

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

        self.connection_name_input.textChanged.connect(self._store_connection_form)
        self.connection_service_input.textChanged.connect(self._store_connection_form)
        self.connection_host_input.textChanged.connect(self._store_connection_form)
        self.connection_port_input.textChanged.connect(self._store_connection_form)
        self.connection_database_input.textChanged.connect(self._store_connection_form)
        self.connection_schema_input.textChanged.connect(self._store_connection_form)
        self.connection_test_button.clicked.connect(self._test_connection_form)
        self.run_manual_button.clicked.connect(self._run_manual_command)

        self.server_installer_radio.toggled.connect(self._apply_server_mode)
        self.server_homebrew_radio.toggled.connect(self._apply_server_mode)
        self.server_windows_radio.toggled.connect(self._apply_server_mode)
        self.server_conda_radio.toggled.connect(self._apply_server_mode)

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

    def _shrink_stack(self, stack):
        current = stack.currentWidget()
        if current is None:
            return
        height = current.sizeHint().height()
        stack.setMinimumHeight(height)
        stack.setMaximumHeight(height)

    def _update_pip_preview(self):
        python_path = self._default_python_executable()
        self.python_runtime_label.setText('QGIS Python interpreter: {}'.format(python_path))
        dependencies = self._requirements_dependencies()
        install_command = '"{}" -m pip install --upgrade pip'.format(python_path)
        if dependencies:
            install_command = '{} && "{}" -m pip install {}'.format(
                install_command,
                python_path,
                ' '.join(shlex.quote(dep) for dep in dependencies),
            )
        preview_text = install_command.replace(' && ', '\n')
        if hasattr(self.pip_command_preview, 'setPlainText'):
            self.pip_command_preview.setPlainText(preview_text)
        else:
            self.pip_command_preview.setText(preview_text.replace('\n', ' && '))

    def _run_pip_preview(self):
        if hasattr(self.pip_command_preview, 'toPlainText'):
            command = self.pip_command_preview.toPlainText().strip().replace('\n', ' && ')
        else:
            command = self.pip_command_preview.text().strip()
        self._run_single_command(command, 'Python executable dependencies')

    def _requirements_dependencies(self):
        requirements_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'requirements.txt')
        )
        if not os.path.exists(requirements_path):
            return []

        with open(requirements_path, 'r', encoding='utf-8') as handle:
            return [line.strip() for line in handle if line.strip()]

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

    def _reset_dependency_defaults(self):
        self._update_pip_preview()
        self._log('Python dependency fields reset to defaults.')

    def _reset_server_defaults(self):
        self.server_installer_command_input.setText('https://www.postgresql.org/download/')
        self.server_homebrew_command_input.setText(self._homebrew_server_command())
        self.server_windows_command_input.setText('winget install PostgreSQL.PostgreSQL')
        self.server_conda_command_input.setText(self._conda_server_command())
        self.server_homebrew_radio.setChecked(True)
        self._log('PostgreSQL setup fields reset to defaults.')

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
