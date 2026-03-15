from qgis.PyQt import QtWidgets


class ConnectionDialog(QtWidgets.QDialog):
    def __init__(self, values, test_callback, parent=None):
        super(ConnectionDialog, self).__init__(parent)
        self._test_callback = test_callback
        self.setWindowTitle('Connection')
        self.resize(520, 220)

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QGridLayout()
        layout.addLayout(form)

        self.name_input = QtWidgets.QLineEdit(values.get('name', ''))
        self.service_input = QtWidgets.QLineEdit(values.get('service', ''))
        self.host_input = QtWidgets.QLineEdit(values.get('host', 'localhost'))
        self.port_input = QtWidgets.QLineEdit(values.get('port', '5432'))
        self.database_input = QtWidgets.QLineEdit(values.get('database', 'landlensdb'))
        self.schema_input = QtWidgets.QLineEdit(values.get('schema', 'public'))
        self.feedback_label = QtWidgets.QLabel('')

        controls = [
            ('Name', self.name_input),
            ('Service', self.service_input),
            ('Host', self.host_input),
            ('Port', self.port_input),
            ('Database', self.database_input),
            ('Schema', self.schema_input),
        ]
        for row, (label_text, widget) in enumerate(controls):
            form.addWidget(QtWidgets.QLabel(label_text), row, 0)
            form.addWidget(widget, row, 1)

        buttons = QtWidgets.QHBoxLayout()
        layout.addLayout(buttons)
        self.test_button = QtWidgets.QPushButton('Test Connection')
        self.save_button = QtWidgets.QPushButton('Save')
        self.cancel_button = QtWidgets.QPushButton('Cancel')
        buttons.addWidget(self.test_button)
        buttons.addWidget(self.feedback_label)
        buttons.addStretch()
        buttons.addWidget(self.save_button)
        buttons.addWidget(self.cancel_button)

        self.test_button.clicked.connect(self.test_connection)
        self.save_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)

    def values(self):
        return {
            'name': self.name_input.text().strip(),
            'service': self.service_input.text().strip(),
            'host': self.host_input.text().strip(),
            'port': self.port_input.text().strip() or '5432',
            'database': self.database_input.text().strip(),
            'schema': self.schema_input.text().strip() or 'public',
        }

    def test_connection(self):
        success, message = self._test_callback(self.values())
        color = '#1b8a3a' if success else '#b42318'
        self.feedback_label.setText(message)
        self.feedback_label.setStyleSheet('color: {}; font-weight: 600;'.format(color))
