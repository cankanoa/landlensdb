"""Small syntax-highlighted YAML editor for import parameters."""

from qgis.PyQt import QtCore, QtGui, QtWidgets


class YamlHighlighter(QtGui.QSyntaxHighlighter):
    """Highlight the YAML constructs used by import parameter files."""

    def __init__(self, document):
        super(YamlHighlighter, self).__init__(document)
        self.rules = []
        self._add_rule(r"^\s*[A-Za-z_][\w-]*(?=\s*:)", "#2563eb", bold=True)
        self._add_rule(r"(['\"])(?:\\.|(?!\1).)*\1", "#15803d")
        self._add_rule(r"\b(?:true|false|null)\b", "#7c3aed", bold=True)
        self._add_rule(r"\b[-+]?(?:\d+\.?\d*|\.\d+)\b", "#b45309")
        self._add_rule(r"#.*$", "#6b7280", italic=True)

    def _add_rule(self, pattern, color, bold=False, italic=False):
        expression = QtCore.QRegularExpression(pattern)
        text_format = QtGui.QTextCharFormat()
        text_format.setForeground(QtGui.QColor(color))
        if bold:
            text_format.setFontWeight(QtGui.QFont.Bold)
        text_format.setFontItalic(italic)
        self.rules.append((expression, text_format))

    def highlightBlock(self, text):
        for expression, text_format in self.rules:
            iterator = expression.globalMatch(text)
            while iterator.hasNext():
                match = iterator.next()
                self.setFormat(
                    match.capturedStart(),
                    match.capturedLength(),
                    text_format,
                )


class ImportYamlDialog(QtWidgets.QDialog):
    """Edit, validate, and save one import configuration."""

    def __init__(self, yaml_text, normalizer, presets=None, parent=None):
        super(ImportYamlDialog, self).__init__(parent)
        self.normalizer = normalizer
        self.presets = dict(presets or {})
        self.setWindowTitle("Import Parameters")
        self.resize(880, 720)

        layout = QtWidgets.QVBoxLayout(self)
        preset_row = QtWidgets.QHBoxLayout()
        preset_row.addWidget(QtWidgets.QLabel("Quick preset", self))
        self.preset_input = QtWidgets.QComboBox(self)
        self.preset_input.addItems(list(self.presets))
        self.apply_preset_button = QtWidgets.QPushButton("Load Preset", self)
        self.apply_preset_button.setEnabled(bool(self.presets))
        preset_row.addWidget(self.preset_input, 1)
        preset_row.addWidget(self.apply_preset_button)
        layout.addLayout(preset_row)

        self.editor = QtWidgets.QPlainTextEdit(self)
        self.editor.setPlainText(yaml_text)
        self.editor.setLineWrapMode(QtWidgets.QPlainTextEdit.NoWrap)
        font = QtGui.QFontDatabase.systemFont(QtGui.QFontDatabase.FixedFont)
        self.editor.setFont(font)
        self.highlighter = YamlHighlighter(self.editor.document())
        layout.addWidget(self.editor, 1)

        self.validation_label = QtWidgets.QLabel(self)
        self.validation_label.setWordWrap(True)
        layout.addWidget(self.validation_label)

        buttons = QtWidgets.QHBoxLayout()
        self.close_button = QtWidgets.QPushButton("Close", self)
        self.copy_button = QtWidgets.QPushButton("Copy", self)
        self.save_button = QtWidgets.QPushButton("Save", self)
        self.save_button.setDefault(True)
        buttons.addStretch()
        buttons.addWidget(self.close_button)
        buttons.addWidget(self.copy_button)
        buttons.addWidget(self.save_button)
        layout.addLayout(buttons)

        self.apply_preset_button.clicked.connect(self.load_selected_preset)
        self.close_button.clicked.connect(self.reject)
        self.copy_button.clicked.connect(self.copy_yaml)
        self.save_button.clicked.connect(self.validate_and_accept)

    def yaml_text(self):
        return self.editor.toPlainText()

    def load_selected_preset(self):
        name = self.preset_input.currentText()
        if name in self.presets:
            self.editor.setPlainText(self.presets[name])
            self.validation_label.clear()

    def copy_yaml(self):
        """Copy the complete YAML document to the system clipboard."""
        QtWidgets.QApplication.clipboard().setText(self.yaml_text())
        self.validation_label.setStyleSheet("color: #15803d;")
        self.validation_label.setText("Import parameter YAML copied.")

    def validate_editor(self):
        try:
            self.normalizer(self.yaml_text())
        except Exception as exc:
            self._show_error(str(exc))
            return False
        self.validation_label.setStyleSheet("color: #15803d;")
        self.validation_label.setText("Valid import parameter YAML.")
        return True

    def validate_and_accept(self):
        if self.validate_editor():
            self.accept()

    def _show_error(self, message):
        self.validation_label.setStyleSheet("color: #b91c1c;")
        self.validation_label.setText(message)
