# QGIS
qgis-build:
#	@cp dist/*.whl qgis_plugin_landlensdb
	PYTHONPATH=. $(BUILD_PLUGIN)
	@echo "Generating plugin requirements.txt..."
	python qgis_plugin_landlensdb/build_plugin.py
	@echo "Copying landlensdb into plugin folder..."
	cp -R landlensdb qgis_plugin_landlensdb/
	@echo "Copying plugin icon into plugin folder..."
	cp docs/images/landlensdb.png qgis_plugin_landlensdb/landlensdb.png
	@echo "Removing __pycache__..."
	rm -rf qgis_plugin_landlensdb/__pycache__ qgis_plugin_landlensdb/test/__pycache__
	@echo "Creating plugin zip..."
	zip -r qgis_plugin_landlensdb.zip qgis_plugin_landlensdb/ \
	  -x "*.DS_Store" "*__MACOSX*"
	@echo "Removing copied landlensdb folder from plugin workspace..."
	rm -rf qgis_plugin_landlensdb/landlensdb
