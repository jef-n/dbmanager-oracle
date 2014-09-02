# TODO


## Table Menu

* Import Table is not working (problem in QgsVectorLayerImport).


# DONE

## General 

* Use the internal QGis SQLite table list cache.
* Add table and column names in sqldictionnary/
* Modify Copyrights.
* Add Fields comment.
* Remove code on database rules (do not exists in Oracle)/
* Show Triggers.
* Fix schema listing made 3 times instead of one !
* Transform all requests from connector.py to Oracle equivalent.
* better handling Schema Privileges.
* Remove schema renaming (unsupported in Oracle).
* Fix nullable Fields not shown correctly.
* Delete all of the vacuum operations as Oracle Databases don't have such mechanism.
* Fix manual row count not launching.
* Add table comment.
* Handle estimated lines (NUM_ROWS in ALL_ALL_TABLES).
* Add geometryless tables management.
* Handle primary key information.
* Show constraints columns.
* Delete the raster code.
* Show SRID definition.
* Show Index columns.
* Handle Spatial extents.
* Retrieve the type of tables (multiple queries, one by table).
* Try to implement a whole scan (Schemas and Tables) at Connexion activation: Impossible due to Oracle model.
* Use connection UseMetadataTable parameter to list tables.
* Use other connection parameters to list tables.
* List the Oracle connections.
* Just install cx_Orace.pyd in C:\Program Files\QGIS Chugiak\apps\Python27\DLLs
* Found a way to make the plugin verbose:
  from qgis.core import *
  QgsMessageLog("message","DBManager", QgsMessageLog.INFO)
* Show Schemas list.
* Get minimum privileges management.
* Make a true SQL dictionnary.
* Show the tree of vector tables.

## SQL Query Dialog

* Show better statistic about query duration.
* handle DatabaseError in the dialog.
* Make the SQL query window works !
* Handle geometry type detection.

## Table Menu

* Add an action to update metadata layer extent to the content of the geocolumn.
* Disable move Table to another schema (impossible in Oracle, you have to import/export).
* Find how to load Unknown WKBGeometryType layers.
* Spatial index creation.
* Edit dialog fully functionnal.
* See why there is so much constraints when creating a new table (normal: Oracle interns).
* Edit Table opens.
* Remove Vacuum operation in Table menu.
* Fix: Add SRID when creating table.
* Rename Geographic Table.
* Can create non geographic tables.
* Can delete non geographic tables.
* Can Create geographic tables.
* Can Delete geographic tables.
* DO Empty table.