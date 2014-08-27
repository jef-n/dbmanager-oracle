# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                 : DB Manager
Description          : Database manager plugin for QGIS (Oracle)
Date                 : Aug 27, 2014
copyright            : (C) 2014 by Médéric RIBREUX
email                : mederic.ribreux@gmail.com

The content of this file is based on
- PG_Manager by Martin Dobias <wonder.sk@gmail.com> (GPLv2 license)
- DB Manager by Giuseppe Sucameli <brush.tyler@gmail.com> (GPLv2 license)
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

# this will disable the dbplugin if the connector raise an ImportError
from .connector import OracleDBConnector

from PyQt4.QtCore import *
from PyQt4.QtGui import *

from ..plugin import ConnectionError, InvalidDataException, DBPlugin, Database, Schema, Table, VectorTable, TableField, TableConstraint, TableIndex, TableTrigger, TableRule

try:
	from . import resources_rc
except ImportError:
	pass

from ..html_elems import HtmlParagraph, HtmlList, HtmlTable

from qgis.core import QgsCredentials


def classFactory():
	return OracleDBPlugin

class OracleDBPlugin(DBPlugin):

	@classmethod
	def icon(self):
		return QIcon(":/db_manager/oracle/icon")

	@classmethod
	def typeName(self):
		return 'oracle'

	@classmethod
	def typeNameString(self):
		return 'Oracle'

	@classmethod
	def providerName(self):
		return 'oracle'

	@classmethod
	def connectionSettingsKey(self):
		return '/Oracle/connections'

	def connectToUri(self, uri):
		self.db = self.databasesFactory( self, uri)
		if self.db:
			return True
		return False

	def databasesFactory(self, connection, uri):
		return ORDatabase(connection, uri)

	def connect(self, parent=None):
		conn_name = self.connectionName()
		settings = QSettings()
		settings.beginGroup( u"/%s/%s" % (self.connectionSettingsKey(), conn_name) )

		if not settings.contains( "database" ): # non-existent entry?
			raise InvalidDataException( self.tr('There is no defined database connection "%s".') % conn_name )

		from qgis.core import QgsDataSourceURI
		uri = QgsDataSourceURI()

		settingsList = ["host", "port", "database", "username", "password"]
		host, port, database, username, password = map(lambda x: settings.value(x, "", type=str), settingsList)

		# qgis1.5 use 'savePassword' instead of 'save' setting
		savedPassword = settings.value("save", False, type=bool) or settings.value("savePassword", False, type=bool)

                # get all of the connexion options

		useEstimatedMetadata = settings.value("estimatedMetadata", False, type=bool)
                uri.setParam('userTablesOnly', str(settings.value("userTablesOnly", False, type=bool)))
                uri.setParam('geometryColumnsOnly', str(settings.value("geometryColumnsOnly", False, type=bool)))
                uri.setParam('allowGeometrylessTables', str(settings.value("allowGeometrylessTables", False, type=bool)))
                uri.setParam('onlyExistingTypes', str(settings.value("onlyExistingTypes", False, type=bool)))
                
		settings.endGroup()

                uri.setConnection(host, port, database, username, password)

		uri.setUseEstimatedMetadata(useEstimatedMetadata)

		err = u""
		try:
			return self.connectToUri(uri)
		except ConnectionError, e:
			err = str(e)

		# ask for valid credentials
		max_attempts = 3
		for i in range(max_attempts):
			(ok, username, password) = QgsCredentials.instance().get(uri.connectionInfo(), username, password, err)

			if not ok:
				return False

			uri.setConnection(host, port, database, username, password)

			try:
				self.connectToUri(uri)
			except ConnectionError, e:
				if i == max_attempts-1:	# failed the last attempt
					raise e
				err = str(e)
				continue

			QgsCredentials.instance().put(uri.connectionInfo(), username, password)

			return True

		return False


class ORDatabase(Database):
	def __init__(self, connection, uri):
		Database.__init__(self, connection, uri)
                self.schema_lst = []

	def connectorsFactory(self, uri):
		return OracleDBConnector(uri)

	def dataTablesFactory(self, row, db, schema=None):
		return ORTable(row, db, schema)

	def vectorTablesFactory(self, row, db, schema=None):
		return ORVectorTable(row, db, schema)

        def info(self):
                from .info_model import ORDatabaseInfo
                return ORDatabaseInfo(self)

	def schemasFactory(self, row, db):
		return ORSchema(row, db)
        
	def sqlResultModel(self, sql, parent):
		from .data_model import ORSqlResultModel
		return ORSqlResultModel(self, sql, parent)

	def toSqlLayer(self, sql, geomCol, uniqueCol, layerName="QueryLayer", layerType=None, avoidSelectById=False):
		from qgis.core import QgsMapLayer, QgsVectorLayer
		uri = self.uri()

		uri.setDataSource("", u"(%s\n)" % sql, geomCol, "", uniqueCol)
		if avoidSelectById:
			uri.disableSelectAtId( True )
		provider = self.dbplugin().providerName()
                vlayer = QgsVectorLayer(uri.uri(), layerName, provider)

                # handling undetermined geometry type
                if not vlayer.isValid():
                        con = self.database().connector
                        wkbType = con.getTableGeomType(u"(%s)" % sql, geomCol)
                        uri.setWkbType(wkbType)
                        vlayer = QgsVectorLayer(uri.uri(), layerName, provider)
                        
		return vlayer

	def registerDatabaseActions(self, mainWindow):
		action = QAction(QApplication.translate("DBManagerPlugin", "&Re-connect"), self)
		mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Database"), self.reconnectActionSlot )

		if self.schemas() != None:
			action = QAction(QApplication.translate("DBManagerPlugin", "&Create schema"), self)
			mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Schema"), self.createSchemaActionSlot )
			action = QAction(QApplication.translate("DBManagerPlugin", "&Delete (empty) schema"), self)
			mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Schema"), self.deleteSchemaActionSlot )

		action = QAction(QApplication.translate("DBManagerPlugin", "Delete selected item"), self)
		mainWindow.registerAction( action, None, self.deleteActionSlot )
		action.setShortcuts(QKeySequence.Delete)

		action = QAction(QIcon(":/db_manager/actions/create_table"), QApplication.translate("DBManagerPlugin", "&Create table"), self)
		mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Table"), self.createTableActionSlot )
		action = QAction(QIcon(":/db_manager/actions/edit_table"), QApplication.translate("DBManagerPlugin", "&Edit table"), self)
		mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Table"), self.editTableActionSlot )
		action = QAction(QIcon(":/db_manager/actions/del_table"), QApplication.translate("DBManagerPlugin", "&Delete table/view"), self)
		mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Table"), self.deleteTableActionSlot )
		action = QAction(QApplication.translate("DBManagerPlugin", "&Empty table"), self)
		mainWindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Table"), self.emptyTableActionSlot )


	def schemas(self):
                """ make a sort of cache for schema listing to improve performances """
                if len(self.schema_lst) == 0:
                        schemas = self.connector.getSchemas()
                        self.schema_lst =  map(lambda x: self.schemasFactory(x, self), schemas)

		return self.schema_lst


class ORSchema(Schema):
	def __init__(self, row, db):
		Schema.__init__(self, db)
		#self.oid, self.name, self.owner, self.perms, self.comment = row
                self.name = row[0]


class ORTable(Table):
	def __init__(self, row, db, schema=None):
		Table.__init__(self, db, schema)
		self.name, schema_name, self.isView, self.owner, self.estimatedRowCount, self.comment = row
                if self.isView == 'VIEW':
                        self.isView = True
                else:
                        self.isView = False
                if not self.estimatedRowCount:
                        self.estimatedRowCount = 0
		self.estimatedRowCount = int(self.estimatedRowCount)

	def runAction(self, action):
		action = unicode(action)

		if action.startswith( "rows/" ):
			if action == "rows/recount":
				self.refreshRowCount()
				return True

		return Table.runAction(self, action)

	def tableFieldsFactory(self, row, table):
		return ORTableField(row, table)

	def tableConstraintsFactory(self, row, table):
		return ORTableConstraint(row, table)

	def tableIndexesFactory(self, row, table):
		return ORTableIndex(row, table)

	def tableTriggersFactory(self, row, table):
		return ORTableTrigger(row, table)

	def tableRulesFactory(self, row, table):
		return ORTableRule(row, table)

	def info(self):
		from .info_model import ORTableInfo
		return ORTableInfo(self)

	def tableDataModel(self, parent):
		from .data_model import ORTableDataModel
		return ORTableDataModel(self, parent)

        def uri(self):
		uri = self.database().uri()
		schema = self.schemaName() if self.schemaName() else ''
		geomCol = self.geomColumn if self.type in [Table.VectorType, Table.RasterType] else ""
		uniqueCol = self.getValidQGisUniqueFields(True) if self.isView else None
		uri.setDataSource(schema, self.name, geomCol if geomCol else None, None, uniqueCol.name if uniqueCol else "" )
                if geomCol:
                        wkbType = self.database().connector.getTableGeomType(self.name, geomCol)
                        uri.setWkbType(wkbType)

		return uri



class ORVectorTable(ORTable, VectorTable):
	def __init__(self, row, db, schema=None):
		ORTable.__init__(self, row[:-4], db, schema)
		VectorTable.__init__(self, db, schema)
		self.geomColumn, self.geomType, self.geomDim, self.srid = row[-4:]

	def info(self):
		from .info_model import ORVectorTableInfo
		return ORVectorTableInfo(self)

	def runAction(self, action):
		if action.startswith( "extent/" ):
			if action == "extent/update":
                                self.updateExtent()
				return True

		if ORTable.runAction(self, action):
			return True
		return VectorTable.runAction(self, action)

        def updateExtent(self):
		self.database().connector.updateExtentMetadata( (self.schemaName(), self.name), self.geomColumn )
                self.refreshTableEstimatedExtent()
                self.refresh()

	def hasSpatialIndex(self, geom_column=None):
		geom_column = geom_column if geom_column != None else self.geomColumn

		for idx in self.indexes():
			if geom_column == idx.column:
				return True
		return False


class ORTableField(TableField):
	def __init__(self, row, table):
                """ build fields information from query and find primary key """
		TableField.__init__(self, table)
		self.num, self.name, self.dataType, self.charMaxLen, self.modifier, self.notNull, self.hasDefault, self.default, typeStr, self.comment = row
		self.primaryKey = False
                if self.notNull.upper() == u"Y":
                        self.notNull = False
                else:
                        self.notNull = True

		# get modifier (e.g. "precision,scale") from formatted type string
		trimmedTypeStr = typeStr.strip()
		regex = QRegExp( "\((.+)\)$" )
		startpos = regex.indexIn( trimmedTypeStr )
		if startpos >= 0:
			self.modifier = regex.cap(1).strip()
		else:
			self.modifier = None

		# find out whether fields are part of primary key
		for con in self.table().constraints():
			if con.type == TableConstraint.TypePrimaryKey and self.name == con.column:
				self.primaryKey = True
				break


class ORTableConstraint(TableConstraint):
	def __init__(self, row, table):
                """ build constraints info from query """
		TableConstraint.__init__(self, table)
		self.name, constr_type_str, self.isDefferable, self.isDeffered, self.column = row[:5]
                constr_type_str = constr_type_str.lower()

		if constr_type_str in TableConstraint.types:
			self.type = TableConstraint.types[constr_type_str]
		else:
			self.type = TableConstraint.TypeUnknown

		if self.type == TableConstraint.TypeCheck:
			self.checkSource = row[5]
		elif self.type == TableConstraint.TypeForeignKey:
			self.foreignTable = row[6]
			self.foreignOnUpdate = TableConstraint.onAction[row[7]]
			self.foreignOnDelete = TableConstraint.onAction[row[8]]
			self.foreignMatchType = TableConstraint.matchTypes[row[9]]
			self.foreignKeys = row[10]

        def fields(self):
                """ Hack to make edit dialog box work """
                fields = self.table().fields()
                field = None
                for fld in fields:
                        if fld.name == self.column:
                                field = fld
                cols = {}
                cols[0] = field
                
                return cols


class ORTableIndex(TableIndex):
	def __init__(self, row, table):
		TableIndex.__init__(self, table)
		self.name, self.column, self.isUnique = row
		#self.columns = map(int, columns.split(' '))

        def fields(self):
                """ Hack to make edit dialog box work """
                fields = self.table().fields()
                field = None
                for fld in fields:
                        if fld.name == self.column:
                                field = fld
                cols = {}
                cols[0] = field
                
                return cols


class ORTableTrigger(TableTrigger):
	def __init__(self, row, table):
		TableTrigger.__init__(self, table)
		self.name, self.event, self.type, self.enabled = row



