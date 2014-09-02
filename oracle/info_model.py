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

from PyQt4.QtCore import *
from PyQt4.QtGui import *

from ..info_model import TableInfo, VectorTableInfo, DatabaseInfo, SchemaInfo
from ..html_elems import HtmlContent, HtmlSection, HtmlParagraph, HtmlList, HtmlTable, HtmlTableHeader, HtmlTableCol

class ORDatabaseInfo(DatabaseInfo):
	def __init__(self, db):
		self.db = db

	def connectionDetails(self):
		tbl = [
			(QApplication.translate("DBManagerPlugin", "Host:"), self.db.connector.host),
			(QApplication.translate("DBManagerPlugin", "User:"), self.db.connector.user),
                        (QApplication.translate("DBManagerPlugin", "SQLite list tables cache:"), "Enabled" if self.db.connector.hasCache else "Unavailable")
                ]

		return HtmlTable( tbl )

	def spatialInfo(self):
		ret = []

		info = self.db.connector.getSpatialInfo()
		if info == None:
			return

		tbl = [
			(QApplication.translate("DBManagerPlugin", "Oracle Spatial:"), info[0])
		]
		ret.append( HtmlTable( tbl ) )

		if not self.db.connector.has_geometry_columns:
			ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", "<warning> ALL_SDO_GEOM_METADATA view doesn't exist!\n"
				"This view is essential for many GIS applications for enumeration of tables." ) ) )

		return ret

        def privilegesDetails(self):
                """ find if user can create schemas (CREATE ANY TABLE or something)"""
                # TODO
                return None

class ORTableInfo(TableInfo):
	def __init__(self, table):
		self.table = table


	def generalInfo(self):
		ret = []

		# if the estimation is less than 100 rows, try to count them - it shouldn't take long time
		if self.table.rowCount == None and self.table.estimatedRowCount < 100:
			# row count information is not displayed yet, so just block
			# table signals to avoid double refreshing (infoViewer->refreshRowCount->tableChanged->infoViewer)
			self.table.blockSignals(True)
			self.table.refreshRowCount()
			self.table.blockSignals(False)

		tbl = [
			(QApplication.translate("DBManagerPlugin", "Relation type:"), QApplication.translate("DBManagerPlugin", "View") if self.table.isView else QApplication.translate("DBManagerPlugin", "Table")),
			(QApplication.translate("DBManagerPlugin", "Owner:"), self.table.owner)
		]
		if self.table.comment:
			tbl.append( (QApplication.translate("DBManagerPlugin", "Comment:"), self.table.comment) )

		tbl.extend([
			(QApplication.translate("DBManagerPlugin", "Rows (estimation):"), self.table.estimatedRowCount )
		])

		# privileges
		# has the user access to this schema?
		schema_priv = self.table.database().connector.getSchemaPrivileges(self.table.schemaName()) if self.table.schema() else None
		if schema_priv == None:
			pass
		elif schema_priv[1] == False:	# no usage privileges on the schema
			tbl.append( (QApplication.translate("DBManagerPlugin", "Privileges:"), QApplication.translate("DBManagerPlugin", "<warning> This user doesn't have usage privileges for this schema!") ) )
		else:
			table_priv = self.table.database().connector.getTablePrivileges( (self.table.schemaName(), self.table.name) )
			privileges = []
			if table_priv[0]:
				privileges.append("select")

				if self.table.rowCount == None or self.table.rowCount >= 0:
					tbl.append( (QApplication.translate("DBManagerPlugin", "Rows (counted):"), self.table.rowCount if self.table.rowCount != None else QApplication.translate("DBManagerPlugin", 'Unknown (<a href="action:rows/recount">find out</a>)')) )

			if table_priv[1]: privileges.append("insert")
			if table_priv[2]: privileges.append("update")
			if table_priv[3]: privileges.append("delete")
			priv_string = u", ".join(privileges) if len(privileges) > 0 else QApplication.translate("DBManagerPlugin", '<warning> This user has no privileges!')
			tbl.append( (QApplication.translate("DBManagerPlugin", "Privileges:"), priv_string ) )

		ret.append( HtmlTable( tbl ) )

		if schema_priv != None and schema_priv[1]:
			if table_priv[0] and not table_priv[1] and not table_priv[2] and not table_priv[3]:
				ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", "<warning> This user has read-only privileges.") ) )

		# primary key defined?
		if not self.table.isView:
			if len( filter(lambda fld: fld.primaryKey, self.table.fields()) ) <= 0:
				ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", "<warning> No primary key defined for this table!") ) )

		return ret

	def getSpatialInfo(self):
		ret = []

		info = self.db.connector.getSpatialInfo()
		if info == None:
			return

		tbl = [
			(QApplication.translate("DBManagerPlugin", "Library:"), info[0])#,
		]
		ret.append( HtmlTable( tbl ) )

		if not self.db.connector.has_geometry_columns:
			ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", "<warning> ALL_SDO_GEOM_METADATA table doesn't exist!\n"
				"This table is essential for many GIS applications for enumeration of tables.") ) )

		return ret


	def fieldsDetails(self):
		tbl = []

		# define the table header
		header = ( "#", QApplication.translate("DBManagerPlugin", "Name"), QApplication.translate("DBManagerPlugin", "Type"), QApplication.translate("DBManagerPlugin", "Length"), QApplication.translate("DBManagerPlugin", "Null"), QApplication.translate("DBManagerPlugin", "Default"), QApplication.translate("DBManagerPlugin", "Comment") )
		tbl.append( HtmlTableHeader( header ) )

		# add table contents
		for fld in self.table.fields():
			char_max_len = fld.charMaxLen if fld.charMaxLen != None and fld.charMaxLen != -1 else ""
			is_null_txt = "N" if fld.notNull else "Y"

			# make primary key field underlined
			attrs = {"class":"underline"} if fld.primaryKey else None
			name = HtmlTableCol( fld.name, attrs )

			tbl.append( (fld.num, name, fld.type2String(), char_max_len, is_null_txt, fld.default2String(), fld.comment) )

		return HtmlTable( tbl, {"class":"header"} )


        def constraintsDetails(self):
		if self.table.constraints() == None or len(self.table.constraints()) <= 0:
			return None

		tbl = []

		# define the table header
		header = ( QApplication.translate("DBManagerPlugin", "Name"), QApplication.translate("DBManagerPlugin", "Type"), QApplication.translate("DBManagerPlugin", "Column(s)") )
		tbl.append( HtmlTableHeader( header ) )

		# add table contents
		for con in self.table.constraints():
			tbl.append( (con.name, con.type2String(), con.column))

		return HtmlTable( tbl, {"class":"header"} )

	def indexesDetails(self):
		if self.table.indexes() == None or len(self.table.indexes()) <= 0:
			return None

		tbl = []

		# define the table header
		header = ( QApplication.translate("DBManagerPlugin", "Name"), QApplication.translate("DBManagerPlugin", "Column(s)") )
		tbl.append( HtmlTableHeader( header ) )

		# add table contents
		for idx in self.table.indexes():
			# get the fields the index is defined on
			tbl.append( (idx.name, idx.column) )

		return HtmlTable( tbl, {"class":"header"} )

	def triggersDetails(self):
		if self.table.triggers() == None or len(self.table.triggers()) <= 0:
			return None

		ret = []

		tbl = []
		# define the table header
		header = ( QApplication.translate("DBManagerPlugin", "Name"), QApplication.translate("DBManagerPlugin", "Event"), QApplication.translate("DBManagerPlugin", "Type"), QApplication.translate("DBManagerPlugin", "Enabled") )
		tbl.append( HtmlTableHeader( header ) )

		# add table contents
		for trig in self.table.triggers():
			name = u'%(name)s (<a href="action:trigger/%(name)s/%(action)s">%(action)s</a>)' % { "name":trig.name, "action":"delete" }

			(enabled, action) = (QApplication.translate("DBManagerPlugin", "Yes"), "disable") if trig.enabled == u"ENABLED" else (QApplication.translate("DBManagerPlugin", "No"), "enable")
			txt_enabled = u'%(enabled)s (<a href="action:trigger/%(name)s/%(action)s">%(action)s</a>)' % { "name":trig.name, "action":action, "enabled":enabled }

			tbl.append( (name, trig.event, trig.type, txt_enabled) )

		ret.append( HtmlTable( tbl, {"class":"header"} ) )

		ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", '<a href="action:triggers/enable">Enable all triggers</a> / <a href="action:triggers/disable">Disable all triggers</a>') ) )

		return ret


	def getTableInfo(self):
		ret = []

		general_info = self.generalInfo()
		if general_info == None:
			pass
		else:
			ret.append( HtmlSection( QApplication.translate("DBManagerPlugin", 'General info'), general_info ) )

		# spatial info
		spatial_info = self.spatialInfo()
		if spatial_info == None:
			pass
		else:
			spatial_info = HtmlContent( spatial_info )
			if not spatial_info.hasContents():
				spatial_info = QApplication.translate("DBManagerPlugin", '<warning> This is not a spatial table.')
			ret.append( HtmlSection( self.table.database().connection().typeNameString(), spatial_info ) )

		# fields
		fields_details = self.fieldsDetails()
		if fields_details == None:
			pass
		else:
			ret.append( HtmlSection( QApplication.translate("DBManagerPlugin", 'Fields'), fields_details ) )

		# constraints
		constraints_details = self.constraintsDetails()
		if constraints_details == None:
			pass
		else:
			ret.append( HtmlSection( QApplication.translate("DBManagerPlugin", 'Constraints'), constraints_details ) )

		# indexes
		indexes_details = self.indexesDetails()
		if indexes_details == None:
			pass
		else:
			ret.append( HtmlSection( QApplication.translate("DBManagerPlugin", 'Indexes'), indexes_details ) )

		# triggers
		triggers_details = self.triggersDetails()
		if triggers_details == None:
			pass
		else:
			ret.append( HtmlSection( QApplication.translate("DBManagerPlugin", 'Triggers'), triggers_details ) )

		return ret

class ORVectorTableInfo(ORTableInfo, VectorTableInfo):
	def __init__(self, table):
		VectorTableInfo.__init__(self, table)
		ORTableInfo.__init__(self, table)

	def spatialInfo(self):
		ret = []
		if self.table.geomType == None:
			return ret

		tbl = [
			(QApplication.translate("DBManagerPlugin", "Column:"), self.table.geomColumn),
			(QApplication.translate("DBManagerPlugin", "Geometry:"), self.table.geomType),
                        (QApplication.translate("DBManagerPlugin", "QGis Geometry type:"), self.table.wkbType)
		]

		# only if we have info from geometry_columns
		if self.table.geomDim:
			tbl.append( (QApplication.translate("DBManagerPlugin", "Dimension:"), self.table.geomDim) )

		srid = self.table.srid if self.table.srid != None else -1
		sr_info = self.table.database().connector.getSpatialRefInfo(srid) if srid != -1 else QApplication.translate("DBManagerPlugin", "Undefined")
		if sr_info:
			tbl.append( (QApplication.translate("DBManagerPlugin", "Spatial ref:"), u"%s (%d)" % (sr_info, srid)) )

		# estimated extent
		if not self.table.isView:
			if self.table.estimatedExtent == None:
				# estimated extent information is not displayed yet, so just block
				# table signals to avoid double refreshing (infoViewer->refreshEstimatedExtent->tableChanged->infoViewer)
				self.table.blockSignals(True)
				self.table.refreshTableEstimatedExtent()
				self.table.blockSignals(False)

			if self.table.estimatedExtent != None and self.table.estimatedExtent[0] != None:
				estimated_extent_str = '%.5f, %.5f - %.5f, %.5f' % self.table.estimatedExtent
				tbl.append( (QApplication.translate("DBManagerPlugin", "Estimated extent:"), estimated_extent_str) )

		# extent
		if self.table.extent != None and self.table.extent[0] != None:
			extent_str = '%.5f, %.5f - %.5f, %.5f' % self.table.extent
		else:
			extent_str = QApplication.translate("DBManagerPlugin", '(unknown) (<a href="action:extent/get">find out</a>)')
		tbl.append( (QApplication.translate("DBManagerPlugin", "Extent:"), extent_str) )

		ret.append( HtmlTable( tbl ) )

                # Handle extent update metadata
                if self.table.extent != None and self.table.extent[0] != None and self.table.estimatedExtent != None and self.table.estimatedExtent[0] != None and self.table.extent != self.table.estimatedExtent:
                        ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", '<warning> Metadata extent is different from real extent. You should <a href="action:extent/update">update it</a> !') ) )

		# is there an entry in geometry_columns?
		if self.table.geomType.lower() == 'geometry':
			ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", "<warning> There isn't entry in geometry_columns!") ) )

		# find out whether the geometry column has spatial index on it
		if not self.table.isView:
			if not self.table.hasSpatialIndex():
				ret.append( HtmlParagraph( QApplication.translate("DBManagerPlugin", '<warning> No spatial index defined (<a href="action:spatialindex/create">create it</a>)') ) )

		return ret


