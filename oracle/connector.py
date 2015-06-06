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
from PyQt4.QtSql import QSqlDatabase

from ..connector import DBConnector
from ..plugin import ConnectionError, DbError, Table

import os
from qgis.core import QGis, QgsApplication, QgsMessageLog
import QtSqlDB
import sqlite3


def classFactory():
    return OracleDBConnector if QSqlDatabase.isDriverAvailable("QOCISPATIAL") else None


class OracleDBConnector(DBConnector):

    def __init__(self, uri, connName):
        DBConnector.__init__(self, uri)

        self.connName = connName
        self.user = uri.username() or os.environ.get('USER')
        self.passwd = uri.password()
        self.host = uri.host()

        if self.host != "":
            self.dbname = self.host
            if uri.port() != "" and uri.port() != "1521":
                self.dbname += ":" + uri.port()
            if uri.database() != "":
                self.dbname += "/" + uri.database()
        elif uri.dbname != "":
            self.dbname = uri.database()

        # Connection options
        self.useEstimatedMetadata = uri.useEstimatedMetadata()
        self.userTablesOnly = uri.param('userTablesOnly').lower() == "true"
        self.geometryColumnsOnly = uri.param(
            'geometryColumnsOnly').lower() == "true"
        self.allowGeometrylessTables = uri.param(
            'allowGeometrylessTables').lower() == "true"
        self.onlyExistingTypes = uri.param(
            'onlyExistingTypes').lower() == "true"

        try:
            self.connection = QtSqlDB.connect(
                "QOCISPATIAL", self.dbname, self.user, self.passwd)

        except self.connection_error_types(), e:
            raise ConnectionError(e)

        # Find if we can connect to data_sources_cache.db
        sqlite_cache_file = os.path.join(
            QgsApplication.qgisSettingsDirPath(), u"data_sources_cache.db")
        if (os.path.isfile(sqlite_cache_file)):
            try:
                self.cache_connection = sqlite3.connect(sqlite_cache_file)
            except sqlite3.Error as e:

                self.cache_connection = False

        # Find if there is cache for our connection:
        if self.cache_connection:
            try:
                cache_c = self.cache_connection.cursor()
                cache_c.execute(
                    u"SELECT COUNT(*) FROM meta_oracle WHERE conn ='%s'" % self.connName)
                has_cached = cache_c.fetchone()[0]
                cache_c.close()
                if not (has_cached and int(has_cached) > 0):
                    self.cache_connection = False

            except sqlite3.Error as e:
                self.cache_connection = False

        self._checkSpatial()
        self._checkGeometryColumnsTable()

    def _connectionInfo(self):
        return unicode(self._uri.connectionInfo())

    def _checkSpatial(self):
        """ check whether Oracle Spatial is present in catalog """
        c = self._execute(
            None, u"SELECT count(*) FROM v$option WHERE parameter='Spatial' AND value='TRUE'")
        self.has_spatial = self._fetchone(c)[0] > 0
        c.close()

        return self.has_spatial

    def _checkGeometryColumnsTable(self):
        """ check if user can read *_SDO_GEOM_METADATA view """
        c = self._execute(
            None, u"SELECT PRIVILEGE FROM ALL_TAB_PRIVS_RECD WHERE TABLE_NAME IN ('ALL_SDO_GEOM_METADATA', 'USER_SDO_GEOM_METADATA') AND PRIVILEGE = 'SELECT' AND ROWNUM=1")
        res = self._fetchone(c)
        c.close()

        self.has_geometry_columns = (res != None and len(res) != 0)

        if not self.has_geometry_columns:
            self.has_geometry_columns_access = self.is_geometry_columns_view = False
        else:
            self.is_geometry_columns_view = True
            # find out whether has privileges to access geometry_columns table
            priv = self.getTablePrivileges('ALL_SDO_GEOM_METADATA')
            self.has_geometry_columns_access = priv[0]
        return self.has_geometry_columns

    def getInfo(self):
        """ returns Oracle Database server version"""
        c = self._execute(None, u"SELECT * FROM V$VERSION WHERE ROWNUM < 2")
        res = self._fetchone(c)
        c.close()
        return res

    def hasCache(self):
        """ returns self.cache_connection """
        if self.cache_connection:
            return True
        return False

    def getSpatialInfo(self):
        """ returns Oracle Spatial version """
        if not self.has_spatial:
            return

        try:
            c = self._execute(None, u"SELECT SDO_VERSION FROM DUAL")
        except DbError:
            return
        res = self._fetchone(c)
        c.close()

        return res

    def hasSpatialSupport(self):
        return self.has_spatial

    def hasRasterSupport(self):
        """ No raster support for the moment"""
        # return self.has_raster
        return False

    def hasCustomQuerySupport(self):
        """ From QGis v2.2 Oracle custom queries are supported. """
        from qgis.core import QGis
        return QGis.QGIS_VERSION[0:3] >= "2.2"

    def hasTableColumnEditingSupport(self):
        return True

    def fieldTypes(self):
        """ From http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1828 """
        return [
            "number", "number(9)",  # integers
            "number(9,2)", "number(*,4)", "binary_float", "binary_double", # floats
            "varchar2(255)", "char(20)", "nvarchar2(255)", "nchar(20)", # strings
            "date", "timestamp" # date/time
        ]

    def getSchemaPrivileges(self, schema):
        """ schema privileges: (can create new objects, can access objects in schema) """
        # TODO: find the best way in Oracle do determine schema privileges
        schema = self.user if schema == None else schema
        # In Oracle world, rights seems quite simple: only schema_owner can
        # create table in the schema

        if schema == self.user:
            return (True, True)
        # getSchemas request only extract schemas where user has access
        return (False, True)

    def getTablePrivileges(self, table):
        """ table privileges: (select, insert, update, delete) """

        schema, tablename = self.getSchemaTableName(table)

        if schema:
            schema_where = u" AND TABLE_SCHEMA = %s" % self.quoteString(schema)
        else:
            schema_where = u""

        sql = u"SELECT DISTINCT PRIVILEGE FROM ALL_TAB_PRIVS WHERE privilege IN ('SELECT','INSERT','UPDATE','DELETE') AND TABLE_NAME = %s %s" % (
            self.quoteString(tablename), schema_where)

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()

        result = [False, False, False, False]
        for line in res:
            if line[0] == u"SELECT":
                result[0] = True
            if line[0] == u"INSERT":
                result[1] = True
            if line[0] == u"UPDATE":
                result[2] = True
            if line[0] == u"DELETE":
                result[3] = True

        return result

    def getSchemasCache(self):
        sql = u"""SELECT DISTINCT ownername FROM "oracle_%s" ORDER BY ownername""" % self.connName
        c = self.cache_connection.cursor()
        c.execute(sql)
        res = c.fetchall()
        c.close()

        return res

    def getSchemas(self):
        """ get list of schemas in tuples: (oid, name, owner, perms, comment) """
        if self.userTablesOnly:
            return [(self.user,)]

        if self.hasCache():
            return self.getSchemasCache()

        # Use cache if avalaible:
        metatable = u"all_objects WHERE object_type IN ('TABLE','VIEW','SYNONYM')"
        if self.geometryColumnsOnly:
            metatable = u"all_sdo_geom_metadata"

        sql = u"""SELECT DISTINCT owner FROM %s ORDER BY owner""" % metatable

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()

        return res

    def getTables(self, schema=None, add_sys_tables=False):
        """ get list of tables """

        if self.hasCache():
            return self.getTablesCache(schema)

        tablenames = []
        items = []

        try:
            vectors = self.getVectorTables(schema)
            for tbl in vectors:
                tablenames.append((tbl[2], tbl[1]))
                items.append(tbl)
        except DbError:
            pass

        if not self.allowGeometrylessTables:
            return sorted(items, cmp=lambda x, y: cmp((x[2], x[1]), (y[2], y[1])))

        # get all non geographic tables and views
        prefix = u"all"
        owner = u"c.owner"
        metatable = u"tab_columns"
        schema_where = u""
        if self.userTablesOnly:
            prefix = u"user"
            owner = u"user As owner"
        if schema and not self.userTablesOnly:
            schema_where = u"WHERE c.owner = %s AND o.owner = c.owner AND e.owner = c.owner " % self.quoteString(
                schema)

        sql = u"""SELECT c.table_name, %s,
                                 o.object_type, %s,
                                 c.num_rows,
                                 e.comments
                          FROM %s_all_tables c
                               JOIN %s_objects o ON
                                 c.table_name=o.object_name
                                 AND o.object_type IN ('TABLE','VIEW','SYNONYM')
                               JOIN %s_tab_comments e ON
                                 c.table_name = e.table_name %s
                          ORDER BY TABLE_NAME""" % (owner, owner, prefix, prefix, prefix, schema_where)

        c = self._execute(None, sql)
        for tbl in self._fetchall(c):
            if tablenames.count((tbl[1], tbl[0])) <= 0:
                item = list(tbl)
                item.insert(0, Table.TableType)
                items.append(item)
        c.close()

        return sorted(items, cmp=lambda x, y: cmp((x[2], x[1]), (y[2], y[1])))

    def getTablesCache(self, schema=None):
        """ get list of tables from SQLite cache"""

        tablenames = []
        items = []

        try:
            vectors = self.getVectorTablesCache(schema)
            for tbl in vectors:
                tablenames.append((tbl[2], tbl[1]))
                items.append(tbl)
        except DbError:
            pass

        if not self.allowGeometrylessTables:
            return sorted(items, cmp=lambda x, y: cmp((x[2], x[1]), (y[2], y[1])))

        # get all non geographic tables and views
        schema_where = u""
        if self.userTablesOnly:
            schema_where = u"AND ownername = '%s'" % self.user
        if schema and not self.userTablesOnly:
            schema_where = u"AND ownername = '%s'" % schema

        sql = u"""SELECT tablename, ownername, isview, ownername,
                                 0 As rowcount, null As comment
                          FROM "oracle_%s"
                          WHERE geometrycolname IS '' %s
                          ORDER BY tablename""" % (self.connName, schema_where)

        c = self.cache_connection.cursor()
        c.execute(sql)
        for tbl in c.fetchall():
            if tablenames.count((tbl[1], tbl[0])) <= 0:
                item = list(tbl)
                item.insert(0, Table.TableType)
                items.append(item)
        c.close()

        return sorted(items, cmp=lambda x, y: cmp((x[2], x[1]), (y[2], y[1])))

    def getVectorTablesCache(self, schema=None):
        """ get list of table with a geometry column from SQLite cache
                it returns:
                        name (table name)
                        namespace (schema)
                        type = 'view' (is a view?)
                        owner
                        estimated row_nums
                        comment (only for tables not materialized views)
                        geometry_column
                        geometry_type (as WKB type)
                        coord_dimension (always NULL for the moment)
                        srid
        """
        schema_where = u""
        if self.userTablesOnly:
            schema_where = u"AND ownername = '%s'" % self.user
        if schema and not self.userTablesOnly:
            schema_where = u"AND ownername = '%s'" % schema

        sql = u"""SELECT tablename, ownername, isview, ownername,
                                 0 As rowcount, null As comment, geometrycolname,
                                 geomtypes, null As wkbtype, 2, geomsrids
                          FROM "oracle_%s"
                          WHERE geometrycolname IS NOT '' %s
                          ORDER BY tablename""" % (self.connName, schema_where)

        items = []

        c = self.cache_connection.cursor()
        c.execute(sql)
        lst_tables = c.fetchall()
        c.close()

        # Handle multiple geometries tables
        for i, tbl in enumerate(lst_tables):
            item = list(tbl)
            item.insert(0, Table.VectorType)
            if len(item[-4]) > 0 and len(item[-1]) > 0:
                geomtypes = [int(l) for l in unicode(item[-4]).split(u",")]
                srids = [int(l) for l in unicode(item[-1]).split(u",")]
                # Intelligent wkbtype grouping (multi with non multi)
                if QGis.WKBPolygon in geomtypes and QGis.WKBMultiPolygon in geomtypes:
                    srids.pop(geomtypes.index(QGis.WKBPolygon))
                    geomtypes.pop(geomtypes.index(QGis.WKBPolygon))
                elif QGis.WKBPoint in geomtypes and QGis.WKBMultiPoint in geomtypes:
                    srids.pop(geomtypes.index(QGis.WKBPoint))
                    geomtypes.pop(geomtypes.index(QGis.WKBPoint))
                elif QGis.WKBLineString in geomtypes and QGis.WKBMultiLineString in geomtypes:
                    srids.pop(geomtypes.index(QGis.WKBLineString))
                    geomtypes.pop(geomtypes.index(QGis.WKBLineString))

                for j in range(len(geomtypes)):
                    buf = list(item)
                    geomtype = geomtypes[j]
                    srid = srids[j]
                    if geomtype in (QGis.WKBPoint, QGis.WKBMultiPoint):
                        geo = u"POINT"
                    elif geomtype in (QGis.WKBLineString, QGis.WKBMultiLineString):
                        geo = u"LINESTRING"
                    elif geomtype in (QGis.WKBPolygon, QGis.WKBMultiPolygon):
                        geo = u"POLYGON"
                    else:
                        geo = u"UNKNOWN"

                    buf[-4] = geo
                    buf[-3] = geomtype
                    buf[-1] = srid
                    items.append(buf)

        return items

    def getVectorTables(self, schema=None):
        """ get list of table with a geometry column
                it returns:
                        name (table name)
                        namespace (schema)
                        type = 'view' (is a view?)
                        owner
                        estimated row_nums
                        comment (only for tables not materialized views)
                        geometry_column
                        geometry_type (as WKB type)
                        coord_dimension (always NULL for the moment)
                        srid
        """
        if not self.has_spatial:
            return []

        # discovery of all geographic tables
        prefix = u"all"
        owner = u"c.owner"
        metatable = u"tab_columns"
        schema_where = u""
        if self.userTablesOnly:
            prefix = u"user"
            owner = u"user As owner"
        if schema and not self.userTablesOnly:
            schema_where = u" WHERE c.owner = %s AND o.owner = c.owner AND d.owner = c.owner AND e.owner = c.owner " % self.quoteString(
                schema)

        sql = u"""SELECT c.table_name, %s,
                                 o.object_type, %s,
                                 d.num_rows,
                                 e.comments,
                                 c.column_name,
                                 NULL as geomtypes,
                                 NULL as wkbtype,
                                 NULL,
                                 c.srid
                          FROM %s_sdo_geom_metadata c
                               JOIN %s_objects o ON
                                 c.table_name=o.object_name
                                 AND o.object_type IN ('TABLE','VIEW','SYNONYM')
                               JOIN %s_all_tables d ON
                                 c.table_name = d.table_name
                               JOIN %s_tab_comments e ON
                                 c.table_name = e.table_name %s
                          ORDER BY TABLE_NAME""" % (owner, owner, prefix, prefix, prefix, prefix, schema_where)

        # For each table, get all of the details
        items = []

        c = self._execute(None, sql)
        lst_tables = self._fetchall(c)
        c.close()

        for i, tbl in enumerate(lst_tables):
            item = list(tbl)
            if schema:
                table_name = u"%s.%s" % (
                    self.quoteId(schema), self.quoteId(item[0]))
            else:
                table_name = self.quoteId(item[0])
            geocol = self.quoteId(item[-5])
            geomtypes = self.getTableGeomTypes(table_name, geocol)
            item.insert(0, Table.VectorType)

            # Intelligent wkbtype grouping (multi with non multi)
            if QGis.WKBPolygon in geomtypes and QGis.WKBMultiPolygon in geomtypes:
                geomtypes.pop(geomtypes.index(QGis.WKBPolygon))
            elif QGis.WKBPoint in geomtypes and QGis.WKBMultiPoint in geomtypes:
                geomtypes.pop(geomtypes.index(QGis.WKBPoint))
            elif QGis.WKBLineString in geomtypes and QGis.WKBMultiLineString in geomtypes:
                geomtypes.pop(geomtypes.index(QGis.WKBLineString))

            for j in range(len(geomtypes)):
                buf = list(item)
                buf[-2] = 2
                geomtype = geomtypes[j]
                if geomtype in (QGis.WKBPoint, QGis.WKBMultiPoint):
                    geo = u"POINT"
                elif geomtype in (QGis.WKBLineString, QGis.WKBMultiLineString):
                    geo = u"LINESTRING"
                elif geomtype in (QGis.WKBPolygon, QGis.WKBMultiPolygon):
                    geo = u"POLYGON"
                else:
                    geo = u"UNKNOWN"

                buf[-4] = geo
                buf[-3] = geomtype
                items.append(buf)

        return items

    def getTableGeomTypes(self, table, geomCol):
        """ Return all the wkbTypes for a table by requesting geometry column"""

        estimated = u""
        if self.useEstimatedMetadata:
            from qgis.core import QgsMessageLog
            QgsMessageLog.logMessage(
                "estimated", 'DBManager', QgsMessageLog.INFO)
            estimated = u"AND ROWNUM < 100"

        # Grab all of geometry types from the layer
        query =  u"""SELECT DISTINCT a.%s.SDO_GTYPE As gtype
                           FROM %s a
                           WHERE a.%s IS NOT NULL %s
                           ORDER BY a.%s.SDO_GTYPE""" % (geomCol, table, geomCol, estimated, geomCol)

        try:
            c = self._execute(None, query)
        except DbError, e:  # handle error views or other problems
            return [QGis.WKBUnknown]

        rows = self._fetchall(c)
        c.close()

        # Handle results
        if len(rows) == 0:
            return [QGis.WKBUnknown]

        # A dict to store the geomtypes
        geomtypes = []

        for row in rows:
            if row[0] == 2001:
                geomtypes.append(QGis.WKBPoint)
            elif row[0] == 2002:
                geomtypes.append(QGis.WKBLineString)
            elif row[0] == 2003:
                geomtypes.append(QGis.WKBPolygon)
            elif row[0] == 2005:
                geomtypes.append(QGis.WKBMultiPoint)
            elif row[0] == 2006:
                geomtypes.append(QGis.WKBMultiLineString)
            elif row[0] == 2007:
                geomtypes.append(QGis.WKBMultiPolygon)

        return geomtypes

    def getTableMainGeomType(self, table, geomCol):
        """ Return the best wkbType for a table by requesting geometry column"""

        wkbType = QGis.WKBUnknown

        estimated = u""
        if self.useEstimatedMetadata:
            estimated = u"AND ROWNUM < 100"

        # Grab all of geometry types from the layer
        query =  u"""SELECT DISTINCT a.%s.SDO_GTYPE As gtype,
                                  COUNT(a.%s.SDO_GTYPE) As nb_rows
                           FROM %s a
                           WHERE a.%s IS NOT NULL %s
                           GROUP BY a.%s.SDO_GTYPE
                           ORDER BY nb_rows DESC""" % (geomCol, geomCol, table, geomCol, estimated, geomCol)

        # A dict to handle geometry types weight
        geom_types = {'Point': 0,
                      'MultiPoint': 0,
                      'Linestring': 0,
                      'MultiLinestring': 0,
                      'Polygon': 0,
                      'MultiPolygon': 0}

        try:
            c = self._execute(None, query)
        except DbError, e:  # handle error views or other problems
            return QGis.WKBUnknown

        rows = self._fetchall(c)
        c.close()

        # Handle results
        if len(rows) == 0:
            return QGis.WKBUnknown

        for row in rows:
            if row[0] == 2001:
                geom_types['Point'] = row[1]
            elif row[0] == 2002:
                geom_types['Linestring'] = row[1]
            elif row[0] == 2003:
                geom_types['Polygon'] = row[1]
            elif row[0] == 2005:
                geom_types['MultiPoint'] = row[1]
            elif row[0] == 2006:
                geom_types['MultiLinestring'] = row[1]
            elif row[0] == 2007:
                geom_types['MultiPolygon'] = row[1]

        # Make the decision:
        champion = list(
            sorted(geom_types, key=geom_types.__getitem__, reverse=True))[0]
        if champion == u"Point":
            wkbType = QGis.WKBPoint
            if geom_types['MultiPoint'] > 0:
                wkbType = QGis.WKBMultiPoint
        elif champion == u"MultiPoint":
            wkbType = QGis.WKBMultiPoint
        elif champion == u"Linestring":
            wkbType = QGis.WKBLineString
            if geom_types['MultiLinestring'] > 0:
                wkbType = QGis.WKBMultiLineString
        elif champion == u"MultiLinestring":
            wkbType = QGis.WKBMultiLineString
        elif champion == u"Polygon":
            wkbType = QGis.WKBPolygon
            if geom_types['MultiPolygon'] > 0:
                wkbType = QGis.WKBMultiPolygon
        elif champion == u"MultiPolygon":
            wkbType = QGis.WKBMultiPolygon

        return wkbType

    def getTableRowCount(self, table):
        """ returns the number of rows of the table """
        c = self._execute(
            None, u"SELECT COUNT(*) FROM %s" % self.quoteId(table))
        res = self._fetchone(c)[0]
        c.close()

        return res

    def getTableFields(self, table):
        """ return list of columns in table """

        schema, tablename = self.getSchemaTableName(table)
        schema_where = u" AND a.OWNER=%s " % self.quoteString(
            schema) if schema else ""

        sql = u"""SELECT a.COLUMN_ID As ordinal_position,
                                 a.COLUMN_NAME As column_name,
                                 a.DATA_TYPE As data_type,
                                 a.DATA_LENGTH As char_max_len,
                                 a.DATA_LENGTH As modifier,
                                 a.NULLABLE As nullable,
                                 a.DEFAULT_LENGTH As hasdefault,
                                 a.DATA_DEFAULT As default_value,
                                 a.DATA_TYPE As formatted_type,
                                 c.COMMENTS
                          FROM ALL_TAB_COLUMNS a
                               JOIN ALL_COL_COMMENTS c ON
                                    a.TABLE_NAME = c.TABLE_NAME
                                    AND a.COLUMN_NAME = c.COLUMN_NAME
                                    AND a.OWNER = c.OWNER
                          WHERE a.TABLE_NAME= %s %s
                          ORDER BY a.COLUMN_ID"""  % (self.quoteString(tablename), schema_where)

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()
        return res

    def getTableIndexes(self, table):
        """ get info about table's indexes """
        schema, tablename = self.getSchemaTableName(table)
        schema_where = u" AND i.OWNER=%s " % self.quoteString(
            schema) if schema else ""

        sql = u"""SELECT i.index_name, c.COLUMN_NAME, i.uniqueness
                          FROM ALL_INDEXES i
                          INNER JOIN ALL_IND_COLUMNS c ON i.index_name = c.index_name
                          WHERE i.table_name = %s %s""" % (self.quoteString(tablename), schema_where)

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()

        return res

    def getTableConstraints(self, table):
        schema, tablename = self.getSchemaTableName(table)
        schema_where = u" AND c.OWNER=%s " % self.quoteString(
            schema) if schema else ""

        sql = u"""SELECT a.CONSTRAINT_NAME, a.CONSTRAINT_TYPE, a.DEFERRABLE, a.DEFERRED, c.COLUMN_NAME,
                                 a.SEARCH_CONDITION
                          FROM ALL_CONS_COLUMNS c
                               INNER JOIN ALL_CONSTRAINTS a ON a.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                          WHERE c.TABLE_NAME = %s %s"""  % (self.quoteString(tablename), schema_where)

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()

        return res

    def getTableTriggers(self, table):
        schema, tablename = self.getSchemaTableName(table)

        sql = u"""SELECT TRIGGER_NAME, TRIGGERING_EVENT, TRIGGER_TYPE, STATUS
                          FROM ALL_TRIGGERS
                          WHERE TABLE_OWNER = %s
                          AND TABLE_NAME=%s""" % (self.quoteString(schema), self.quoteString(tablename))

        c = self._execute(None, sql)
        res = self._fetchall(c)
        c.close()

        return res

    def enableAllTableTriggers(self, enable, table):
        """ enable or disable all triggers on table """
        triggers = [l[0] for l in self.getTableTriggers(table)]
        for trigger in triggers:
            self.enableTableTrigger(trigger, enable, table)

    def enableTableTrigger(self, trigger, enable, table):
        """ enable or disable one trigger on table """
        schema, tablename = self.getSchemaTableName(table)
        trigger = u".".join([self.quoteId(schema), self.quoteId(trigger)])
        sql = u"ALTER TRIGGER %s %s" % (
            trigger, "ENABLE" if enable else "DISABLE")
        self._execute_and_commit(sql)

    def deleteTableTrigger(self, trigger, table):
        """ delete trigger on table """
        schema, tablename = self.getSchemaTableName(table)
        trigger = u".".join([self.quoteId(schema), self.quoteId(trigger)])
        sql = u"DROP TRIGGER %s" % trigger
        self._execute_and_commit(sql)

    def updateExtentMetadata(self, table, geom):
        # TODO: rebuild index
        schema, tablename = self.getSchemaTableName(table)

        res = [str(l) for l in self.getTableExtent(table, geom)]

        if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[2]:
            sql = u"""UPDATE ALL_SDO_GEOM_METADATA SET DIMINFO =
                                   MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', %s, %s, 0.005),
                                                       MDSYS.SDO_DIM_ELEMENT('Y', %s, %s, 0.005) )
                          WHERE TABLE_NAME = %s
                          AND OWNER = %s""" % ( res[0], res[1], res[2], res[3], self.quoteString(tablename), self.quoteString(schema))
        elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[2]:
            sql = u"""UPDATE USER_SDO_GEOM_METADATA SET DIMINFO =
                                  MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', %s, %s, 0.005),
                                                       MDSYS.SDO_DIM_ELEMENT('Y', %s, %s, 0.005) )
                          WHERE TABLE_NAME = %s """ % ( res[0], res[1], res[2], res[3], self.quoteString(tablename))

        else:
            # nothing to do, you don't have sufficient rights...
            pass

        self._execute_and_commit(sql)

    def getTableExtent(self, table, geom):
        """ find out table extent """
        schema, tablename = self.getSchemaTableName(table)
        tablename = "'%s.%s'" % (schema, tablename)
        # if table as spatial index:
        if self.getTableIndexes(table):
            sql = u"SELECT SDO_TUNE.EXTENT_OF(%s, %s).SDO_ORDINATES FROM DUAL" % (
                tablename, self.quoteString(geom))
        else:
            sql = u"SELECT SDO_AGGR_MBR(%s).SDO_ORDINATES FROM %s" % (
                self.quoteId(geom), self.quoteId(table))

        try:
            c = self._execute(None, sql)
        except DbError, e:  # no spatial index on table, try aggregation
            return None

        res = self._fetchone(c)[0]
        c.close()

        if res is not None:
            res = (res[0],res[2],res[1],res[3])

        return tuple(res) if res is not None else None

    def getTableEstimatedExtent(self, table, geom):
        """ find out estimated extent (from metadata view) """
        schema, tablename = self.getSchemaTableName(table)

        sql = u"SELECT sdo_lb,sdo_ub FROM mdsys.all_sdo_geom_metadata m, table(m.diminfo) WHERE owner=%s AND table_name=%s AND column_name=%s AND sdo_dimname='X'" %(self.quoteString(schema), self.quoteString(tablename), self.quoteString(geom))

        try:
            c = self._execute(None, sql)
        except DbError, e:  # no statistics for the current table
            return None

        res_x = self._fetchone(c)
        c.close()

        if not res_x or len(res_x) < 2:
            return None

        sql = u"SELECT sdo_lb,sdo_ub FROM mdsys.all_sdo_geom_metadata m, table(m.diminfo) WHERE owner=%s AND table_name=%s AND column_name=%s AND sdo_dimname='Y'" % (
            self.quoteString(schema), self.quoteString(tablename), self.quoteString(geom))

        try:
            c = self._execute(None, sql)
        except DbError, e:  # no statistics for the current table
            return None

        res_y = self._fetchone(c)
        c.close()

        if not res_y or if len(res_y) < 2:
            return None

        return (res_x[0], res_x[1], res_y[0], res_y[1])

    def getViewDefinition(self, view):
        """ returns definition of the view """

        schema, tablename = self.getSchemaTableName(view)
        schema_where = u" AND OWNER=%s " % self.quoteString(
            schema) if schema else ""

        sql = u"""SELECT TEXT FROM ALL_VIEWS WHERE VIEW_NAME = %s %s""" % (
            self.quoteString(tablename), schema_where)

        c = self._execute(None, sql)
        res = self._fetchone(c)
        c.close()

        return res[0] if res is not None else None

    def getSpatialRefInfo(self, srid):
        """ returns human name from an srid as describe in Oracle sys table"""
        if not self.has_spatial:
            return

        try:
            c = self._execute(
                None, "SELECT CS_NAME FROM MDSYS.CS_SRS WHERE SRID = %d" % srid)
        except DbError, e:
            return
        sr = self._fetchone(c)
        c.close()

        return sr[0] if sr is not None else None

    def isVectorTable(self, table):
        """ determine if a table is a vector one by looking into metadata view """
        if self.has_geometry_columns and self.has_geometry_columns_access:
            schema, tablename = self.getSchemaTableName(table)
            sql = u"SELECT count(*) FROM all_sdo_geom_metadata WHERE owner = %s AND table_name = %s" % (
                self.quoteString(schema), self.quoteString(tablename))

            c = self._execute(None, sql)
            res = self._fetchone(c)
            c.close()
            return res != None and res[0] > 0

        return False

    def createTable(self, table, field_defs, pkey):
        """ create ordinary table
                        'fields' is array containing field definitions
                        'pkey' is the primary key name
        """
        if len(field_defs) == 0:
            return False

        sql = "CREATE TABLE %s (" % self.quoteId(table)
        sql += u", ".join(field_defs)
        if pkey != None and pkey != "":
            sql += u", PRIMARY KEY (%s)" % self.quoteId(pkey)
        sql += ")"

        self._execute_and_commit(sql)
        return True

    def deleteTable(self, table):
        """ delete table and its reference in sdo_geom_metadata """

        schema, tablename = self.getSchemaTableName(table)
        schema_part = u"AND owner = %s " % self.quoteString(
            schema) if schema else ""

        if self.isVectorTable(table):
            if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[3]:
                sql = u"DELETE FROM ALL_SDO_GEOM_METADATA WHERE table_name = %s %s" % (
                    self.quoteString(tablename), schema_part)
            elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[3]:
                sql = u"DELETE FROM USER_SDO_GEOM_METADATA WHERE table_name = %s" % self.quoteString(
                    tablename)
            self._execute_and_commit(sql)

        sql = u"DROP TABLE %s" % self.quoteId(table)
        self._execute_and_commit(sql)

    def emptyTable(self, table):
        """ delete all rows from table """
        sql = u"TRUNCATE TABLE %s" % self.quoteId(table)
        self._execute_and_commit(sql)

    def renameTable(self, table, new_table):
        """ rename a table in database """
        schema, tablename = self.getSchemaTableName(table)
        if new_table == tablename:
            return

        c = self._get_cursor()

        # update geometry_columns if Spatial is enabled
        if self.isVectorTable(table):
            if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[2]:
                sql = u"UPDATE ALL_SDO_GEOM_METADATA SET TABLE_NAME = %s WHERE table_name = %s AND owner = %s" % (
                    self.quoteString(new_table), self.quoteString(tablename), self.quoteString(schema))
            elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[2]:
                sql = u"UPDATE USER_SDO_GEOM_METADATA SET TABLE_NAME = %s WHERE table_name = %s" % (
                    self.quoteString(new_table), self.quoteString(tablename))
            else:
                self._commit()

            self._execute(c, sql)

        sql = u"ALTER TABLE %s RENAME TO %s" % (
            self.quoteId(table), self.quoteId(new_table))
        self._execute(c, sql)

        self._commit()

    def createView(self, view, query):
        sql = u"CREATE VIEW %s AS %s" % (self.quoteId(view), query)
        self._execute_and_commit(sql)

    def deleteView(self, view):
        sql = u"DROP VIEW %s" % self.quoteId(view)
        self._execute_and_commit(sql)

    def renameView(self, view, new_name):
        """ rename view in database """
        self.renameTable(view, new_name)

    def createSchema(self, schema):
        """ create a new empty schema in database """
        # Not tested
        sql = u"CREATE SCHEMA AUTHORIZATION %s" % self.quoteId(schema)
        self._execute_and_commit(sql)

    def deleteSchema(self, schema):
        """ drop (empty) schema from database """
        sql = u"DROP USER %s CASCADE" % self.quoteId(schema)
        self._execute_and_commit(sql)

    def renameSchema(self, schema, new_schema):
        """ rename a schema in database """
        # Unsupported in Oracle
        pass

    def addTableColumn(self, table, field_def):
        """ add a column to table """
        sql = u"ALTER TABLE %s ADD %s" % (self.quoteId(table), field_def)
        self._execute_and_commit(sql)

    def deleteTableColumn(self, table, column):
        """ delete column from a table """
        schema, tablename = self.getSchemaTableName(table)
        prefix = schema_where = u""
        if self.isGeometryColumn(table, column):
            if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[3]:
                prefix = u"ALL"
                schema_where = u"AND OWNER = %s " % self.quoteString(schema)
            elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[3]:
                prefix = u"USER"

            sql = u"""DELETE FROM %s_SDO_GEOM_METADATA
                                  WHERE TABLE_NAME = %s
                                  AND COLUMN_NAME = %s %s""" % (prefix, self.quoteString(tablename), self.quoteString(column.upper()), schema_where)
            self._execute_and_commit(sql)

        sql = u"ALTER TABLE %s DROP COLUMN %s" % (
            self.quoteId(table), self.quoteId(column))
        self._execute_and_commit(sql)

    def updateTableColumn(self, table, column, new_name=None, data_type=None, not_null=None, default=None):
        schema, tablename = self.getSchemaTableName(table)
        if new_name == None and data_type == None and not_null == None and default == None:
            return

        c = self._get_cursor()

        # update column definition
        col_actions = []
        if data_type != None:
            col_actions.append(u"%s" % data_type)
        if not_null != None:
            col_actions.append(u"NOT NULL" if not_null else u"NULL")
        if default != None:
            if default and default != '':
                col_actions.append(u"DEFAULT %s" % default)
            else:
                col_actions.append(u"DEFAULT NULL")
        if len(col_actions) > 0:
            sql = u"ALTER TABLE %s" % self.quoteId(table)
            alter_col_str = u"MODIFY ( %s " % self.quoteId(column)
            for a in col_actions:
                sql += u" %s %s," % (alter_col_str, a)
                sql = sql[:-1] + u" )"
            self._execute(c, sql)

        # rename the column
        if new_name != None and new_name != column:
            sql = u"ALTER TABLE %s RENAME COLUMN %s TO %s" % (
                self.quoteId(table), self.quoteId(column), self.quoteId(new_name))
            self._execute(c, sql)

            # update geometry_columns if Spatial is enabled
            if self.isGeometryColumn(table, column):
                prefix = schema_where = u""
                if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[2]:
                    prefix = u"ALL"
                    schema_where = u"AND OWNER = %s" % self.quoteString(schema)
                elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[2]:
                    prefix = u"USER"

                sql = u"""UPDATE %s_SDO_GEOM_METADATA SET COLUMN_NAME = %s
                                          WHERE TABLE_NAME = %s
                                          AND COLUMN_NAME = %s %s""" % (prefix, self.quoteString(new_name), self.quoteString(tablename), self.quoteString(column), schema_where)
                self._execute(c, sql)

        self._commit()

    def renameTableColumn(self, table, column, new_name):
        """ rename column in a table """
        return self.updateTableColumn(table, column, new_name)

    def setTableColumnType(self, table, column, data_type):
        """ change column type """
        return self.updateTableColumn(table, column, None, data_type)

    def setTableColumnNull(self, table, column, is_null):
        """ change whether column can contain null values """
        return self.updateTableColumn(table, column, None, None, not is_null)

    def setTableColumnDefault(self, table, column, default):
        """ change column's default value.
            If default=None or an empty string drop default value """
        return self.updateTableColumn(table, column, None, None, None, default)

    def isGeometryColumn(self, table, column):
        schema, tablename = self.getSchemaTableName(table)
        schema_where = u"AND owner = %s " % self.quoteString(
            schema) if schema else ""

        sql = u"SELECT count(*) FROM all_sdo_geom_metadata WHERE table_name=%s AND column_name = %s %s" % (
            self.quoteString(tablename), self.quoteString(column.upper()), schema_where)

        c = self._execute(None, sql)
        res = self._fetchone(c)[0] > 0

        c.close()
        return res

    def addGeometryColumn(self, table, geom_column='GEOM', geom_type='POINT', srid=-1, dim=2):
        """ Add a geometry column and update Oracle Spatial metadata."""
        schema, tablename = self.getSchemaTableName(table)
        # in Metadata view, geographic column is always in uppercase
        geom_column = geom_column.upper()

        # Add the column to the table
        sql = u"ALTER TABLE %s ADD %s SDO_GEOMETRY" % (
            self.quoteId(table), self.quoteId(geom_column))

        self._execute_and_commit(sql)

        # Can we insert into all_sdo_geom_metadata or user_sdo_geom_metadata ?
        if self.getTablePrivileges('ALL_SDO_GEOM_METADATA')[1]:
            sql = u"""INSERT INTO ALL_SDO_GEOM_METADATA ( OWNER, TABLE_NAME, COLUMN_NAME, DIMINFO, SRID )
                          VALUES( """ + self.quoteString(schema) + u""",""" + self.quoteString(tablename) + u""",""" + self.quoteString(geom_column) + u""",
                          MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',0 , 20,0.005),
                          MDSYS.SDO_DIM_ELEMENT('Y',20 ,0, 0.005) ), """ + str(srid) + u""" )"""
        elif schema.lower() == self.user.lower() and self.getTablePrivileges('USER_SDO_GEOM_METADATA')[1]:
            sql = u"""INSERT INTO USER_SDO_GEOM_METADATA ( TABLE_NAME, COLUMN_NAME, DIMINFO, SRID )
                          VALUES( """ + self.quoteString(tablename) + u""",""" + self.quoteString(geom_column) + u""",
                          MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',0 , 20,0.005),
                          MDSYS.SDO_DIM_ELEMENT('Y',20 ,0, 0.005) ), """ + str(srid) + u""" )"""
        else:
            # nothing to do, you don't have sufficient rights...
            pass

        self._execute_and_commit(sql)

    def deleteGeometryColumn(self, table, geom_column):
        return self.deleteTableColumn(table, geom_column)

    def addTableUniqueConstraint(self, table, column):
        """ add a unique constraint to a table """
        sql = u"ALTER TABLE %s ADD UNIQUE (%s)" % (
            self.quoteId(table), self.quoteId(column))
        self._execute_and_commit(sql)

    def deleteTableConstraint(self, table, constraint):
        """ delete constraint in a table """
        sql = u"ALTER TABLE %s DROP CONSTRAINT %s" % (
            self.quoteId(table), self.quoteId(constraint))
        self._execute_and_commit(sql)

    def addTablePrimaryKey(self, table, column):
        """ add a primery key (with one column) to a table """
        sql = u"ALTER TABLE %s ADD PRIMARY KEY (%s)" % (
            self.quoteId(table), self.quoteId(column))
        self._execute_and_commit(sql)

    def createTableIndex(self, table, name, column):
        """ create index on one column using default options """
        sql = u"CREATE INDEX %s ON %s (%s)" % (
            self.quoteId(name), self.quoteId(table), self.quoteId(column))
        self._execute_and_commit(sql)

    def deleteTableIndex(self, table, name):
        schema, tablename = self.getSchemaTableName(table)
        sql = u"DROP INDEX %s" % self.quoteId((schema, name))
        self._execute_and_commit(sql)

    def createSpatialIndex(self, table, geom_column='GEOM'):
        schema, tablename = self.getSchemaTableName(table)
        idx_name = self.quoteId(u"sidx_%s_%s" % (tablename, geom_column))
        sql = u"CREATE INDEX %s ON %s(%s) INDEXTYPE IS MDSYS.SPATIAL_INDEX" % (
            idx_name, self.quoteId(table), self.quoteId(geom_column))
        self._execute_and_commit(sql)

    def deleteSpatialIndex(self, table, geom_column='GEOM'):
        schema, tablename = self.getSchemaTableName(table)
        idx_name = self.quoteId(u"sidx_%s_%s" % (tablename, geom_column))
        return self.dropTableIndex(table, idx_name)

    def execution_error_types(self):
        return QtSqlDB.ExecError

    def connection_error_types(self):
        return QtSqlDB.ConnectionError

    # moved into the parent class: DbConnector._execute()
    # def _execute(self, cursor, sql):
    #     pass

    # moved into the parent class: DbConnector._execute_and_commit()
    # def _execute_and_commit(self, sql):
    #     pass

    # moved into the parent class: DbConnector._get_cursor()
    # def _get_cursor(self, name=None):
    #     pass

    # moved into the parent class: DbConnector._fetchall()
    # def _fetchall(self, c):
    #     pass

    # moved into the parent class: DbConnector._fetchone()
    # def _fetchone(self, c):
    #     pass

    # moved into the parent class: DbConnector._commit()
    # def _commit(self):
    #     pass

    # moved into the parent class: DbConnector._rollback()
    # def _rollback(self):
    #     pass

    # moved into the parent class: DbConnector._get_cursor_columns()
    # def _get_cursor_columns(self, c):
    #     pass

    def getSqlDictionary(self):
        """ Returns the dictionnary for SQL dialog """
        from .sql_dictionary import getSqlDictionary
        sql_dict = getSqlDictionary()

        # get schemas, tables and field names
        items = []

        # First look into the cache if available
        if self.hasCache():
            sql = u"""SELECT DISTINCT tablename FROM "oracle_%s"
                                  UNION
                                  SELECT DISTINCT ownername FROM "oracle_%s" """ % (self.connName, self.connName)
            if self.userTablesOnly:
                sql = u"""SELECT DISTINCT tablename FROM "oracle_%s" WHERE ownername = '%s'
                                          UNION
                                          SELECT DISTINCT ownername FROM "oracle_%s" WHERE ownername = '%s' """ % (self.connName, self.user, self.connName, self.user)

            c = self.cache_connection.cursor()
            c.execute(sql)
            for row in c.fetchall():
                items.append(row[0])
            c.close()

        if self.hasCache():
            if self.userTablesOnly:
                sql = u"""SELECT DISTINCT COLUMN_NAME FROM USER_TAB_COLUMNS"""
            else:
                sql = u"""SELECT DISTINCT COLUMN_NAME FROM ALL_TAB_COLUMNS"""
        elif self.userTablesOnly:
            sql = u"""SELECT DISTINCT TABLE_NAME FROM USER_ALL_TABLES
                                  UNION
                                  SELECT USER FROM DUAL
                                  UNION
                                  SELECT DISTINCT COLUMN_NAME FROM USER_TAB_COLUMNS"""
        else:
            sql = u"""SELECT TABLE_NAME FROM ALL_ALL_TABLES
                                  UNION
                                  SELECT DISTINCT OWNER FROM ALL_ALL_TABLES
                                  UNION
                                  SELECT DISTINCT COLUMN_NAME FROM ALL_TAB_COLUMNS"""

        c = self._execute(None, sql)
        for row in self._fetchall(c):
            items.append(row[0])
        c.close()

        sql_dict["identifier"] = items
        return sql_dict
