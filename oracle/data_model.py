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

from ..data_model import TableDataModel, SqlResultModel, BaseTableModel
from ..plugin import BaseError
from qgis.core import *


class ORTableDataModel(TableDataModel):

    def __init__(self, table, parent=None):
        self.cursor = None
        TableDataModel.__init__(self, table, parent)

        if self.table.rowCount == None:
            self.table.refreshRowCount()
            if self.table.rowCount == None:
                return

        self.connect(self.table, SIGNAL("aboutToChange"), self._deleteCursor)
        self._createCursor()

    def _createCursor(self):
        fields_txt = u", ".join(self.fields)
        table_txt = self.db.quoteId(
            (self.table.schemaName(), self.table.name))

        self.cursor = self.db._get_cursor()
        sql = u"SELECT %s FROM %s" % (fields_txt, table_txt)

        self.db._execute(self.cursor, sql)

    def _sanitizeTableField(self, field):
        # get fields, ignore geometry columns
        # TODO: return the geometry type of the table
        if field.dataType.lower() == "sdo_geometry":
            return u"CASE WHEN %(fld)s IS NULL THEN NULL ELSE 'GEOMETRY' END AS %(fld)s" % {'fld': self.db.quoteId(field.name)}
        if field.dataType.lower() == "date":
            return u"CAST(%s AS VARCHAR2(8))" % self.db.quoteId(field.name)

        return u"CAST(%s As VARCHAR2(%s))" % (self.db.quoteId(field.name), field.charMaxLen)

    def _deleteCursor(self):
        self.db._close_cursor(self.cursor)
        self.cursor = None

    def __del__(self):
        self.disconnect(
            self.table, SIGNAL("aboutToChange"), self._deleteCursor)
        self._deleteCursor()
        pass  # print "PGTableModel.__del__"

    def fetchMoreData(self, row_start):
        if not self.cursor:
            self._createCursor()

        # try:
        #	self.cursor.scroll(row_start, mode='absolute')
        # except self.db.error_types():
        #	self._deleteCursor()
        #	return self.fetchMoreData(row_start)

        self.resdata = self.cursor.fetchmany(self.fetchedCount)
        self.fetchedFrom = row_start


class ORSqlResultModel(SqlResultModel):

    def __init__(self, db, sql, parent=None):
        self.db = db.connector

        t = QTime()
        t.start()
        c = self.db._execute(None, unicode(sql))

        self._affectedRows = 0
        data = []
        header = self.db._get_cursor_columns(c)
        if header == None:
            header = []

        try:
            if len(header) > 0:
                data = self.db._fetchall(c)
            self._affectedRows = c.rowcount
        except DbError:
            # nothing to fetch!
            data = []
            header = []

        self._secs = t.elapsed() / 1000.0
        del t

        BaseTableModel.__init__(self, header, data, parent)

        # commit before closing the cursor to make sure that the changes are
        # stored
        self.db._commit()
        c.close()
        del c
