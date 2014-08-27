# Oracle implementation of QGis DBManager plugin

## Introduction

This Python code try to implement the Oracle part of the QGis DBManager plugin. DBManager plugin is a good tool from QGis with which you can easily manage your databases and create your own queries which can be dynamically added to QGis maps.

For the moment, DBManager plugin is only able to connect to PostGIS and Spatialite databases. If you want to manage your Oracle Spatial repository, you can (try) to do this with this code implementation.

The code base of this implementation was the Postgis one. I tried to make every features of the PostGIS work under Oracle but there are some limitations. Read TODO.md to have more details about what is working and what needs to be done.

Expect bugs !


## Installation

The code needs [cx_Oracle](http://cx-oracle.sourceforge.net/) Python library to work. For the moment it is not included in QGis binary installation, so you have to install it first.

If you are running a GNU/Linux distribution and have installed QGis from the official repositories, you'll have to install Oracle Client and SDK and then install cx_Oracle. [Read this](https://stackoverflow.com/a/9859027)...

If you are running QGis on MS-Windows, you have to [download](https://pypi.python.org/pypi/cx_Oracle/5.1.3) the good version of cx_Oracle binaries (32 or 64 bits) and extract and copy cx_Oracle.pyd file in C:\Program Files\QGis Chugiak/apps/Python2.7/DLL .

Once cx_Oracle is correctly installed, you just have to copy the entire oracle directory in the db_plugins directory of the db_manager installation. (C:\Program Files\QGis Chugiak/apps/qgis/python/plugins/db_manager/db_plugins under MS-Windows).


## Limitations

* You have to define Oracle connections directly in QGis for the plugin to work (same thing than PostGIS and Spatialite).
* Oracle Spatial Rasters are not supported (as I've don't have a way to test them).
* The code try to use the maximum of your Oracle connections parameters. If you have a huge geographic database with a lot of layers, listing tables can take time. So be careful about your connections parameters (try to restrict to user tables to reduce internal queries duration).
* Tests have been done with QGis 2.4 only. You probably should use this version (and upper ones) because before 2.4 the Oracle provider of QGis was not able to load dynamic queries.
* Some things could not have been well tested, particulary everything that requires administrative rights on DB like schema creation/deletion.
* Tests have been done againts an Oracle 10g database. I tried to incorporate the official Oracle 12c "dictionnary" of commands and the internal queries should also work with 11g and 12c versions of Oracle Database server.
* Some tasks cannot been done under Oracle Database like moving a table from a schema to another. There is also no PostgreSQL Rules features under Oracle.
* Code has been tested only under MS-Windows (bad) but as it is Python code, I hope it will also works under other OS.


## Bug reports

For the moment, use the ["issues" tool of GitHub](https://github.com/medspx/dbmanager-oracle/issues) to report bugs. 


## Main goal

My main goal is that this code can be incorporated in the official QGis source code repository. Once this has been done, the code upgrades will take place there.


## License

This code is released under the GNU GPLv2 license. Read headers code for more information.