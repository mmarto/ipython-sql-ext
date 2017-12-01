"""
Author: mhristov
Source: readSqlExt.py
Date: 02/11/17 15:20

Purpose: IPython extension for working with databases.

Provides magic functions for various db related tasks like:
    1. Running and explaining sqls.
    2. Finding and getting info about tables, columns, indexes.
    3. Finding and getting info about plsql packages, functions and procedures.
    4. Table statistics.
    5. Getting plsql packages/views source.

Doc: %helpsql
"""

import os
import sys
from cx_Oracle import DatabaseError
from sqlalchemy import exc
import pydoc

sys.path.append(os.path.join(os.environ['HOME'], 'python_lib'))
import db_utilities as dbu

default_db_alias = 'oradb'
mysql_schema = None
engine = dbu.getDbConnection(default_db_alias, schema=mysql_schema, asEngine=True)


def parse_line(line, usage_fn):
    """
    Function for parsing the line parameter of a magic function
    :param line:
    :param usage_fn:
    :return:
    """
    line = line.strip()
    args = [a for a in line.split(' ') if a != '']
    status = True
    table_name = None
    alias = None
    # print(args)
    if len(line) == 0:
        usage_fn()
        status = False
    elif len(args) == 1:
        table_name = line
        alias = default_db_alias
    elif len(args) == 2:
        table_name = args[0]
        alias = args[1]
    else:
        usage_fn()
        status = False

    return status, table_name, alias


def setDefaultDbAlias(line):
    """
    Extension function to set a default db alias
    :param db_alias:
    :param mysql_schema:
    :return:
    """
    global default_db_alias
    global mysql_schema
    global engine

    line = line.strip()
    args = [a for a in line.split(' ') if a != '']
    if len(line) == 0:
        print('Default DB Alias: {}'.format(default_db_alias))
        if mysql_schema is not None:
            print('Mysql Schema: {}'.format(mysql_schema))
        return
    elif len(args) == 1:
        default_db_alias = str(args[0]).upper()
    elif len(args) == 2:
        default_db_alias = str(args[0]).upper()
        mysql_schema = str(args[1])

    # if default_db_alias.startswith('MYSQL') and mysql_schema is None:
    #     default_db_alias = 'oradb'
    #     print('MYSQL alias requires setting db schema.\nPlease set db schema ex: %setDefaultDbAlias MYSQLDEV clams')
    #     return

    aliases = dbu.getDbAliases(asDataFrame=True)

    if default_db_alias not in aliases[0].tolist():
        print('DB Alias {} not in config.'.format(default_db_alias))
        default_db_alias = 'oradb'
        return

    print('Default DB Alias set to {}'.format(default_db_alias))
    if default_db_alias is not None:
        print('Mysql Schema: {}'.format(mysql_schema))

    engine = dbu.getDbConnection(default_db_alias, schema=mysql_schema, asEngine=True)


def helpsql(line):
    """
    IPython extension function for getting documentaion on sql related functions in readSqlExt extension.
    :return: None
    """
    module_ = pydoc.importfile(__file__)
    pydoc.doc(module_)


def getDbObjectSource(line):
    """
    Ipython extension function to get the source of an oracle db object
    Usage: %getDbObjectSource object_name [object_type (default: TABLE)] [db_alias (default: oradb)]
    :param object_name: Name of the package
    :param db_alias: db alias
    :return: None
    """

    def usage():
        print('Usage: %getDbObjectSource object_name [object_type (default: TABLE)] [db_alias (default: {})]'.format(default_db_alias))

    line = line.strip()

    if len(line) == 0:
        usage()
        return
    elif len(line.split(' ')) == 1:
        object_name = line
        object_type = 'TABLE'
        alias = default_db_alias
    elif len(line.split(' ')) == 2:
        args = line.split(' ')
        object_name = args[0]
        object_type = args[1]
        alias = default_db_alias
    elif len(line.split(' ')) == 3:
        args = line.split(' ')
        object_name = args[0]
        object_type = args[1]
        alias = args[2]
    else:
        usage()
        return

    try:
        source = dbu.getDbObjectSource(object_name, object_type, alias)
        return source
    except DatabaseError as e:
        print('{}'.format(e))


def findFunction(line):
    """
    IPython extension function to find a plsql function within a package for a given schema
    Note: Standalone functions are not included
    Usage: %findFunction function_name [db_alias (default: oradb)]
    :param function_name:
    :param alias:
    :return: None
    """

    def usage():
        print('Usage: %findFunction function_name [db_alias (default: {})]'.format(default_db_alias))

    status, function_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.findFunction(function_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def getPackageFunctions(line):
    """
    Ipython extension function to get all functions for a package
    Usage: %getPackageFunctions package_name [db_alias (default: oradb)]
    :param package_name: Name of the package
    :param db_alias: db alias
    :return: None
    """

    def usage():
        print('Usage: %getPackageFunctions package_name [db_alias (default: {})]'.format(default_db_alias))

    status, package_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getPackageFunctions(package_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def getPackages(line):
    """
    Ipython extension function for getting db packages
    Usage: %getPackage package_name [db_alias (default: oradb)]
    :param package_name: Name of the package
    :param db_alias: db alias
    :return: None
    """

    def usage():
        print('Usage: %getPackage package_name [db_alias (default: {})]'.format(default_db_alias))

    status, package_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getPackages(package_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def gatherTableStats(line):
    """
    Ipython extension function for gathering table statistics
    Usage: %gatherTableStats table_name [owner (default: USER)] [db_alias (default: oradb)]
    :param table_name:
    :param owner:
    :param db_alias:
    :return:
    """

    def usage():
        print('Usage: %gatherTableStats table_name [owner (default: USER)] [db_alias (default: oradb)]')

    line = line.strip()

    if len(line) == 0:
        usage()
        return
    elif len(line.split(' ')) == 1:
        table_name = line
        owner = 'USER'
        alias = default_db_alias
    elif len(line.split(' ')) == 2:
        args = line.split(' ')
        table_name = args[0]
        owner = args[1]
        alias = default_db_alias
    elif len(line.split(' ')) == 3:
        args = line.split(' ')
        table_name = args[0]
        owner = args[1]
        alias = args[2]
    else:
        usage()
        return

    try:
        dbu.gatherTableStats(table_name, owner, alias)
    except DatabaseError as e:
        print('{}'.format(e))


def getTableStats(line):
    """
    Ipython extenstion function for getting table statistics
    Usage: %getTableStats table_name [db_alias (default: oradb)]
    :param table_name:
    :param db_alias:
    :return: None
    """

    def usage():
        print('Usage: %getTableStats table_name [db_alias (default: {})]'.format(default_db_alias))

    status, table_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getTableStats(table_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def getTables(line):
    """
    Ipython extenstion function for finding tables
    Usage: %getTables table_name [db_alias (default: oradb)]
    :param table_name:
    :param db_alias:
    :return: None
    """

    def usage():
        print('Usage: %getTables table_name [db_alias (default: {})]'.format(default_db_alias))

    status, table_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getTables(table_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def getTabColumns(line):
    """
    Ipython extenstion function for getting table columns
    Usage: %getTabColumns table_name [db_alias (default: oradb)]
    :param table_name: Name of table
    :param db_alias:
    :return:
    """

    def usage():
        print('Usage: %getTabColumns table_name [db_alias (default: {})]'.format(default_db_alias))

    status, table_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getTableColumns(table_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def findColumn(line):
    """
    Ipython extenstion function for searching for columns by name
    Usage: %findColumns column_name [db_alias (default: oradb)]
    :param column_name: Name of column
    :param db_alias:
    :return:
    """

    def usage():
        print('Usage: %findColumns column_name [db_alias (default: {})]'.format(default_db_alias))

    status, col_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.findColumns(col_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def getTabIndex(line):
    """
    Ipython extenstion function for getting table indeces
    :param table_name: Name of table
    :param db_alias:
    :return: None
    """

    def usage():
        print('Usage: %getTabIndex table_name [db_alias (default: {})]'.format(default_db_alias))

    status, table_name, alias = parse_line(line, usage)

    if status:
        try:
            dbu.getTableIndex(table_name, alias)
        except DatabaseError as e:
            print('{}'.format(e))


def read_sql(line, cell=None):
    """
    Ipython extension function for running sql statements
    Usage: %read_sql [dbAlias (default: oradb)] sql|sql_file
    :param db_alias:
    :param sql: sql statement, sql variable, sql file
    :return: pd.DataFrame
    """

    def usage():
        print('Usage: %read_sql [dbAlias (default: {})] sql|sql_file'.format(default_db_alias))

    # if 'default_db_alias' not in globals():
    #     print('Variable default_db_alias not set')
    # else:
    #     print('DB Alias: {}'.format(default_db_alias))

    line = line.strip()

    if len(line) == 0 and cell is None:
        usage()
        return

    if cell is None:  # line magic
        alias = default_db_alias
        if dbu.isSql(line):
            sql = line
        elif os.path.isfile(line):
            print('File: {}'.format(line))
            with open(line) as f:
                sql = f.read()
        else:
            args = line.split(' ')
            alias = args[0].strip()
            sql = ' '.join(args[1:])

            if os.path.isfile(sql):
                print('File: {}'.format(sql))
                with open(sql) as f:
                    sql = f.read()
        print('DB Alias: {}'.format(alias))
        if mysql_schema is not None:
            print('Mysql Schema: {}'.format(mysql_schema))
        try:
            df = dbu.readSql(sql, con=engine)
            return df
        except exc.DatabaseError as e:
            print('{}'.format(e))

    else:
        if len(line) == 0:
            alias = default_db_alias
        else:
            alias = line

        print('DB Alias: {}'.format(alias))

        if cell is not None:
            try:
                df = dbu.readSql(cell, con=engine)
                return df
            except exc.DatabaseError as e:
                print('{}'.format(e))
        return


def explain_sql(line, cell=None):
    """
    Ipython extension for explaining sql statements
    Usage: %explain_sql [dbAlias (default: oradb)] sql
    :param dbAlias:
    :param sql: sql statement
    :return: None
    """

    def usage():
        print('Usage: %explain_sql [dbAlias (default: {})] sql'.format(default_db_alias))

    line = line.strip()

    if len(line) == 0 and cell is None:
        usage()
        return

    if cell is None:  # line magic

        if dbu.isSql(line):
            alias = default_db_alias
            sql = line
        else:
            args = line.split(' ')
            alias = args[0].strip()
            sql = ' '.join(args[1:])
        try:
            dbu.explainSQL(sql, alias.upper())
        except Exception as e:
            print('{}'.format(e))
        return

    else:
        if len(line) == 0:
            alias = default_db_alias
        else:
            alias = line

        if cell is not None:
            try:
                dbu.explainSQL(cell, alias)
            except DatabaseError as e:
                print('{}'.format(e))
            return
        else:
            return


def getDbAliases(line):
    """
    IPython extension function for get a list of available db aliases.
    Usage: %getDbAliases [filter]
    :param filter: filter alias by string
    :return: None
    """
    line = line.strip()
    dbu.getDbAliases(line)


def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(setDefaultDbAlias, 'line', magic_name='setDefaultDbAlias')
    ipython.register_magic_function(read_sql, 'line_cell', magic_name='read_sql')
    ipython.register_magic_function(explain_sql, 'line_cell', magic_name='explain_sql')
    ipython.register_magic_function(getTables, 'line', magic_name='getTables')
    ipython.register_magic_function(getTableStats, 'line', magic_name='getTableStats')
    ipython.register_magic_function(getTabColumns, 'line', magic_name='getTabColumns')
    ipython.register_magic_function(findColumn, 'line', magic_name='findColumn')
    ipython.register_magic_function(getTabIndex, 'line', magic_name='getTabIndex')
    ipython.register_magic_function(gatherTableStats, 'line', magic_name='gatherTableStats')
    ipython.register_magic_function(getPackages, 'line', magic_name='getPackages')
    ipython.register_magic_function(getPackageFunctions, 'line', magic_name='getPackageFunctions')
    ipython.register_magic_function(findFunction, 'line', magic_name='findFunction')
    ipython.register_magic_function(getDbObjectSource, 'line', magic_name='getDbObjectSource')
    ipython.register_magic_function(helpsql, 'line', magic_name='helpsql')
    ipython.register_magic_function(getDbAliases, 'line', magic_name='getDbAliases')

# # Uncomment if ever need to unload the extension
# def unload_ipython_extension(ipython):
#     pass
