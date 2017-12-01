#!/usr/local/python-3.4.1/bin/python3
"""
Author: mhristov
Source: db_utilities.py
Date: 20/11/17 17:54

Purpose:
"""

import os
import sys
import re
import datetime
import tables
import pandas as pd
import sqlalchemy as sa
import cx_Oracle
import mysql.connector
from functools import wraps
from time import time, gmtime, strftime
from textwrap import dedent, indent

sys.path.append(os.path.join(os.environ['HOME'], 'python_lib'))

import logger


class DupColsRenamer():
    def __init__(self):
        self.d = dict()

    def __call__(self, x):
        if x not in self.d:
            self.d[x] = 0
            return x
        else:
            self.d[x] += 1
            return "{}_{}".format(x, self.d[x])


def isSql(sql):
    if sql.lstrip().lower().startswith('select') and 'from' in sql.lower():
        return True
    else:
        return False


def getDbCredentials(dbalias, asEngineStr=False):
    """Function to get database credentials from a config file ~/config/.dbaccess for a database alias
    If asEngineStr is true returns sqlalchemy engine connection string"""
    dbalias = dbalias.upper()
    config_file = os.environ['HOME'] + "/config/.dbaccess"
    file = open(config_file)
    lines = file.readlines()
    data = []
    mysqlConfig = dict()
    for line in lines:
        if line.startswith('#'): continue
        m = re.match("^{0}\|".format(dbalias), line)
        if m is None:
            continue
        data = line.strip().split('|')
        schema = data[1].upper()
        port = data[2]
        user = data[3]
        passwd = data[4]
        # for mysql only
        mysqlConfig['user'] = user
        mysqlConfig['password'] = passwd
        mysqlConfig['host'] = schema
        mysqlConfig['port'] = port
    if data:
        if dbalias.startswith('ORA'):
            if asEngineStr:
                return 'oracle://{0}:{1}@{2}'.format(user, passwd, schema)
            else:
                return '{0}/{1}@{2}'.format(user, passwd, schema)
        elif dbalias.startswith('MYSQL'):
            if asEngineStr:
                return 'mysql+mysqlconnector://{0}:{1}@{2}:{3}'.format(mysqlConfig['user'], mysqlConfig['password'],
                                                                       mysqlConfig['host'], mysqlConfig['port'])
            else:
                return mysqlConfig
    else:
        return "X"


def getDbConnection(dbAlias, schema=None, asEngine=False, echo=False):
    """Returns connection object based on dbAlias.
    Schema argument is only applicable for mysql connections.
    If asEngine argument is set to True returns sqlalchemy engine.
    If echo is set to True makes the engine in echo mode

    Usage: conn = getDbConnection('ORADEVIBCUST')
    """

    conn = None
    dbCredentials = getDbCredentials(dbAlias)
    engineEcho = False

    if asEngine:
        if echo:
            engineEcho = True
    if dbCredentials != 'X':
        if dbAlias.startswith('ORA'):
            if asEngine:
                connStr = 'oracle://{}'.format(dbCredentials.replace('/', ':'))
                conn = sa.create_engine(connStr, echo=engineEcho)
            else:
                conn = cx_Oracle.connect(dbCredentials)
        elif dbAlias.startswith('MYSQL'):
            if asEngine:
                dbCredentials['allow_local_infile'] = True
                if schema is not None:
                    dbCredentials['schema'] = schema

                    connStr = 'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{schema}'.format(**dbCredentials)
                else:
                    connStr = 'mysql+mysqlconnector://{user}:{password}@{host}:{port}'.format(**dbCredentials)
                conn = sa.create_engine(connStr, echo=engineEcho)
            else:
                conn = mysql.connector.connect(database=schema, **dbCredentials)
    else:
        # print('Error: dbAlias {} not valid!'.format(dbAlias))
        raise Exception('Error: dbAlias {} not valid!'.format(dbAlias))
    return conn


def format_size(size, decimal_places):
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return "{size:.{decimal_places}f} {unit}".format(size=size, decimal_places=decimal_places, unit=unit)


def df_size(df):
    """Returns the size of a DataFrame (Index included) in human readable format"""
    total = 0.0
    for col in df:
        total += df[col].nbytes
    total += df.index.nbytes
    return format_size(total, 1)


def logging_decorator(fn):
    @wraps(fn)  # using wraps to be able to preserve the docstring of the method
    def logging(*args, **kwargs):
        if 'logger' in kwargs and kwargs['logger'] is not None:
            logger = kwargs['logger']
        else:
            logger = logger

        if 'logger' in kwargs:
            del(kwargs['logger'])

        if fn.__name__ == 'readSql':
            logger.info('Read sql:\n{}'.format(kwargs['sql'] if 'sql' in kwargs else args[0]))
        elif fn.__name__ == 'readCsv':
            logger.info('Read file: {}'.format(kwargs['filepath_or_buffer'] if 'filepath_or_buffer' in kwargs else args[0]))

        if 'params' in kwargs:
            logger.info('params: {}'.format(kwargs['params']))
        start = time()
        df = fn(*args, **kwargs)
        end = time() - start
        elapsed = strftime('%H:%M:%S', gmtime(end))

        if type(df) == pd.DataFrame:
            df = df.rename(columns=DupColsRenamer())
            logger.info(indent(dedent('''
            Rows fetched: {}
            Memory usage: {}
            Elapsed time: {}'''.format(len(df), df_size(df), elapsed)), '    '))

        return df

    return logging


def getDbObjectSource(name, object_type='TABLE', alias='oradb'):
    """
    Funtion to get oracle db object source
    :param name:
    :param object_type:
    :param alias:
    :return:
    """

    name = name.upper()
    object_type = object_type.upper()
    alias = alias.upper()

    conn = getDbConnection(alias)
    cur = conn.cursor()

    r = cur.execute('''select dbms_metadata.get_ddl('{}', '{}') c from dual'''.format(object_type, name))
    r = r.fetchone()
    r = r[0].read()
    conn.close()
    return r


def getPackages(package_name, alias='oradb'):
    """
    Function to search for PLSQL packages in a given oracle db
    :param package_name:
    :param alias:
    :return:
    """
    package_name = package_name.upper()
    alias = alias.upper()

    conn = getDbConnection(alias)
    cur = conn.cursor()

    sql = '''
    select distinct owner, object_name 
    from all_procedures 
    where object_type = 'PACKAGE' and owner not like '%SYS' and owner not like 'XDB' 
    and object_name like '%{}%'
    '''.format(package_name)
    config = tables.Config(border=True)
    r = cur.execute(sql)
    columns = [tables.Column(c[0]) for c in r.description]
    table = tables.Table(config, columns)

    for row in r:
        table.addRow((row[0], row[1].lower()))

    print(table.asString())
    conn.close()


def findFunction(function_name, alias='oradb'):
    """
    Function to find a plsql function in a oracle schema
    :param function_name:
    :param alias:
    :return:
    """
    function_name = function_name.upper()
    alias = alias.upper()

    engine = getDbConnection(alias, asEngine=True)
    conn = engine.connect()

    sql = '''
        select p.owner, p.object_name, p.PROCEDURE_NAME, a.POSITION, a.ARGUMENT_NAME||' '||a.IN_OUT||' '||a.DATA_TYPE arg
        from all_procedures p, all_arguments a
        where p.procedure_name = a.object_name(+)
        and p.object_name = a.package_name
        and p.OWNER not like '%SYS' and p.OWNER  not like 'XDB' 
        and p.object_type = 'PACKAGE'
        and p.PROCEDURE_NAME like '%{}%'
        and a.ARGUMENT_NAME is not null
        order by 3
        '''.format(function_name)

    config = tables.Config(border=True)
    df = pd.read_sql(sql, conn)
    if len(df) == 0:
        return
    df = df.pivot_table(index=['owner', 'object_name', 'procedure_name'], columns=['position'], values=['arg'],
                        aggfunc='first')
    # del(df[0])
    df = df.apply(lambda x: ', '.join([xx for xx in x if xx is not None]), axis=1)
    df = df.reset_index()
    df[0] = '(' + df[0] + ')'
    df.rename(columns={0: 'arguments', 'object_name': 'package_name'}, inplace=True)
    columns = [tables.Column(c) for c in df.columns]
    table = tables.Table(config, columns)

    for row in df.to_records(index=False):
        table.addRow([r.lower() for r in row])

    print(table.asString())
    conn.close()


def getPackageFunctions(package_name, alias='oradb'):
    """
    Function to get all functions for a given package
    :param package_name:
    :param alias:
    :return:
    """
    package_name = package_name.upper()
    alias = alias.upper()

    engine = getDbConnection(alias, asEngine=True)
    conn = engine.connect()

    sql = '''
    select p.owner, p.object_name, p.PROCEDURE_NAME, a.POSITION, a.ARGUMENT_NAME||' '||a.IN_OUT||' '||a.DATA_TYPE arg
    from all_procedures p, all_arguments a
    where p.procedure_name = a.object_name(+)
    and p.object_name = a.package_name
    and p.OWNER not like '%SYS' and p.OWNER  not like 'XDB' 
    and p.object_type = 'PACKAGE' 
    and p.OBJECT_NAME = '{}'
    and a.ARGUMENT_NAME is not null
    order by 3
    '''.format(package_name)

    config = tables.Config(border=True)
    df = pd.read_sql(sql, conn)
    if len(df) == 0:
        return
    df = df.pivot_table(index=['owner', 'object_name', 'procedure_name'], columns=['position'], values=['arg'],
                        aggfunc='first')
    # del(df[0])
    df = df.apply(lambda x: ', '.join([xx for xx in x if xx is not None]), axis=1)
    df = df.reset_index()
    df[0] = '(' + df[0] + ')'
    df.rename(columns={0: 'arguments', 'object_name': 'package_name'}, inplace=True)
    columns = [tables.Column(c) for c in df.columns]
    table = tables.Table(config, columns)

    for row in df.to_records(index=False):
        table.addRow([r.lower() for r in row])

    print(table.asString())
    conn.close()


def explainSQL(sql, alias='oradb'):
    """
    Function running sql explaing plan against oracle/mysql sqls
    http://websrv3.prod.ibkr-int.com/twiki/bin/view/SoftwareDev/DBAQueryTuning
    :param sql: sql statement
    :param alias: db alias
    """
    conn = getDbConnection(alias)
    cur = conn.cursor()
    if alias.upper().startswith('ORA'):
        cur.execute('explain plan for {}'.format(sql))
        r = cur.execute('select * from table(dbms_xplan.display)')
        for l in r.fetchall():
            print(l[0])
    elif alias.upper().startswith('MYSQL'):
        config = tables.Config(border=True)
        cur.execute('explain extended {}'.format(sql))
        columns = [tables.Column(c) for c in cur.column_names]
        table = tables.Table(config, columns)
        for row in cur:
            table.addRow(row)
        print(table.asString())
    conn.close()


def gatherTableStats(table_name, owner=None, alias='oradb', logger=logger):
    """
    Function calling dbms_stats.gather_table_stats oracle procedure
    http://websrv3.prod.ibkr-int.com/twiki/bin/view/SoftwareDev/DBAQueryTuning
    :param table_name:
    :param owner: schema
    :param alias: Db alias
    :param logger: logger
    :return: None
    """
    table_name = table_name.upper()
    owner = owner.upper()
    logger.info('Start gathering statistics for table {}'.format(table_name))
    engine = getDbConnection(alias, asEngine=True)
    conn = engine.connect()
    if not owner:
        sql = '''begin dbms_stats.gather_table_stats(USER, '{}', estimate_percent=>100, cascade=>true); end;'''.format(table_name)
    else:
        sql = '''begin dbms_stats.gather_table_stats('{}', '{}', estimate_percent=>100, cascade=>true); end;'''.format(owner, table_name)
    logger.info(sql)
    conn.execute(sql)
    logger.info('End.')
    conn.close()


def getTableStats(table_name, alias='oradb'):
    """
    Function to get statistics on a oracle table
    :param table_name:
    :param alias: Db alias
    """
    table_name = table_name.upper()
    alias = alias.upper()
    engine = getDbConnection(alias, asEngine=True)
    conn = engine.connect()

    if alias.startswith('ORA'):
        tableStatsSql = '''
        select owner, table_name, 
        --partition_name, 
        object_type, num_rows, avg_row_len, last_analyzed
        from all_tab_statistics where TABLE_NAME = :tbl'''

        df = pd.read_sql(tableStatsSql, conn, params={'tbl': table_name})
        if len(df) == 0:
            print('Table {} not found in all_tab_statistics!'.format(table_name))
            return
        num_rows = df.num_rows[0]
        df.columns = df.columns.str.replace('_', ' ').str.title()
        config = tables.Config(border=True)
        columns = [tables.Column('Property'), tables.Column('Value')]
        table = tables.Table(config, columns)
        for rec in df.T.to_records(convert_datetime64=True):
            table.addRow(rec)
        print(table.asString())

        tableColsStatsSql = '''
        select c.column_id, s.COLUMN_NAME, s.NUM_DISTINCT, s.NUM_NULLS, s.AVG_COL_LEN 
        from all_tab_col_statistics s, all_tab_cols c
        where s.TABLE_NAME = :tbl 
        and s.table_name = c.table_name
        and s.column_name = c.column_name
        and s.column_name not like 'SYS_%'
        '''

        df = pd.read_sql(tableColsStatsSql, conn, params={'tbl': table_name}, index_col='column_id')
        df.insert(1, 'num_rows', num_rows)
        df.insert(3, 'not_nulls', df.num_rows - df.num_nulls)
        df.columns = df.columns.str.replace('_', ' ').str.title()
        df.sort_index(ascending=True, inplace=True)
        columns = [tables.Column(c) for c in df.columns]
        table = tables.Table(config, columns)
        for rec in df.to_records(index=False):
            table.addRow(rec)
        print(table.asString())

    conn.close()


@logging_decorator
def readSql(*args, **kwargs):
    """Wrapper around pandas.read_sql function with added logging decorator"""
    df = pd.read_sql(*args, **kwargs)
    return df


@logging_decorator
def readCsv(*args, **kwargs):
    """Wrapper around pandas.read_csv function with added logging decorator"""
    df = pd.read_csv(*args, **kwargs)
    return df


def findColumns(col_name, alias='oradb'):

    alias = alias.upper()
    conn = getDbConnection(alias)

    if alias.startswith('ORA'):
        sql = '''select owner, table_name, column_name, NULLABLE, data_type, DATA_LENGTH 
                 from all_tab_columns where column_name like upper('%'||:col||'%') 
                 order by owner, table_name, column_id'''
        cur = conn.cursor()
        cur.execute(sql, {'col': col_name})
    elif alias.startswith('MYSQL'):
        sql = '''select table_schema owner, table_name, column_name, is_nullable, column_type, null data_length 
                  from information_schema.columns where column_name like upper(concat('%', %s, '%'))'''
        cur = conn.cursor()
        cur.execute(sql, (col_name,))

    config = tables.Config(border=True)
    columns = list()
    columns.append(tables.Column('Schema'))
    columns.append(tables.Column('Table Name'))
    columns.append(tables.Column('Column Name'))
    columns.append(tables.Column('Null?'))
    columns.append(tables.Column('Data Type'))

    table = tables.Table(config, columns)

    for row in cur.fetchall():
        schemaName, tableName, columnName, notNull, columnType, columnSize = row
        if alias.startswith('ORA'):
            columnTypeSize = '{}({})'.format(columnType, columnSize)
            if notNull == 'N':
                notNull = 'NOT NULL'
            else:
                notNull = ''
        elif alias.startswith('MYSQL'):
            columnTypeSize = columnType
            if notNull == 'NO':
                notNull = 'NOT NULL'
            else:
                notNull = ''
        table.addRow((schemaName, tableName, columnName, notNull, columnTypeSize))

    print(table.asString())
    conn.close()


def getTableColumns(table_name, alias='oradb'):

    alias = alias.upper()
    conn = getDbConnection(alias)

    if alias.startswith('ORA'):
        sql = '''select table_name, column_name, NULLABLE, data_type, DATA_LENGTH, LAST_ANALYZED
        from all_tab_columns where table_name like upper(:tab) order by table_name, column_id'''
        cur = conn.cursor()
        cur.execute(sql, {'tab': table_name})
    elif alias.startswith('MYSQL'):
        sql = '''select table_name, column_name, is_nullable, column_type, null data_length, null last_analyzed 
        from information_schema.columns where table_name like upper(%s)'''
        cur = conn.cursor()
        cur.execute(sql, (table_name,))

    config = tables.Config(border=True)
    columns = list()
    columns.append(tables.Column('Table Name'))
    columns.append(tables.Column('Column Name'))
    columns.append(tables.Column('Null?'))
    columns.append(tables.Column('Data Type'))
    columns.append(tables.Column('Last Analyzed'))

    table = tables.Table(config, columns)

    for row in cur.fetchall():
        tableName, columnName, notNull, columnType, columnSize, lastAnalayzed = row
        if alias.startswith('ORA'):
            columnTypeSize = '{}({})'.format(columnType, columnSize)
            if notNull == 'N':
                notNull = 'NOT NULL'
            else:
                notNull = ''
        elif alias.startswith('MYSQL'):
            columnTypeSize = columnType
            if notNull == 'NO':
                notNull = 'NOT NULL'
            else:
                notNull = ''
        table.addRow((tableName, columnName, notNull, columnTypeSize, lastAnalayzed))

    print(table.asString())
    conn.close()


def getTableIndex(table_name, alias='oradb'):
    alias = alias.upper()
    conn = getDbConnection(alias)

    if alias.startswith('ORA'):
        sql = '''
        select a.table_name, a.index_name, a.column_name, a.column_position, b.index_type, b.status, b.last_analyzed
        from all_ind_columns a, all_indexes b
        where a.table_name = upper(:tab)
        and a.index_name = b.index_name
        order by a.table_owner,a.index_name,a.column_position
        '''
        cur = conn.cursor()
        cur.execute(sql, {'tab': table_name})
    elif alias.startswith('MYSQL'):
        sql = '''select table_name, index_name, column_name, null column_position, index_type, null status, null last_analyzed 
        from information_schema.statistics where table_name like upper(%s)'''
        cur = conn.cursor()
        cur.execute(sql, (table_name,))

    config = tables.Config(border=True)
    columns = []
    columns.append(tables.Column('Table Name'))
    columns.append(tables.Column('Index Name'))
    columns.append(tables.Column('Column Name'))
    columns.append(tables.Column('Column Position'))
    columns.append(tables.Column('Index Type'))
    columns.append(tables.Column('Status'))
    columns.append(tables.Column('Last Analyzed'))

    table = tables.Table(config, columns)

    for row in cur.fetchall():
        table.addRow(row)

    print(table.asString())
    conn.close()


def getTables(table_name, alias='oradb'):
    tableName = '%{}%'.format(table_name)
    alias = alias.upper()
    conn = getDbConnection(alias)

    if alias.startswith('ORA'):
        sql = '''select table_name, owner, last_analyzed from all_tables where table_name like upper(:tab) order by 1'''
        cur = conn.cursor()
        cur.execute(sql, {'tab': tableName})
    elif alias.startswith('MYSQL'):
        sql = '''select table_name, table_schema, update_time from information_schema.tables where table_name like upper(%s) order by 1'''
        cur = conn.cursor()
        cur.execute(sql, (tableName,))

    config = tables.Config(border=True)
    columns = list()
    columns.append(tables.Column('Table Name'))
    columns.append(tables.Column('Owner'))
    columns.append(tables.Column('Last Analyzed'))

    table = tables.Table(config, columns)

    for row in cur.fetchall():
        table.addRow(row)

    print(table.asString())
    conn.close()


def getDbAliases(filter_=None, asDataFrame=False):
    file = os.environ['HOME'] + '/config/.dbaccess'
    df = pd.read_csv(file, sep='|', skiprows=5, usecols=[0, 1, 2, 3], header=None, comment='#')

    if filter_:
        filter_ = str(filter_)
        df = df[df[0].str.contains(filter_.upper())]
    df = df.fillna('')
    if asDataFrame:
        return df
    else:
        config = tables.Config(border=True)
        columns = [tables.Column('Alias'), tables.Column('Details')]
        table = tables.Table(config, columns)

        for row in df.to_records(index=False):
            alias = row[0]
            details = '{}@{}:{}'.format(row[3], row[1], row[2])
            if details.endswith(':'):
                details = details[:-1]
            if details.endswith('.0'):
                details = details[:-2]
            table.addRow([alias, details])
        print(table.asString())
