import sys
import configparser
import sqlalchemy as sa
import pandas as pd
import pathlib
import base64
from tabulate import tabulate

db_config = pathlib.Path().home() / 'config' / '.dbaccess.cfg'


def fetch_data(sql, engine, params={}):
    # conn = engine.connect()
    sql = sa.text(sql)
    res = engine.execute(sql, params)
    # conn.close()
    return res


def print_tabular_data(df):
    df.columns = df.columns.str.replace('_', ' ').str.title()
    print(tabulate(df.to_dict('records'), headers='keys', tablefmt='psql'))


def sql2df(sql, engine, params={}, print_result=False):
    res = fetch_data(sql, engine, params)
    if res.returns_rows:
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
        if print_result:
            print_tabular_data(df.fillna('-'))
        else:
            return df
    else:
        print('Empty result set.')


def get_db_aliases(filter_=None, as_dataframe=False):
    config = configparser.ConfigParser()
    config.read(db_config)
    print(db_config)
    dfs = list()
    for k in config.keys():
        if k == 'DEFAULT': continue
        db = config[k]['db'] if 'db' in config[k] else None
        host = config[k]['host'] if 'host' in config[k] else None
        username = config[k]['username'] if 'username' in config[k] else None
        dfs.append(pd.DataFrame([[k, db, host, username]], columns=['alias', 'db', 'host', 'username']))

    df = pd.concat(dfs)

    if filter_ is not None:
        df = df[df.alias.str.contains(filter_.upper())]

    if as_dataframe:
        return df
    else:
        print_tabular_data(df.fillna('-'))


def add_db_alias(alias, db, host, username, password):
    """TODO:"""



def get_dbcredentials(db_alias, with_schema=True, as_engine_str=False):
    db_alias = db_alias.upper()
    config = configparser.ConfigParser()
    config.read(db_config)

    if db_alias not in config:
        raise Exception(f'{db_alias} not in db config file.')

    db_config_ = config[db_alias]

    if db_config_['db'] == 'sqlite':
        if as_engine_str:
            return f"sqlite:///{db_config_['host']}"
        else:
            return dict(db_config_)
    if db_config_['db'] == 'mysql':
        db_config_['password'] = base64.b64decode(db_config_['password']).decode()
        if as_engine_str:
            if with_schema and 'schema' in db_config_:
                return f"mysql://{db_config_['username']}:{db_config_['password']}@{db_config_['host']}/{db_config_['schema']}"
            else:
                return f"mysql://{db_config_['username']}:{db_config_['password']}@{db_config_['host']}"
        else:
            return dict(db_config_)
    else:
        raise Exception(f"{db_config_['db']} not implemented.")


def get_dbconnection(db_alias, mysql_schema=None, as_engine=True, echo=False):
    db_alias = db_alias.upper()

    creds = get_dbcredentials(db_alias, as_engine_str=as_engine)

    if as_engine:
        return sa.create_engine(creds, pool_recycle=280, echo=echo)
    else:
        raise Exception('Raw db connection not implemented.')


def exec_sql(sql, engine, params={}, commit=False):
    if isinstance(engine, sa.engine.base.Engine):
        conn = engine.connect()
        trans = conn.begin()
        try:
            res = conn.execute(sql, params)
            if commit:
                trans.commit()
            else:
                trans.rollback()
            if res.returns_rows:
                return pd.DataFrame(res.fetchall(), columns=res.keys())
            else:
                return res
        except:
            trans.rollback()
            raise
    else:
        try:
            res = engine.execute(sql, params)
            if res.returns_rows:
                return pd.DataFrame(res.fetchall(), columns=res.keys())
            else:
                return res
        except:
            raise


def get_tables(table_name, engine, print_result=True, exact_match=False):

    if engine.url.drivername.startswith('oracle'):
        sql = '''select table_name, owner, last_analyzed from all_tables where table_name like upper(:tab) order by 1'''
    elif engine.url.drivername.startswith('mysql'):
        sql = '''select table_name, table_schema owner, update_time from information_schema.tables
        where table_name like upper(:tab) and table_type like '%TABLE%' order by 1'''
    elif engine.url.drivername.startswith('sqlite'):
        sql = '''select tbl_name table_name, null owner, null last_analyzed from sqlite_master
        where type = 'table' and name like :tab'''
    else:
        raise Exception(f'{engine.url.drivername} not supported!')

    if not exact_match:
        table_name = f'%{table_name}%'
    print(sql, table_name)
    return sql2df(sql, engine, {'tab': table_name}, print_result=print_result)


def get_views(view_name, engine, print_result=True, exact_match=False):

    if engine.url.drivername.startswith('oracle'):
        sql = '''select table_name, owner, view_type, read_only from all_views where view_name like upper(:view) order by 1'''
    elif engine.url.drivername.startswith('mysql'):
        sql = '''select table_name, table_schema owner, row_format from information_schema.tables
        where table_name like upper(:view) and table_type like '%VIEW%' order by 1'''
    elif engine.url.drivername.startswith('sqlite'):
        sql = '''select tbl_name table_name, null owner, null last_analyzed from sqlite_master
        where type = 'view' and name like :view'''
    else:
        raise Exception(f'{engine.url.drivername} not supported!')

    if not exact_match:
        view_name = f'%{view_name}%'
    print(sql, view_name)
    return sql2df(sql, engine, {'view': view_name}, print_result=print_result)


def desc_table(table_name, engine, print_result=True):
    """
    Function to find columns for given table
    """

    if engine.url.drivername.startswith('oracle'):
        sql = '''select owner, table_name, column_name, nullable, data_type, data_length, char_used, last_analyzed
        from all_tab_columns where table name like upper(:tab) order by table_name, column_id'''
    elif engine.url.drivername.startswith('mysql'):
        sql = '''select table_schema, table_name, column_name, is_nullable, column_type, null data_length, null char_used, null last_analyzed
        from information_schema.columns where table_name like upper(:tab)'''
    elif engine.url.drivername.startswith('sqlite'):
        sql = f'''PRAGMA table_info({table_name})'''
    else:
        raise Exception(f'{engine.url.drivername} not supported!')

    print(sql, table_name)
    return sql2df(sql, engine, {'tab': table_name}, print_result=print_result)


def get_db_version(engine, print_result=True):
    """Function to get the db version"""
    if engine.url.drivername.startswith('oracle'):
        sql = '''select banner version from v$version'''
    elif engine.url.drivername.startswith('mysql'):
        sql = '''show variables like '%version%' '''
    elif engine.url.drivername.startswith('sqlite'):
        sql = '''select sqlite_version()'''
    else:
        raise Exception(f'{engine.url.drivername} not supported!')

    return sql2df(sql, engine, print_result=print_result)


def load_table(table_name, engine, schema=None, sample_size=None):
    """Function to load entire table into a pd.DataFrame"""
    if sample_size:
        if schema:
            schema += '.'
        else:
            schema = ''

        if engine.url.drivername.startswith('oracle'):
            sql = f'''select * from {schema}{table_name} sample({sample_size})'''
        elif engine.url.drivername.startswith('mysql'):
            sql = f'''select * from {schema}{table_name} where rand() < {sample_size}/100'''
        elif engine.url.drivername.startswith('sqlite'):
            sql = f'''select * from {schema}{table_name} where random() < {sample_size}/100'''
        else:
            raise Exception(f'{engine.url.drivername} not supported!')

        df = pd.read_sql(sql, engine)
    else:
        df = pd.read_sql_table(table_name, engine, schema=schema)

    return df


def get_table_counts(table_name, column_names, engine, agg=list(), filter_=list(), sort=None, asc=False, print_result=True):
    """
    Function wrapper around select count(1) cnt from table_name
    and select column_name, count(1) cnt from table_name group by column_name order by 2 desc
    :param table_name:
    :param column_name:
    :param engine:
    :param print_result:
    :return:
    """
    default_agg = ['count(1) cnt']
    agg_ = ['{0}({1}) {0}_{1}'.format(*i.popitem()) for i in agg if 'cnt_distinct' not in i]
    if len(agg_) == 0:
        agg_ = default_agg

    if 'cnt_distinct' in agg:
        agg = agg_ + ['count(distinct {0}) distinct_{0}'.format(agg['cnt_distinct'])]
    elif 'distinct_cnt' in agg:
        agg = agg_ + ['count(distinct {0}) distinct_{0}'.format(agg['distinct_cnt'])]
    else:
        agg = agg_

    where_clause = ''

    if filter_:
        where_clause = f"where {' and '.join(filter_)}"

    if not sort:
        sort = len(column_names)

    if column_names:
        column_names = ', '.join(column_names)
        sql = f'''select {column_names}, {', '.join(agg)}
        from {table_name} {where_clause} group by {column_names}
        order by {sort} {'asc' if asc else 'desc'}'''
    else:
        sql = f'''select {', '.join(agg)} from {table_name} {where_clause}'''

    return sql2df(sql, engine, print_result=print_result)


def find_columns(col_name, engine, print_result=True, exact_match=False):
    """
    Function to find columns by name for a fiben db engine
    :param col_name:
    :param engine:
    :param print_result: Print instead of return a DataFrame
    :param exact_match: Exact match on column name
    :return: None or pd.DataFrame
    """
    if engine.url.drivername.startswith('oracle'):
        if exact_match:
            col = ':col'
        else:
            col = """'%'||:col||'%'"""

        sql = f'''select owner, table_name, column_name, NULLABLE, data_type, data_length
        from all_tab_columns where column_name like upper({col})'''
    elif engine.url.drivername.startswith('mysql'):
        if exact_match:
            col = ':col'
        else:
            col = """concat('%', :col ,'%')"""

        sql = f'''select table_schema owner, table_name, column_name, is_nullable, column_type
        null data_length from information_schema.columns where column_name like ({col})'''
    else:
        raise Exception(f'{engine.url.drivername} not supported!')

    return sql2df(sql, engine, params={'col': col_name}, print_result=print_result)


def find_table_by_column_value(col_name, col_value, engine, print_result=True):
    """
    Function to find tables by given column value
    """
    df = find_columns(col_name, engine, print_result=False, exact_match=True)
    print(f'Found {len(df)} rables containing column {col_name}')
    df['cnt'] = 0
    df['order'] = df.owner.str.len() + df.table_name.str.len()
    for rec in df[['owner', 'table_name', 'order']].sort_values('order', ascending=True).to_records(index=False):
        res = fetch_data(f'''select count(1) c from {rec[0]}.{rec[1]} where {col_name} = :val''',
                        engine, params={'val': col_name})
        cnt = res.fetchone()[0]
        sys.stdout.write("\r" + f'Scanning {rec[0]}.{rec[1]} ...')
        sys.stdout.flush()
        if cnt == 0:
            df = df[df.table_name != rec[1]]
        else:
            df.loc[df.table_name == rec[1], 'cnt'] = cnt
        print('')
        if print_result:
            print_tabular_data(df[['owner', 'table_name', 'column_name', 'cnt']])
        else:
            return df[['owner', 'table_name', 'column_name', 'cnt']]
