import sys
import pathlib
import sqlalchemy as sa


from IPython.core.magic import Magics, magics_class, line_magic, line_cell_magic, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.terminal.prompts import Prompts, Token

utils_path = pathlib.Path().home() / 'utils'
sys.path.append(utils_path.as_posix())

import db_utils as dbu

class SqlPrompt(Prompts):
    def __init__(self, shell, name, is_trans=False):
        self.shell = shell
        self.name = name
        self.is_trans = is_trans

    def in_prompt_tokens(self, cli=None):
        return [(Token.Prompt, 'In ['),
                (Token.PromptNum, str(self.shell.execution_count)),
                (Token.Prompt, f'] '),
                (Token.Comment, f'({self.name})'),
                (Token.Prompt, ': ')]


@magics_class
class SqlMagic(Magics):
    #"""Provides magic functions for various db related tasks""""

    default_db_alias = 'sqlite_tesla'
    mysql_schema = None

    engine = dbu.get_dbconnection(default_db_alias)

    conn, trans = None, None

    def get_engine(self, args):
        mysql_schema = None
        if 'db_alias' in args and args.db_alias:
            alias = args.db_alias
            if '.' in alias:
                alias, schema = alias.split('.')
                mysql_schema = schema.lower()
        else:
            alias = self.default_db_alias

        if alias != self.default_db_alias:
            engine = dbu.get_dbconnection(alias, mysql_schema=mysql_schema, as_engine=True)
        else:
            engine = self.engine
        return engine


    @magic_arguments()
    @argument('filter', nargs='?', help='Filter by db alias')
    @argument('-f', '--as-frame', action='store_true', help='Return a DataFrame instaed of printing')
    @line_magic('get_db_aliases')
    def get_db_aliases(self, line):
        """Function to get the available db aliases"""
        args = parse_argstring(self.get_db_aliases, line)
        return dbu.get_db_aliases(args.filter, args.as_frame)


    @line_magic('get_default_connection')
    def get_default_connection(self, line):
        """Shows default connection alias"""
        print(f'Default DB Connection: {self.default_db_alias}')
        ip = get_ipython()
        ip.prompts = SqlPrompt(ip, self.default_db_alias)
        if self.mysql_schema is not None:
            print(f'Mysql Schema: {self.mysql_schema}')
            ip.prompts = SqlPrompt(ip, f'{self.default_db_alias}.{self.mysql_schema}')


    @magic_arguments()
    @argument('db_alias', nargs='?', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @line_magic('set_default_connection')
    def set_default_connection(self, line):
        """Function to set a default db connection"""
        args = parse_argstring(self.set_default_connection, line)

        if args.db_alias is None:
            self.get_default_connection(line)
            return
        if '.' in args.db_alias:
            parts = args.db_alias.split('.')
            db_alias = parts[0]
            mysql_schema = parts[1].lower()
        else:
            db_alias = args.db_alias
            mysql_schema = None

        aliases = dbu.get_db_aliases(as_dataframe=True)

        if db_alias not in aliases.alias.tolist():
            print(f'Db alias {db_alias} not in config')
            return

        self.default_db_alias = db_alias
        self.mysql_schema = mysql_schema

        self.engine = dbu.get_dbconnection(self.default_db_alias, mysql_schema=self.mysql_schema, as_engine=True)
        self.get_default_connection(line)

        self.conn = None
        self.trans = None


    @magic_arguments()
    @argument('-d', '--db-alias', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @argument('--commit', action='store_true', help='Commit sql. Default: False')
    @argument('sql', type=str, nargs='*')
    @line_cell_magic('sql')
    def exec_sql(self, line, cell=None):
        user_ns = self.shell.user_ns

        args = parse_argstring(self.exec_sql, line)

        sql = ' '.join(args.sql)

        if cell:
            if pathlib.Path(cell).is_file():
                print(f'File: {cell}')
                cell = pathlib.Path(cell).read_text()

            if sql:
                sql += f'\n{cell}'
            else:
                sql = cell
        elif pathlib.Path(sql).is_file():
            print(f'File: {sql}')
            sql = pathlib.Path(sql).read_text()

        # handle bind variables with sa.text (makes sql variable style agnostic)
        params = dict()
        sql = sa.text(sql)
        sql = sql.compile()

        for param in sql.binds:
            if param in user_ns:
                params[param] = user_ns[param]
            else:
                raw_data = input(f'Please enter value for {param}:')
                params[param] = raw_data

        try:
            if self.trans is None:
                engine = self.get_engine(args)
                df = dbu.exec_sql(sql, engine=engine, params=params, commit=args.commit)
            else:
                if self.trans.is_active:
                    df = dbu.exec_sql(sql, engine=self.conn, params=params)
                else:
                    print('Error: Transaction is not active')
                    return

            if df is not None and len(df) == 1 and len(df.columns) > 3:
                return df.T
            else:
                return df
        except sa.exc.DatabaseError as e:
            print(f'{e}')

    @magic_arguments()
    @argument('table_name', type=str, help='Table name')
    @argument('-d', '--db-alias', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @argument('-f', '--as-frame', action='store_true', help='Return a DataFrame instaed of printing')
    @argument('-e', '--exact-match', action='store_true', help='Exact table name match')
    @line_magic('get_tables')
    def get_tables(self, line):
        """Ipython extension function for finding tables
        """
        args = parse_argstring(self.get_tables, line)

        engine = self.get_engine(args)

        try:
            df = dbu.get_tables(args.table_name, engine, print_result=not args.as_frame, exact_match=args.exact_match)
            if args.as_frame:
                return df
        except sa.exc.DatabaseError as e:
            print(f'{e}')

    @magic_arguments()
    @argument('table_name', type=str, help='Table name')
    @argument('-d', '--db-alias', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @argument('-f', '--as-frame', action='store_true', help='Return a DataFrame instead of printing')
    @line_magic('desc_table')
    @line_magic('desc')
    def describe_table(self, line):
        """IPython extension to describe a table"""
        args = parse_argstring(self.get_tables, line)

        engine = self.get_engine(args)

        try:
            df = dbu.desc_table(args.table_name, engine, print_result=not args.as_frame)
            return df
        except sa.exc.DatabaseError as e:
            print(f'{e}')

    @magic_arguments()
    @argument('-d', '--db-alias', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @argument('-f', '--as-frame', action='store_true', help='Return a DataFrame instead of printing')
    @argument('--cnt-distinct', type=str.lower, help='Count distinct column name')
    @argument('--distinct-cnt', type=str.lower, help='Alias for cnt-distnct option')
    @argument('--count', type=str.lower, help='Count column name')
    @argument('--sum', type=str.lower, help='Sum column name')
    @argument('--avg', type=str.lower, help='Avg column name')
    @argument('--max', type=str.lower, help='Max column name')
    @argument('--min', type=str.lower, help='Min column name')
    @argument('--sort', type=int, help='Sort by column number')
    @argument('--asc', action='store_true', help='Ascending')
    @argument('--filter', action='append', nargs='+', help="Where condition. Ex: 'col_name >= 0'")
    @argument('table_name', type=str.lower, help='Table name')
    @argument('column_names', type=str.lower, nargs='*', help='Column names used to group by')
    @line_magic('get_table_counts')
    def get_table_counts(self, line):
        """
        Ipython extension function to get table columns counts
        :return: None or pd.Dataframe
        """
        args = parse_argstring(self.get_table_counts, line)

        engine = self.get_engine(args)

        args = vars(args)

        aggregators = ('cnt_distinct', 'distinct_cnt', 'count', 'sum', 'avg', 'min', 'max')

        agg = list()
        for aggr in aggregators:
            if args[aggr]:
                agg.append({aggr:args[aggr]})

        filters = list()
        if args['filter']:
            filters += [' '.join(f) for f in args['filter']]

        try:
            df = dbu.get_table_counts(args['table_name'], args['column_names'], engine,
                                        agg, filters, args['sort'], args['asc'],
                                        print_result=not args['as_frame'])
            return df
        except sa.exc.DatabaseError as e:
            print(f'{e}')


    @magic_arguments()
    @argument('table_name', type=str.lower, help='Table name')
    @argument('-s', '--schema', type=str.lower, help='Schema')
    @argument('-d', '--db-alias', type=str.upper, help='Db Alias: db_alias|db_alias.schema (mysql)')
    @argument('-r', '--random-sample-size', type=float, help='Fetch only percentage sample of the table')
    @line_magic('load_table')
    def load_table(self, line):
        """
        Ipython extension function to load table into a DataFrame
        :return: pd.DataFrame
        """
        args = parse_argstring(self.load_table, line)
        engine = self.get_engine(args)
        print(args)
        try:
            df = dbu.load_table(args.table_name, engine, schema=args.schema, sample_size=args.random_sample_size)
            return df
        except ValueError as e:
            print(f'{e}')


def load_ipython_extension(ipython):
    global _loaded
    if not _loaded:
        ipython.register_magics(SqlMagic)
        _loaded = True


_loaded = False
