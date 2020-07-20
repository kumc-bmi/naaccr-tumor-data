# TODO: prune pyspark stuff incl. this sql_script.py

r"""

Run SQL create ... statements given DFs for input views.

    >>> spark = MockCTX()
    >>> create_objects(spark, _TestData.ont_script,
    ...     current_task=MockDF(spark, 'current_task'),
    ...     naaccr_top=MockDF(spark, 'naaccr_top'),
    ...     section=MockDF(spark, 'section'),
    ...     loinc_naaccr_answers=MockDF(spark, 'lna'),
    ...     code_labels=MockDF(spark, 'code_labels'),
    ...     icd_o_topo=MockDF(spark, 'icd_o_topo'),
    ...     seer_site_terms=MockDF(spark, 'site_terms'),
    ...     cs_terms=MockDF(spark, 'cs_terms'),
    ...     tumor_item_type=MockDF(spark, 'ty'))
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    {'i2b2_path_concept': MockDF(i2b2_path_concept),
     'naaccr_top_concept': MockDF(naaccr_top_concept),
     'section_concepts': MockDF(section_concepts),
     ...}

Replace SQL create view statements:

    >>> s2 = _TestData.ont_script.replace_ddl('section_concepts', 'select 1 from x')
    >>> SqlScript.find_ddl('section_concepts', s2.code)
    'create view section_concepts as\nselect 1 from x'

Treat a database table as a DataFrame:

    >>> from sqlite3 import connect as connect_mem
    >>> conn = connect_mem(':memory:', detect_types=PARSE_COLNAMES)
    >>> conn.execute('create table t1 as select 1 as c1') and None
    >>> ctx = DBSession(conn)
    >>> DataFrame.select_from(ctx, 't1')
    DataFrame({'c1': 'number'})

Or a query:
    >>> ctx.sql('''SELECT '2019-10-01' as "t0 [date]" ''')
    DataFrame({'t0': 'date'})


Load CSV into a database table and select from it:

    >>> import heron_load
    >>> ctx = DBSession.in_memory()
    >>> with res.path(heron_load, 'section.csv') as p:
    ...     df = ctx.load_data_frame('section', tab.read_csv(p))
    ...     print(df)
    ...     for ix, row in df.iterrows():
    ...         if ix >= 3:
    ...             break
    ...         print(row)
    DataFrame({'sectionid': 'number', 'section': 'string'})
    (1, 'Cancer Identification')
    (2, 'Demographic')
    (3, 'Edit Overrides/Conversion History/System Admin')

Project / select columns:

    >>> df.select('section')
    DataFrame({'section': 'string'})
"""

from typing import Callable, Dict, List, Optional as Opt, Text, Tuple, Union
from typing import Iterator, Iterable, TextIO
from contextlib import contextmanager
from importlib import resources as res
from pathlib import Path as Path_T
from sqlite3 import Connection, Cursor, PARSE_COLNAMES
import datetime as dt
import json
import logging
import re

import tabular as tab

log = logging.getLogger(__name__)

Name = Text
SQL = Text
Environment = Dict[Name, Text]
BindValue = Union[None, str, int, dt.date, dt.datetime]
Params = Dict[str, BindValue]
Line = int
Comment = Text
StatementInContext = Tuple[Line, Comment, SQL]


def main(argv: List[str], cwd: Path_T, connect: Callable[..., Connection]) -> None:
    db = argv[1]

    ctx = DBSession(connect(db, detect_types=PARSE_COLNAMES))
    for csv_file in argv[2:]:
        p = cwd / csv_file
        df = ctx.load_data_frame(p.stem, tab.read_csv(p))
        log.info('%s -> %s', p, df)
        for ix, row in df.iterrows():
            if ix >= 3:
                break
            log.info('row %d: %s', ix, row)


class SqlScript(object):
    def __init__(self, name: str, code: SQL,
                 object_info: List[Tuple[str, List[str]]]):
        """
        @param object_info: list of view names created in the code
               along with names of views they depend on.
               See also: tumor_reg_ont.create_objects.
        """
        self.name = name
        self.code = code
        self.objects = [
            (name, self.find_ddl(name, code), inputs)
            for name, inputs in object_info
        ]

    @classmethod
    def find_ddl(cls, name: str, script: str) -> str:
        for _l, _c, stmt in cls.each_statement(script):
            if stmt.startswith('create '):
                if name in stmt.split('\n')[0].split():
                    return stmt
        raise KeyError(name)

    def replace_ddl(self, name: str, query: SQL,
                    is_table: bool = False) -> 'SqlScript':
        parts = []
        state = 'before'
        obj_type = "table" if is_table else "view"
        for line in self.code.split('\n')[:-1]:
            if (state == 'before' and line.startswith('create ') and
                name in line.split()):
                parts.append(f'create {obj_type} {name} as\n{query.strip()};\n')
                state = 'during'
            elif state == 'during' and line.endswith(';'):
                state = 'after'
            elif state != 'during':
                parts.append(line + '\n')
        if state == 'before':
            raise KeyError(name)
        return SqlScript(self.name, ''.join(parts), [])

    @classmethod
    def each_statement(cls, sql: str,
                       variables: Opt[Environment] = None,
                       skip_unbound: bool = False) -> Iterable[StatementInContext]:
        # idea: use statement namese lik https://github.com/honza/anosql
        for line, comment, statement in iter_statement(sql):
            try:
                ss = substitute(statement, variables)
            except KeyError:
                # ISSUE: dead code?
                if skip_unbound:
                    continue
                else:
                    raise
            yield line, comment, ss

    @classmethod
    def sqlerror(cls, s: SQL) -> Opt[bool]:
        if s.strip().lower() == 'whenever sqlerror exit':
            return False
        elif s.strip().lower() == 'whenever sqlerror continue':
            return True
        return None


class SqlScriptError(IOError):
    '''Include script file, line number in diagnostics
    '''
    def __init__(self, exc: Exception, fname: str, line: int, statement: SQL,
                 conn_label: str) -> None:
        message = '%s <%s>\n%s:%s:\n'
        args = [exc, conn_label, fname, line]
        message += '%s'
        args += [statement]

        self.message = message
        self.args = tuple(args)

    def __str__(self) -> str:
        return self.message % self.args


def iter_statement(txt: SQL) -> Iterable[StatementInContext]:
    r'''Iterate over SQL statements in a script.
    >>> list(iter_statement("drop table foo; create table foo"))
    [(1, '', 'drop table foo'), (1, '', 'create table foo')]
    >>> list(iter_statement("-- blah blah\ndrop table foo"))
    [(2, '-- blah blah\n', 'drop table foo')]
    >>> list(iter_statement("drop /* blah blah */ table foo"))
    [(1, '', 'drop  table foo')]
    >>> list(iter_statement("select '[^;]+' from dual"))
    [(1, '', "select '[^;]+' from dual")]
    >>> list(iter_statement('select "x--y" from z;'))
    [(1, '', 'select "x--y" from z')]
    >>> list(iter_statement("select 'x--y' from z;"))
    [(1, '', "select 'x--y' from z")]
    '''

    statement = comment = ''
    line = 1
    sline = None  # type: Opt[int]

    def save(txt: SQL) -> Tuple[SQL, Opt[int]]:
        return (statement + txt, sline or (line if txt else None))

    while 1:
        m = SQL_SEPARATORS.search(txt)
        if not m:
            statement, sline = save(txt)
            break

        pfx, match, txt = (txt[:m.start()],
                           txt[m.start():m.end()],
                           txt[m.end():])
        if pfx:
            statement, sline = save(pfx)

        if m.group('sep'):
            if sline:
                yield sline, comment, statement
            statement = comment = ''
            sline = None
        elif [n for n in ('lit', 'hint', 'sym')
              if m.group(n)]:
            statement, sline = save(match)
        elif (m.group('space') and statement):
            statement, sline = save(match)
        elif ((m.group('comment') and not statement) or
              (m.group('space') and comment)):
            comment += match

        line += (pfx + match).count("\n")

    if sline and (comment or statement):
        yield sline, comment, statement


# Check for hint before comment since a hint looks like a comment
SQL_SEPARATORS = re.compile(
    r'(?P<space>^\s+)'
    r'|(?P<hint>/\*\+.*?\*/)'
    r'|(?P<comment>(--[^\n]*(?:\n|$))|(?:/\*([^\*]|(\*(?!/)))*\*/))'
    r'|(?P<sym>"[^\"]*")'
    r"|(?P<lit>'[^\']*')"
    r'|(?P<sep>;)')


def substitute(sql: SQL, variables: Opt[Environment]) -> SQL:
    '''Evaluate substitution variables in the style of Oracle sqlplus.
    >>> substitute('select &&not_bound from dual', {})
    Traceback (most recent call last):
    KeyError: 'not_bound'
    '''
    if variables is None:
        return sql
    sql_esc = sql.replace('%', '%%')  # escape %, which we use specially
    return re.sub(r'&&(\w+)', r'%(\1)s', sql_esc) % variables


def params_used(params: Params, statement: SQL) -> Params:
    return dict((k, v) for (k, v) in params.items()
                if k in param_names(statement))


def param_names(s: SQL) -> List[Name]:
    '''
    >>> param_names('select 1+1 from dual')
    []
    >>> param_names('select 1+:y from dual')
    ['y']
    '''
    return [expr[1:]
            for expr in re.findall(r':\w+', s)]


def to_qmark(sql: SQL, params: Params) -> Tuple[SQL, List[BindValue]]:
    """Convert bind params to qmark style.

    Named params are replaced by ? and the
    corredponding param values are given in a list:

    >>> to_qmark('select x where id=:id and pos=:pos and x=2',
    ...          dict(id=23, pos=11))
    ('select x where id=? and pos=? and x=2', [23, 11])

    Using a name more than once works:
    >>> to_qmark('select x, :id where id=:id', dict(id=23, x=2))
    ('select x, ? where id=?', [23, 23])

    References to undefined parameters are left alone:

    >>> to_qmark("select to_char(dt, 'hh:mm') from dual", dict(id=23))
    ("select to_char(dt, 'hh:mm') from dual", [])

    Limitation: bind params in strings, comments are goofy:

    >>> to_qmark('select "ab :c" from x', dict(c=1))
    ('select "ab ?" from x', [1])
    """
    values = []
    out = ''
    done = 0
    for ref in re.finditer(fr':(\w+)\b', sql):
        out += sql[done:ref.start()]
        n = ref.group(1)
        if n in params:
            values.append(params[n])
            out += '?'
        else:
            out += ref.group(0)
        done = ref.end()
    out += sql[done:]
    return out, values


class DBSession:
    """a la SparkSession, but using python stdlib only
    """

    def __init__(self, conn: Connection) -> None:
        self.__conn = conn

    @classmethod
    def in_memory(cls) -> 'DBSession':
        from sqlite3 import connect as connect_mem
        return cls(connect_mem(':memory:'))

    def table(self, k: str) -> 'DataFrame':
        return DataFrame.select_from(self, k)

    def sql(self, code: str) -> 'DataFrame':
        if code.lower().strip().startswith('select '):
            return DataFrame.select_from(self, f'({code})')

        with txn(self.__conn) as work:  # type: Cursor
            log.debug('DBSession.sql: %s', code)
            work.execute(code)
        return DataFrame.select_from(self, '(select 1 as _dummy)')

    @contextmanager
    def _query(self, sql: str) -> Iterator[Cursor]:
        with txn(self.__conn) as q:
            log.debug('DBSession._query: %s', sql)
            q.execute(sql)
            yield q

    def load_data_frame(self, name: str, df: tab.Relation) -> 'DataFrame':
        with txn(self.__conn) as work:  # type: Cursor
            work.execute(f'drop table if exists {name}')
            work.execute(table_ddl(name, df.schema))
        load_table(self.__conn, name,
                   header=[col['name'] for col in df.schema['columns']],
                   rows=(row for (_, row) in df.iterrows()))
        return DataFrame.select_from(self, name)

    def read_csv(self, access: Path_T) -> 'DataFrame':
        df = tab.read_csv(access)
        return self.load_data_frame(access.stem, df)


@contextmanager
def txn(conn: Connection) -> Iterator[Cursor]:
    cur = conn.cursor()
    try:
        yield cur
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


def table_ddl(name: str, schema: tab.Schema) -> str:
    '''
    ISSUE: we assume all numbers are int; probably should say so.

    Denote text columns as such in order to preserve leading 0s in codes:

    >>> record = dict(code='001', num=123, dob=dt.date(1970, 1, 1))
    >>> schema = tab.DataFrame.from_records([record]).schema
    >>> print(table_ddl('t1', schema))
    create table "t1" ("code" text,
      "num" int,
      "dob" date)
    '''
    sqltypes = {'string': 'text',
                'number': 'int',
                'boolean': 'int',
                'date': 'date'}
    col_info = [(col['name'], sqltypes[col['datatype']])
                for col in schema['columns']]
    col_specs = ',\n  '.join(f'"{name}" {ty}'
                             for (name, ty) in col_info)
    return f'create table "{name}" ({col_specs})'


def insert_stmt(table: str, header: List[str]) -> str:
    return """
      insert into {table} ({header})
      values ({placeholders})""".format(
        table=table,
        header=', '.join(f'"{col}"' for col in header),
        placeholders=', '.join('?' for _ in header)
    )


def load_table(dest: Connection, table: str,
               header: List[str], rows: Iterable[tab.Row],
               batch_size: int = 500) -> None:
    '''Load rows of a table in batches.
    '''
    log.info('loading %s', table)
    work = dest.cursor()
    stmt = insert_stmt(table, header)
    log.debug('%s', stmt)
    batch = []  # type: List[tab.Row]

    def do_batch() -> None:
        work.executemany(stmt, batch)
        log.info('inserted %s rows into %s', len(batch), table)
        del batch[:]

    for row in rows:
        if row:  # skip blank row at end
            batch.append(row)
        if len(batch) >= batch_size:
            do_batch()
    if batch:
        do_batch()
    dest.commit()


class DataFrame(tab.Relation):
    """a la pandas or Spark DataFrame, based on a sqlite3 table

    Handle empty results. KLUDGE assume string columns:

    >>> ctx = DBSession.in_memory()
    >>> t1 = DataFrame.select_from(ctx, '(select 1 as c where 1=0)')
    >>> t1
    DataFrame({'c': 'string'})
    >>> t2 = t1.withColumn('c2', 'c + 1')
    >>> t2
    DataFrame({'c': 'string', 'c2': 'string'})
    >>> for _, row in t2.iterrows():
    ...     print(row)
    """
    def __init__(self, ctx: DBSession, table: str, schema: tab.Schema) -> None:
        tab.Relation.__init__(self, schema)
        self._ctx = ctx
        self.table = table

    @classmethod
    def select_from(cls, ctx: DBSession, table: str) -> 'DataFrame':
        with ctx._query(f'select * from {table} limit 1') as q:
            row = q.fetchone()
            if row is None:
                row = ['_' for _ in q.description]
            record = dict(zip((d[0] for d in q.description), row))
            schema = tab.DataFrame.from_records([record]).schema
        return DataFrame(ctx, table, schema)

    chunk_size = 1000

    def select(self, *cols: str) -> 'DataFrame':
        assert set(cols) <= set(self.columns)
        sep = '\n  , '
        return self._ctx.sql(f'''select {sep.join(cols)} from {self.table}''')

    def withColumn(self, name: str, col_expr: str) -> 'DataFrame':
        return self._ctx.sql(f'''select self.*, {col_expr} as {name} from {self.table} self''')

    def iterrows(self) -> Iterator[Tuple[int, tab.Row]]:
        ix = 0
        with self._ctx._query(f'select * from {self.table}') as q:
            while True:
                chunk = q.fetchmany(self.chunk_size)
                if not chunk:
                    break
                for row in chunk:
                    yield ix, row
                    ix += 1

    def dump(self, out: TextIO) -> None:
        json.dump({'sql': insert_stmt(self.table, self.columns)}, out)
        out.write('\n')
        for _, row in self.iterrows():
            json.dump(row, out)
            out.write('\n')


class _TestData:
    per_item_view = 'tumor_item_type'
    import heron_load

    ont_script = SqlScript(
        'naaccr_concepts_load.sql',
        res.read_text(heron_load, 'naaccr_concepts_load.sql'),
        [
            ('i2b2_path_concept', []),
            ('naaccr_top_concept', ['naaccr_top', 'current_task']),
            ('section_concepts', ['section', 'naaccr_top']),
            ('item_concepts', [per_item_view]),
            ('code_concepts', [per_item_view, 'loinc_naaccr_answers', 'code_labels']),
            ('primary_site_concepts', ['icd_o_topo']),
            # TODO: morphology
            ('seer_recode_concepts', ['seer_site_terms', 'naaccr_top']),
            ('site_schema_concepts', ['cs_terms']),
            ('naaccr_ontology', []),
        ])


def create_objects(spark: DBSession, script: SqlScript,
                   **kwargs: tab.DataFrame) -> Dict[str, DataFrame]:
    # IDEA: use a contextmanager for temp views
    for key, df in kwargs.items():
        log.info('%s: %s = %s', script.name, key, df)
        spark.load_data_frame(key, df)
    provided = set(kwargs.keys())
    out = {}
    for name, ddl, inputs in script.objects:
        missing = set(inputs) - provided
        if missing:
            raise TypeError(missing)
        log.info('%s: create %s', script.name, name)
        spark.sql(f'drop view if exists {name}')
        spark.sql(ddl)
        dft = spark.table(name)
        out[name] = dft
    return out


class MockCTX(DBSession):
    def __init__(self) -> None:
        self._tables = {}  # type: Dict[str, DataFrame]

    def load_data_frame(self, k: str, v: tab.Relation) -> 'DataFrame':
        df = self._tables[k] = MockDF(self, k)
        return df

    def table(self, k: str) -> DataFrame:
        return MockDF(self, k)

    def sql(self, code: str) -> DataFrame:
        c, _v, name, _ = code.split(None, 3)
        df = MockDF(self, name)
        self._tables[name] = df
        return df


class MockDF(DataFrame):
    def __init__(self, ctx: MockCTX, label: str) -> None:
        self._ctx = ctx
        self.label = label

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.label})'


if __name__ == '__main__':
    def _script_io() -> None:
        from sys import argv
        from pathlib import Path
        from sqlite3 import connect

        logging.basicConfig(level=logging.INFO)
        main(argv[:], Path('.'), connect)

    _script_io()
