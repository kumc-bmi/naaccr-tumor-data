from typing import Dict, Iterable, List, Optional as Opt, Text, Tuple, Union
from importlib import resources as res
from pathlib import Path as Path_T
from sqlite3 import Connection
import datetime as dt
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
        self._tables = {}  # type: Dict[str, DataFrame]
        self.__conn = conn

    def __setitem__(self, k: str, v: 'DataFrame') -> None:
        self._tables[k] = v

    def table(self, k: str) -> 'DataFrame':
        return self._tables[k]

    def sql(self, code: str) -> 'DataFrame':
        _c, _o, _r, _v, name, _ = code.split(None, 6)
        self.__conn.execute(code)
        df = DataFrame(self, name)
        self._tables[name] = df
        return df

    def createDataFrame(self, df: tab.DataFrame) -> 'DataFrame':
        gen_name = '_table_%d' % abs(hash(str(df.data)))  # KLUDGE?
        load_table(self.__conn, gen_name,
                   header=[col['name'] for col in df.schema['columns']],
                   rows=df.data)
        return DataFrame(self, gen_name)

    def read_csv(self, access: Path_T) -> 'DataFrame':
        df = tab.read_csv(access)
        return self.createDataFrame(df)


def load_table(dest: Connection, table: str,
               header: List[str], rows: Iterable[tab.Row],
               batch_size: int = 500) -> None:
    '''Load rows of a table in batches.
    '''
    log.info('loading %s', table)
    work = dest.cursor()
    stmt = """
      insert into "{table}" ({header})
      values ({placeholders})""".format(
        table=table,
        header=', '.join(f'"{col}"' for col in header),
        placeholders=', '.join('?' for _ in header)
    )
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


class DataFrame:
    """a la pandas or Spark DataFrame, but using python stdlib only
    """
    def __init__(self, ctx: DBSession, label: str) -> None:
        self.__ctx = ctx
        self.label = label

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.label})'

    def createOrReplaceTempView(self, name: str) -> None:
        self.__ctx[name] = self


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
                   **kwargs: DataFrame) -> Dict[str, DataFrame]:
    """Run SQL create ... statements given DFs for input views.

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
    """
    # IDEA: use a contextmanager for temp views
    for key, df in kwargs.items():
        df = kwargs[key]
        log.info('%s: %s = %s', script.name, key, df)
        df.createOrReplaceTempView(key)  # ISSUE: don't replace?
    provided = set(kwargs.keys())
    out = {}
    for name, ddl, inputs in script.objects:
        missing = set(inputs) - provided
        if missing:
            raise TypeError(missing)
        log.info('%s: create %s', script.name, name)
        spark.sql(ddl)
        df = spark.table(name)
        df.createOrReplaceTempView(name)  # ISSUE: don't replace?
        out[name] = df
    return out


class MockCTX(DBSession):
    def __init__(self) -> None:
        self._tables = {}  # type: Dict[str, DataFrame]

    def __setitem__(self, k: str, v: DataFrame) -> None:
        self._tables[k] = v

    def table(self, k: str) -> DataFrame:
        return self._tables[k]

    def sql(self, code: str) -> DataFrame:
        _c, _o, _r, _t, _v, name, _ = code.split(None, 6)
        df = MockDF(self, name)
        self._tables[name] = df
        return df


class MockDF(DataFrame):
    def __init__(self, ctx: MockCTX, label: str) -> None:
        self.__ctx = ctx
        self.label = label

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.label})'

    def createOrReplaceTempView(self, name: str) -> None:
        self.__ctx[name] = self
