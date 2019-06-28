"""tumor_reg_summary -- summarize NAACCR tumor registry file

Capture statistics useful for synthesizing data.

Usage:

  tumor_reg_summary ddict.db tr.db naaccr_file

where `ddict.db` is generated using `scrape.py` and `csvdb.py`.


Static Typing
~~~~~~~~~~~~~

Check with `mypy --strict`.

"""

from contextlib import contextmanager
from itertools import islice
from pathlib import Path as Path_T  # use type only, per ocap discipline
from sys import stderr  # ocap exception for logging
from typing import (
    Any,
    Callable, Iterable, Iterator,
    List, NamedTuple, Optional, Tuple,
    TextIO,
    cast,
)
import csv
import math
from sqlite3 import Connection, Cursor

import pkg_resources as pkg

Chunk = List[Tuple[int, str]]


def main(argv: List[str], cwd: Path_T,
         connect: Callable[[str], Connection],
         tumor_limit: int = 500) -> None:
    [ddict_fn, db_fn, naaccr_file] = argv[1:4]

    db = connect(db_fn)
    ddict = connect(ddict_fn)
    tr = Registry.make(db, ddict)
    tr.load_eav(cwd / naaccr_file,
                tumor_limit=tumor_limit)
    stats = DataSummary(db)
    stats.prep(ddict_fn)
    stats.create_eav_agg()


def _log(*args: Any) -> None:
    print(*args, file=stderr)


class Item(NamedTuple):
    """NAACCR Item, i.e. a field
    """

    start: int
    end: int
    length: int
    item: int
    name: str
    xmlId: str
    parentTag: str
    section: str
    note: str


class Registry(object):
    """
    >>> proto = Item(1, 2, 1, 10, 'Size', 'size', 'Tumor', 'C', '')
    >>> layout = [proto, proto._replace(xmlId='weight', length=3)]

    >>> import sqlite3
    >>> tr = Registry(sqlite3.connect(':memory:'), layout)
    >>> tr.insert_dml()
    'insert into tumors (size, weight) values(?, ?)'
    """
    def __init__(self, conn: Connection, layout: List['Item'],
                 table_name: str = 'tumors') -> None:
        self.__conn = conn
        self.layout = layout
        self.table_name = table_name

    @classmethod
    def make(cls, conn: Connection, ddict: Connection,
             max_length: int = 10,
             table_name: str = 'tumors') -> 'Registry':
        conn.cursor().execute(f'drop table if exists {table_name}_eav')
        eav_ddl = f'''
        create table {table_name}_eav (tumorid int, itemnbr int, value text)
        '''
        conn.cursor().execute(eav_ddl)
        _log(f'created {table_name}')

        q = ddict.cursor()
        q.execute(
            'select * from record_layout where length <= ?',
            (max_length,))
        items = [Item(*row) for row in q.fetchall()]
        ddl = cls.create_ddl(table_name, items)
        conn.cursor().execute(f'drop table if exists {table_name}')
        conn.cursor().execute(ddl)
        _log(f'created: {table_name} with {len(items)} columns')
        return cls(conn, items)

    def load_eav(self, tumors: Path_T,
                 tumor_limit: Optional[int] = None) -> int:
        insert_stmt = f'''
        insert into {self.table_name}_eav
               (tumorid, itemnbr, value) values (?, ?, ?)
        '''
        qty = 0
        with txn(self.__conn) as work:
            for lines in self._file_chunks(tumors, limit=tumor_limit):
                records = [
                    (ix, it.item, chars)
                    for ix, line in lines
                    for it in self.layout
                    for chars in [line[it.start - 1:it.end].strip()]
                    if chars
                ]
                work.executemany(insert_stmt, records)
                qty += len(records)
                id, _ = lines[-1]
                _log(f'tumor {id}: records inserted += {len(records)} = {qty}')

        return qty

    def load_file(self, tumors: Path_T) -> int:
        insert_stmt = self.insert_dml()

        qty = 0
        with txn(self.__conn) as work:
            for lines in self._file_chunks(tumors):
                records = [
                    tuple([line[it.start - 1:it.end]
                           for it in self.layout])
                    for ix, line in lines
                ]
                work.executemany(insert_stmt, records)
                qty += len(records)
                _log(f'records inserted += {len(records)} = {qty}')

        return qty

    def _file_chunks(self, tumors: Path_T,
                     limit: Optional[int] = None,
                     chunk_size: int = 4096) -> Iterable[Chunk]:
        with cast(TextIO, tumors.open(mode='r')) as fp:  # typeshed/issues/2911
            lines = enumerate(fp)
            while True:
                chunk = list(islice(lines, chunk_size))
                if not chunk:
                    break
                yield chunk
                if limit is not None:
                    last_ix, _ = chunk[-1]
                    if last_ix > limit:
                        break

    @classmethod
    def load_layout(cls, lines: Iterator[str]) -> Iterator[Item]:
        for rec in csv.DictReader(lines):
            yield Item(start=int(rec.pop('start')),
                       end=int(rec.pop('end')),
                       length=int(rec.pop('length')),
                       item=int(rec.pop('item')),
                       **rec)

    @classmethod
    def create_ddl(cls, name: str, items: Iterable['Item']) -> str:
        coldefs = ',\n'.join(f'  {it.xmlId} varchar({it.length})'
                             for it in items)
        return f'create table {name} (\n{coldefs})\n'

    def insert_dml(self) -> str:
        colnames = ', '.join(it.xmlId for it in self.layout)
        params = ', '.join(['?'] * len(self.layout))
        return f'insert into {self.table_name} ({colnames}) values({params})'


class SqlScript(object):
    @classmethod
    def ea_stmt(cls, txt: str) -> List[str]:
        return [stmt.strip()
                for stmt in txt.split(';\n')]

    @classmethod
    def find_view_def(cls, name: str, script: str) -> str:
        for stmt in cls.ea_stmt(script):
            if ((stmt.startswith('create ')
                 and name in stmt.split('\n')[0])):
                return stmt
        raise KeyError(name)


class DataSummary(object):
    prep_script = pkg.resource_string(
        __name__, 'naaccr_txform.sql').decode('utf-8')

    script = pkg.resource_string(
        __name__, 'data_char_sim.sql').decode('utf-8')

    def __init__(self, conn: Connection) -> None:
        self.__conn = conn

    def prep(self, ddict: str) -> None:
        self.__conn.create_aggregate("stddev", 1, StdevFunc)
        with txn(self.__conn) as work:
            work.execute(f'attach {repr(ddict)} as ddict')
            for view_name in ['t_item', 'tumor_item_type']:
                ddl = SqlScript.find_view_def(view_name, self.prep_script)
                work.execute(f'drop table if exists {view_name}')
                _log(ddl)
                work.execute(ddl)

    def create_eav_agg(self,
                       table: str = 'data_agg_naaccr') -> None:
        agg_ddl = SqlScript.find_view_def(
            'data_agg_naaccr', self.script)
        agg_view_ddl = SqlScript.find_view_def(
            'data_char_naaccr', self.script)
        _log(agg_ddl)
        with txn(self.__conn) as work:
            work.execute(f'drop table if exists {table}')
            work.execute(agg_ddl)
            work.execute(agg_view_ddl)


class StdevFunc:
    """ack: https://stackoverflow.com/a/24423341
    """
    def __init__(self) -> None:
        self.M = 0.0
        self.S = 0.0
        self.k = 1

    def step(self, value: Optional[float]) -> None:
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1

    def finalize(self) -> Optional[float]:
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))


@contextmanager
def txn(conn: Connection) -> Iterator[Cursor]:
    cur = conn.cursor()
    try:
        yield cur
    except:  # noqa
        conn.rollback()
        raise
    else:
        conn.commit()


if __name__ == '__main__':
    def _script() -> None:
        # Access ambient authority only when invoked as script,
        # as a form of ocap discipline.
        from sys import argv
        from pathlib import Path
        from sqlite3 import connect

        main(argv[:], Path('.'), lambda f: connect(f))

    _script()
