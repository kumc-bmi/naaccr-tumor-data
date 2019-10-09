"""tabular -- DataFrame API for tabular data

This provides features of the DataFrame APIs from pandas and pyspark
but using only the python3 standard library.

ref https://www.w3.org/TR/tabular-data-primer/#datatypes

"""

from typing import Callable, Iterable, List, Optional as Opt, Union
from typing_extensions import Literal, TypedDict
from pathlib import Path as Path_T
import csv
import json
import logging

Value = Union[str, int]
DataType = Union[Literal['string'], Literal['number']]
Column = TypedDict('Column', {
    'name': str,
    'datatype': DataType,
    'null': List[str],
    'number': int,
})
Schema = TypedDict('Schema', {'columns': List[Column]})
Table = TypedDict('Table', {'tableSchema': Schema})
TableMeta = TypedDict('TableMeta', {
    '@context': Literal['http://www.w3.org/ns/csvw'],
    'tableSchema': Schema})

log = logging.getLogger(__name__)


def main(argv: List[str], cwd: Path_T) -> None:
    for csvname in argv[1:]:
        add_meta(cwd / csvname)


class DataFrame:
    def __init__(self, data: Iterable[List[Opt[Value]]], schema: Schema) -> None:
        self.schema = schema
        self.byName = {col['name']: col for col in schema['columns']}
        self.data = list(data)

    @property
    def columns(self) -> List[str]:
        return [c['name'] for c in self.schema['columns']]

    def drop(self, names: List[str]) -> 'DataFrame':
        schema = Schema(columns=[col for col in self.schema['columns']
                                 if col['name'] not in names])
        return DataFrame(self.data, schema)

    @classmethod
    def decoder(cls, schema: Schema) -> (Callable[[List[str]], List[Opt[Value]]]):
        ea = [
            (col['number'] - 1, Seq.decoder(col))
            for col in schema['columns']
        ]

        def decode(row: List[str]) -> List[Opt[Value]]:
            return [f(row[ix]) for (ix, f) in ea]

        return decode

    def __getattr__(self, name: str) -> 'Seq':
        col = self.byName.get(name)
        if not col:
            raise AttributeError(col)

        return Seq(col, self.data)


def meta_path(path: Path_T) -> Path_T:
    return path.parent / (path.stem + '-metadata.json')


def read_csv(path: Path_T) -> DataFrame:
    meta = json.load(meta_path(path).open())
    schema = meta['tableSchema']
    decode = DataFrame.decoder(schema)

    with path.open() as fp:
        reader = csv.reader(fp)
        # IDEA: check consistency between header and schema
        _ = next(reader)
        return DataFrame((decode(row) for row in reader), schema)


class Seq:
    def __init__(self, column: Column, data: List[List[Opt[Value]]]) -> None:
        self.column = column
        self.data = data

    @property
    def values(self) -> List[Opt[Value]]:
        ix = self.column['number'] - 1
        return [row[ix] for row in self.data]

    @classmethod
    def decoder(cls, col: Column) -> Callable[[str], Opt[Value]]:
        def boolean(s: str) -> Value:
            return bool(s)

        def number(s: str) -> Value:
            return int(s)

        def text(s: str) -> Value:
            return s

        def maybe(nulls: List[str], f: Callable[[str], Value]) -> Callable[[str], Opt[Value]]:
            def check(s: str) -> Opt[Value]:
                return None if s in nulls else f(s)
            return check

        def some(dt: Callable[[str], Value]) -> Callable[[str], Opt[Value]]:
            return dt

        dt = {'number': number, 'string': text, 'boolean': boolean}[col['datatype']]
        return maybe(col['null'], dt) if col['null'] else some(dt)


def add_meta(path: Path_T) -> TableMeta:
    with path.open() as infp:
        names = next(csv.reader(infp))
    dest = meta_path(path)
    dirty = False
    if dest.exists():
        meta: TableMeta = json.load(dest.open())
        for ix, col in enumerate(meta['tableSchema']['columns']):
            if not col.get('number'):
                log.info('%s -> %d', col['name'], ix + 1)
                col['number'] = ix + 1
                dirty = True
            if not col.get('null'):
                log.info('%s.null -> %s', col['name'], [''])
                col['null'] = ['']
                dirty = True
    else:
        meta = {
            '@context': 'http://www.w3.org/ns/csvw',
            'tableSchema': Schema(columns=[
                Column(name=n, datatype='string', null=[''], number=ix + 1)
                for ix, n in enumerate(names)])}
        dirty = True
    if dirty:
        log.info('%s -> %s', dest, names)
        with dest.open('w') as out:
            json.dump(meta, out, indent=2)
    return meta


if __name__ == '__main__':
    def _script_io() -> None:
        from sys import argv
        from pathlib import Path

        logging.basicConfig(level=logging.INFO)
        main(argv[:], Path('.'))

    _script_io()
