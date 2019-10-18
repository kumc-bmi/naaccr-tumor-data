"""tabular -- DataFrame API for tabular data

Script usage
~~~~~~~~~~~~

To produce `a-metadata.json` etc. (or to add missing number, null
info):

  python tabular.py a.csv b.csv c.csv ...

ref https://www.w3.org/TR/tabular-data-primer/#datatypes

DataFrame API
~~~~~~~~~~~~~

This provides some features of the DataFrame APIs from pandas and
pyspark but using only the python3 standard library, mostly with the
goal of getting data from CSV and XML files into sqlite where more
advanced transformations are well supported. (See sql_script.py)

Note datatypes taken from metadata:

    >>> from importlib import resources as res
    >>> import heron_load
    >>> with res.path(heron_load, 'section.csv') as sp:
    ...     df = read_csv(sp)
    >>> df
    DataFrame({'sectionid': 'number', 'section': 'string'})

Note the printed representation has only schema info, as in spark,
rather than the data, as in pandas. To get the data, use `iterrows` as
in pandas:

    >>> for ix, row in df.iterrows():
    ...     if ix >= 3:
    ...         break
    ...     print(row)
    [1, 'Cancer Identification']
    [2, 'Demographic']
    [3, 'Edit Overrides/Conversion History/System Admin']

Metadata can also be determined from values:

    >>> DataFrame.from_records([dict(id=1, name='Pete', dob=dt.date(1970, 1, 1))])
    DataFrame({'id': 'number', 'name': 'string', 'dob': 'date'})

(Oops; I think `from_records` in pandas actually takes tuples, not
dicts)

"""

from typing import Dict, List, Optional as Opt, Sequence, Tuple, TextIO
from typing import Callable, Iterable, Iterator, Union
from typing_extensions import Literal, TypedDict
from abc import abstractmethod
from pathlib import Path as Path_T
import csv
import datetime as dt
import itertools
import json
import logging

Value = Union[str, int, dt.date]
Row = Sequence[Opt[Value]]
DataType = Union[Literal['string'], Literal['number'], Literal['boolean'], Literal['date']]
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


class Relation:
    def __init__(self, schema: Schema) -> None:
        self.schema = schema
        self.columns = [c['name'] for c in self.schema['columns']]
        self.byName = {col['name']: col for col in schema['columns']}

    def __repr__(self) -> str:
        info = {col['name']: col['datatype']
                for col in self.schema['columns']}
        return f'{self.__class__.__name__}({info})'

    @abstractmethod
    def iterrows(self) -> Iterator[Tuple[int, Row]]: ...

    def save_meta(self, dest: Path_T) -> Path_T:
        mp = meta_path(dest)
        with mp.open('w') as mout:
            json.dump(self.schema, mout, indent=2)
        return mp

    def to_csv(self, wr: TextIO) -> None:
        dest = csv.writer(wr)
        dest.writerow(self.columns)
        for _, row in self.iterrows():
            dest.writerow(row)


class DataFrame(Relation):
    def __init__(self, data: Iterable[Row], schema: Schema) -> None:
        Relation.__init__(self, schema)
        byNum = self.byNum = {col['number']: col for col in schema['columns']}
        self._col_ixs = [n - 1 for n in byNum.keys()]
        self._data = list(data)

    def __hash__(self) -> int:
        return hash(str((self.schema, self._data)))

    def iterrows(self) -> Iterator[Tuple[int, Row]]:
        col_ixs = self._col_ixs
        return ((cx, [row[ix] for ix in col_ixs])
                for (cx, row) in enumerate(self._data))

    @classmethod
    def from_columns(cls, seqs: Dict[str, 'Seq']) -> 'DataFrame':
        schema: Schema = {'columns': [
            {'name': name, 'number': ix + 1,
             'datatype': seq.column['datatype'], 'null': seq.column['null']}
            for (ix, (name, seq)) in enumerate(seqs.items())]}
        data = zip(*[seq.values for seq in seqs.values()])
        return DataFrame(data, schema)

    @classmethod
    def from_records(cls, records: Iterable[Dict[str, Opt[Value]]]) -> 'DataFrame':
        """
        Note: we assume all records have the same keys and the 1st record has no nulls.
        """
        reciter = iter(records)
        r0 = next(reciter)
        data = (list(r.values()) for r in itertools.chain([r0], reciter))
        columns = [Seq.column_of(val, name=name, number=ix + 1)
                   for (ix, (name, val)) in enumerate(r0.items())]
        return cls(data, {'columns': columns})

    def apply(self, dty: DataType, f: Callable[..., Opt[Value]]) -> 'Seq':
        names = self.columns
        data: List[Row] = [[f(**dict(zip(names, row)))] for (_, row) in self.iterrows()]
        column: Column = {'number': 1, 'name': '_', 'datatype': dty, 'null': ['']}
        return Seq(column, data)

    def withColumn(self, name: str, seq: 'Seq') -> 'DataFrame':
        df = self.drop([name]) if name in self.columns else self

        data = [list(row) + [value]
                for (row, value) in zip(df._data, seq.values)]
        col: Column = {'number': len(data[0]),
                       'name': name,
                       'datatype': seq.column['datatype'],
                       'null': seq.column['null']}
        schema: Schema = {'columns': df.schema['columns'] + [col]}
        return DataFrame(data, schema)

    def withColumnRenamed(self, old: str, new: str) -> 'DataFrame':
        schema: Schema = {'columns': [
            {'name': new if old == col['name'] else col['name'],
             'number': col['number'], 'datatype': col['datatype'], 'null': col['null']}
            for col in self.schema['columns']]}
        return DataFrame(self._data, schema)

    def select(self, *names: str) -> 'DataFrame':
        """i.e. project (but following pyspark API)

        Take care with column order:
        >>> df = DataFrame.from_records([dict(a=1, b=2)])
        >>> df.select('b', 'a')
        DataFrame({'b': 'number', 'a': 'number'})
        """
        schema = Schema(columns=[self.byName[n] for n in names])
        return DataFrame(self._data, schema)

    def drop(self, names: List[str]) -> 'DataFrame':
        schema = Schema(columns=[col for col in self.schema['columns']
                                 if col['name'] not in names])
        return DataFrame(self._data, schema)

    def __getitem__(self, which: Iterable[bool]) -> 'DataFrame':
        data = [row for (ok, row) in zip(which, self._data) if ok]
        return DataFrame(data, self.schema)

    def head(self, qty: int = 5) -> 'DataFrame':
        return DataFrame(self._data[:qty], self.schema)

    def filter(self, f: Callable[..., bool]) -> 'DataFrame':
        names = self.columns
        ok = (f(**dict(zip(names, row))) for (_, row) in self.iterrows())
        return self[ok]

    def sort_values(self, keys: List[str]) -> 'DataFrame':
        ixs = [self.byName[key]['number'] - 1 for key in keys]
        data = sorted(self._data, key=lambda row: tuple(row[ix] for ix in ixs))
        return DataFrame(data, self.schema)

    def merge(self, rt: 'DataFrame') -> 'DataFrame':
        """Natural join.

        Note: we assume they intersecting columns
        form a unique key on rt.
        """
        keys = (set(self.columns) & set(rt.columns))
        if not keys:
            raise ValueError

        def key_ixs(s: Schema) -> List[int]:
            return [col['number'] - 1 for col in s['columns']
                    if col['name'] in keys]

        def data_ixs(s: Schema) -> List[int]:
            return [col['number'] - 1 for col in s['columns']
                    if col['name'] not in keys]

        # key column indexes (right side)
        kx_r = key_ixs(rt.schema)
        # data columns indexes (rt)
        dx_r = data_ixs(rt.schema)
        kx_l = key_ixs(self.schema)
        dx_l = data_ixs(self.schema)
        # key -> non-key cols
        rtByKey = {tuple(row[kx] for kx in kx_r): [row[ix] for ix in dx_r]
                   for row in rt._data}

        # key column indexes (self, i.e. left side)
        def lookup(row_lt: Row) -> Iterable[Row]:
            key = tuple(row_lt[ix] for ix in kx_l)
            row_rt = rtByKey.get(key)
            if row_rt:
                yield [row_lt[ix] for ix in kx_l + dx_l] + row_rt

        data = [new for old in self._data for new in lookup(old)]

        # renumber rt columns
        out_cols = []  # type: List[Column]
        for col in self.schema['columns']:
            col = col.copy()
            col['number'] = len(out_cols) + 1
            out_cols.append(col)
        for col in rt.schema['columns']:
            if col['name'] in keys:
                continue
            col = col.copy()
            col['number'] = len(out_cols) + 1
            out_cols.append(col)

        schema: Schema = {'columns': out_cols}

        return DataFrame(data, schema)

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
        assert name != 'data', 'older API'
        col = self.byName.get(name)
        if not col:
            raise AttributeError(name)

        return Seq(col, self._data)


def concat(dfs: Iterable[DataFrame]) -> DataFrame:
    from functools import reduce
    dfs = iter(dfs)
    df0 = next(dfs)
    data = reduce(lambda acc, next: acc + next, (df._data for df in dfs), df0._data)
    return DataFrame(data, schema=df0.schema)


def meta_path(path: Path_T) -> Path_T:
    return path.parent / (path.stem + '-metadata.json')


def read_csv(path: Path_T,
             skiprows: int = 0,
             schema: Opt[Schema] = None) -> DataFrame:
    if schema is None:
        meta = json.load(meta_path(path).open())
        schema = meta['tableSchema']
    decode = DataFrame.decoder(schema)

    with path.open() as fp:
        reader = csv.reader(fp)
        for _ in range(skiprows):
            next(reader)
        # IDEA: check consistency between header and schema
        next(reader)
        return DataFrame((decode(row) for row in reader), schema)


def _decoders() -> Dict[DataType, Callable[[str], Value]]:
    def boolean(s: str) -> Value:
        return bool(s)

    def number(s: str) -> Value:
        return int(s)

    def text(s: str) -> Value:
        return s

    def date(s: str) -> Value:
        return dt.datetime.strptime(s.replace('-', '')[:8], '%Y%m%d').date()

    return {'number': number, 'string': text, 'boolean': boolean, 'date': date}


class Seq:
    def __init__(self, column: Column, data: List[Row]) -> None:
        self.column = column
        self._data = data

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({len(self._data)} x {self.column})'

    @classmethod
    def column_of(cls, val: Opt[Value],
                  null: List[str] = [''],
                  number: int = 1, name: Opt[str] = None) -> Column:
        if type(val) == int:
            datatype: DataType = 'number'
        elif isinstance(val, dt.date):
            datatype = 'date'
        elif isinstance(val, bool):
            datatype = 'boolean'
        else:
            datatype = 'string'
        return {'number': number, 'name': name or '_%d' % number,
                'datatype': datatype, 'null': null}

    @classmethod
    def from_value(cls, val: Opt[Value],
                   name: Opt[str] = None) -> 'Seq':
        return Seq(cls.column_of(val, name=name), [[val]])

    @classmethod
    def from_values(cls, vals: Iterable[Opt[Value]]) -> 'Seq':
        valiter = iter(vals)
        v0 = next(valiter)
        valiter = itertools.chain([v0], valiter)
        return cls(cls.column_of(v0), [[v] for v in valiter])

    def const(self, value: Opt[Value]) -> 'Seq':
        column = self.column_of(value)
        data: List[Row] = [[value] for _ in self._data]
        return Seq(column, data)

    def apply(self, f: Callable[[Opt[Value]], Opt[Value]]) -> 'Seq':
        column = self.column.copy()
        column['number'] = 1
        data: List[Row] = [[f(v)] for v in self.values]
        return Seq(column, data)

    def isin(self, those: Iterable[Opt[Value]]) -> Iterable[bool]:
        """
        >>> nums = Seq.from_values([1, 2, 3, 4])
        >>> odds = [1, 3, 5, 7]
        >>> list(nums.isin(odds))
        [True, False, True, False]
        """
        target = list(those)
        return (v in target for v in self.values)

    @property
    def values(self) -> List[Opt[Value]]:
        ix = self.column['number'] - 1
        return [row[ix] for row in self._data]

    def unique(self) -> List[Opt[Value]]:
        return list(set(self.values))

    def __eq__(self, val: object) -> List[bool]:  # type: ignore
        # Return type "List[bool]" of "__eq__" incompatible with return type "bool" in supertype "
        return [v == val for v in self.values]

    decoders = _decoders()

    @classmethod
    def decoder(cls, col: Column) -> Callable[[str], Opt[Value]]:
        def maybe(nulls: List[str], f: Callable[[str], Value]) -> Callable[[str], Opt[Value]]:
            def check(s: str) -> Opt[Value]:
                return None if s in nulls else f(s)
            return check

        def some(dty: Callable[[str], Value]) -> Callable[[str], Opt[Value]]:
            return dty

        dty = cls.decoders[col['datatype']]
        return maybe(col['null'], dty) if col['null'] else some(dty)


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
