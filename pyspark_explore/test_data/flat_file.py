"""flat_file - read, write NAACCR flat file
"""
from typing import Iterator, List, Union

from pyspark.sql import functions as func, Column, Row
from pyspark.sql.dataframe import DataFrame
import pandas as pd  # type: ignore
import numpy as np  # type: ignore


# ISSUE: refactor overlap with tumor_reg_data
def naaccr_read_fwf(flat_file: DataFrame, record_layout: DataFrame,
                    value_col: str = 'value',
                    exclude_pfx: str = 'reserved') -> DataFrame:
    """
    @param flat_file: as from spark.read.text()
                      typically with .value
    @param record_layout: as from http://datadictionary.naaccr.org/?c=7
                          with .start, .length, .xmlId
    """
    fields = [
        func.substring(flat_file[value_col],
                       item.start, item.length).alias(item.xmlId)
        for item in record_layout.collect()
        if not item.xmlId.startswith(exclude_pfx)
    ]  # type: List[Union[Column, str]]
    return flat_file.select(fields)


def naaccr_make_fwf(data: pd.DataFrame,
                    record_layout: pd.DataFrame) -> Iterator[str]:
    r"""
    ack: Matt Kramer Mar 2016 https://stackoverflow.com/a/35974742
    >>> items = [Row(start=1, length=2, xmlId='c1'),
    ...          Row(start=3, length=1, xmlId='c2')]
    >>> data = pd.DataFrame(dict(c1=['x', 'y'], c2=['1', '2']))
    >>> for line in naaccr_make_fwf(data, items):
    ...     print(line.rstrip('\r\n'))
     x1
     y2
    """
    # TODO: numbers, dates

    items = record_layout.sort_values('start')

    # put blanks in any missing columns
    data = data.copy()
    for _, item in items.iterrows():
        if item.xmlId not in data.columns:
            data[item.xmlId] = ''

    # re-order data in record_layout order
    data = data[[item.xmlId for _, item in items.iterrows()]]

    spec = fmt_specs(items)
    for _, record in data.iterrows():
        sv_rec = record
        line = spec % tuple(record.fillna('').values)
        yield line
    pd.DataFrame(dict(spec=[spec],
                      items=[record_layout],
                      lines=[line],
                      records=[sv_rec],
                      dtypes=[data.dtypes])).to_pickle(',test-stuff.pkl')


def fmt_specs(items: pd.DataFrame) -> str:
    r"""
    >>> items = [Row(start=1, length=2), Row(start=3, length=4)]
    >>> fmt_specs(items)
    '%2.2s%4.4s\r\n'
    """
    pos = 1
    spec = ''
    for _, item in items.iterrows():
        # assert item.start == pos, (pos, item)
        if item.start != pos:
            import pdb; pdb.set_trace()
            spec += ' ' * (item.start - pos)
        spec += fmt_spec(item)
        pos = item.start + item.length
    spec += '\r\n'
    return spec


def fmt_spec(item: Row) -> str:
    """
    >>> item = Row(start=1, length=2)
    >>> spec = fmt_spec(item)
    >>> spec
    '%2.2s'
    """
    # ISSUE: all left aligned?
    return f'%{item.length}.{item.length}s'


def _integration_test(argv, stdout, cwd):
    [rl_fn] = argv[1:2]

    rl = pd.read_csv(cwd / rl_fn)
    spec = fmt_specs(rl.sort_values('start'))
    print('spec:', spec, file=stdout)

    data = pd.DataFrame(dict(primarySite=['x', 'y'],
                             recordType=['1', '2']))
    for line in naaccr_make_fwf(data, rl):
        print(line, file=stdout, end='')


if __name__ == '__main__':
    def _script_io():
        from sys import argv, stdout
        from pathlib import Path

        _integration_test(argv[:], stdout, cwd=Path('.'))

    _script_io()
