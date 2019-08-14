"""flat_file - read, write NAACCR flat file
"""
from typing import Iterable, List, Union
from pathlib import Path as Path_T

from pyspark.sql import functions as func, Column, Row
from pyspark.sql.dataframe import DataFrame
from tabulate import tabulate
import pandas as pd


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


def naaccr_write_fwf_pd(dest: Path_T,
                        data: pd.DataFrame, record_layout: DataFrame) -> None:
    """
    ack: Matt Kramer Mar 2016 https://stackoverflow.com/a/35974742
    """
    items = record_layout.collect()
    with dest.open('w') as outfp:
        txt = tabulate(data.values.tolist(),
                       list(data.columns),
                       tablefmt="plain")  # TODO: items.start/end -> tablefmt
        outfp.write(txt)


def naaccr_write_fwf_spark(dest: Path_T,
                     data: DataFrame, record_layout: DataFrame) -> None:
    """
    ack: Matt Kramer Mar 2016 https://stackoverflow.com/a/35974742
    """
    items = record_layout.collect()
    with dest.open('w') as outfp:
        def write_part(rows: Iterable[Row]) -> None:
            txt = tabulate([[row[item.xmlId] for item in items]
                            for row in rows],
                           list(data.columns),
                           tablefmt="plain")
            outfp.write(txt)
        data.foreachPartition(write_part)
