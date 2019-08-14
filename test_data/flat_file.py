"""flat_file - read, write NAACCR flat file
"""
from pathlib import Path as Path_T

from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame
from tabulate import tabulate


def naaccr_read_fwf(flat_file: DataFrame, record_layout: DataFrame,
                    value_col='value',
                    exclude_pfx='reserved'):
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
    ]
    return flat_file.select(fields)


def naaccr_write_fwf(dest: Path_T, data: DataFrame, record_layout: DataFrame):
    """
    ack: Matt Kramer Mar 2016 https://stackoverflow.com/a/35974742
    """
    with dest.open('w') as outfp:
        def write_part(records):
            txt = tabulate(records.toPandas().values.tolist(),
                           list(data.columns),
                           tablefmt="plain")
            outfp.write(txt)
        data.forEachPartition(write_part)


def _integration_test(argv, cwd, builder,
                      driver_memory='8g'):
    [ddict_dir, flat_file_name, out_filename] = argv[1:4]
    spark = (builder
             .appName('flat_file')
             .config('driver-memory', driver_memory)
             .getOrCreate())
    record_layout = read_csv(spark, ddict_dir / 'record_layout.csv')
    lines = spark.read.text(str(cwd / flat_file_name))
    extract = naaccr_read_fwf(lines, record_layout)
    naaccr_write_fwf(cwd / out_filename, extract, record_layout)


def read_csv(spark, path):
    df = spark.read.csv(str(path),
                        header=True, inferSchema=True)
    return df


if __name__ == '__main__':
    def _script_io():
        from sys import argv
        from pathlib import Path
        from pyspark.sql import SparkSession

        _integration_test(argv[:], Path('.'), SparkSession.builder)

    _script_io()
