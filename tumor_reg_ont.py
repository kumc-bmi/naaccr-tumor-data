from pathlib import Path as Path_T  # for type only
# ISSUE: new in 3.7; use importlib_resources to allow older python?
# https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package
from importlib import resources as res
from typing import Iterable, Optional as Opt

from pyspark import SparkFiles
from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql.dataframe import DataFrame

from sql_script import SqlScript
import heron_load


class DataDictionary(object):
    filenames = [
        'record_layout.csv',
        'data_descriptor.csv',
        'item_description.csv',
        'section.csv',
    ]

    def __init__(self, dfs4: Iterable[DataFrame]) -> None:
        [
            self.record_layout,
            self.data_descriptor,
            self.item_description,
            self.section,
        ] = dfs4

    @classmethod
    def make_in(cls, spark: SparkSession_T, data: Path_T) -> 'DataDictionary':
        # avoid I/O in constructor
        return cls([csv_view(spark, data / name)
                    for name in cls.filenames])


class NAACCR_I2B2(object):
    view_names = [
        't_item',
        'naaccr_ont_aux',
        'naaccr_ontology',
    ]

    # ISSUE: ocap exception for "linking" design-time resources.
    # https://importlib-resources.readthedocs.io/en/latest/migration.html#pkg-resources-resource-string
    script = res.read_text(heron_load, 'naaccr_concepts_load.sql')

    @classmethod
    def ont_view_in(cls, spark: SparkSession_T, ddict: Path_T) -> DataFrame:
        DataDictionary.make_in(spark, ddict)
        for view in cls.view_names:
            create_object(view, cls.script, spark)

        return spark.table(cls.view_names[-1])


def create_object(name: str, script: str, spark: SparkSession_T) -> None:
    ddl = SqlScript.find_ddl(name, script)
    spark.sql(ddl)


def csv_view(spark: SparkSession_T, path: Path_T,
             name: Opt[str] = None) -> DataFrame:
    df = spark.read.csv(str(path),
                        header=True, escape='"', multiLine=True,
                        inferSchema=True, mode='FAILFAST')
    df.createOrReplaceTempView(name or path.stem)
    return df


def tumor_item_type(spark: SparkSession_T, cache: Path_T) -> DataFrame:
    DataDictionary.make_in(spark, cache)

    create_object('t_item',
                  res.read_text(heron_load, 'naaccr_concepts_load.sql'),
                  spark)

    create_object('tumor_item_type',
                  res.read_text(heron_load, 'naaccr_txform.sql'),
                  spark)
    spark.catalog.cacheTable('tumor_item_type')
    return spark.table('tumor_item_type')
