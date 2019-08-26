from pathlib import Path as Path_T  # for type only
# ISSUE: new in 3.7; use importlib_resources to allow older python?
# https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package
from importlib import resources as res
from typing import List, Optional as Opt

from pyspark import SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from heron_staging.tumor_reg import seer_recode
from sql_script import SqlScript
import heron_load


class DataDictionary(object):
    filenames = [
        'record_layout.csv',
        'data_descriptor.csv',
        'item_description.csv',
        'section.csv',
    ]

    def __init__(self, dfs4: List[DataFrame]) -> None:
        [
            self.record_layout,
            self.data_descriptor,
            self.item_description,
            self.section,
        ] = dfs4

    @classmethod
    def make_in(cls, spark: SparkSession, data: Path_T) -> 'DataDictionary':
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
    def ont_view_in(cls, spark: SparkSession,
                    ddict: Path_T, recode: Path_T) -> DataFrame:
        DataDictionary.make_in(spark, ddict)
        cls.seer_terms_in(spark, recode)
        for view in cls.view_names:
            create_object(view, cls.script, spark)

        return spark.sql(f'select * from {cls.view_names[-1]}')

    @classmethod
    def seer_terms_in(cls, spark: SparkSession, recode: Path_T,
                      name: str = 'seer_site_terms') -> DataFrame:
        rules = seer_recode.Rule.from_lines(recode.open())
        terms = seer_recode.Rule.as_terms(rules)
        df = spark.createDataFrame(terms)
        df.createOrReplaceTempView(name)
        return df


def create_object(name: str, script: str, spark: SparkSession) -> None:
    ddl = SqlScript.find_ddl(name, script)
    spark.sql(ddl)


def csv_view(spark: SparkSession, path: Path_T,
             name: Opt[str] = None) -> DataFrame:
    spark.sparkContext.addFile(str(path))
    df = spark.read.csv(SparkFiles.get(path.name),
                        header=True, inferSchema=True)
    df.createOrReplaceTempView(name or path.stem)
    return df
