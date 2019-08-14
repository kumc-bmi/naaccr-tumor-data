"""tr_summary -- summarize NAACCR tumor registry file

Capture statistics useful for synthesizing data.

Usage:

  tumor_reg_summary naaccr_file ddict_dir tr_stats.parquet

where `ddict_dir` has output from from `../naaccr_ddict/scrape.py`.
"""

from importlib import resources as res
from pathlib import Path as Path_T  # use type only, per ocap discipline
from sys import stderr  # ocap exception for logging
from typing import (
    Any,
    Callable, Iterable,
    List,
)

from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame

import heron_load  # ISSUE: relative import?
from flat_file import naaccr_read_fwf
from tumor_reg_ont import create_object, DataDictionary  # ISSUE: relative import?


def main(argv: List[str], cwd: Path_T,
         builder: SparkSession_T.Builder,
         driver_memory: str = '16g',
         # TODO: don't limit input. bonus: sample
         tumor_limit: int = 50) -> None:
    [naaccr_file, ddict_dir, stats_out] = argv[1:4]
    spark = (builder
             .appName(__file__)
             .config('driver-memory', driver_memory)
             .getOrCreate())

    ndd = DataDictionary.make_in(spark, cwd / ddict_dir)
    data_raw = naaccr_read_fwf(
        spark.read.text(str(cwd / naaccr_file)),
        ndd.record_layout,
    )
    data_raw = data_raw.limit(tumor_limit)
    stats = DataSummary.nominal_stats(data_raw, spark, ndd)

    stats.to_csv(cwd / stats_out)
    print(stats.head(10))


def _log(*args: Any) -> None:
    print(*args, file=stderr)


class DataSummary(object):
    tumors_view = 'naaccr_extract'
    eav_view = 'tumors_eav'

    txform_script = res.read_text(heron_load, 'naaccr_txform.sql')
    txform_view = 'tumor_item_type'

    concepts_script = res.read_text(heron_load, 'naaccr_concepts_load.sql')
    t_item_view = 't_item'

    script = res.read_text(heron_load, 'data_char_sim.sql')
    views = ['data_agg_naaccr', 'data_char_naaccr']

    @classmethod
    def nominal_stats(cls, tumors_raw, spark,
                      ndd: DataDictionary):
        tumors_raw.createOrReplaceTempView(cls.tumors_view)

        create_object(cls.t_item_view, cls.concepts_script, spark)
        create_object(cls.txform_view, cls.txform_script, spark)
        tumors_eav = cls.stack_nominals(
            tumors_raw, ty=spark.table(cls.txform_view))
        tumors_eav.createOrReplaceTempView(cls.eav_view)
        tumors_eav.cache()
        # print? tumors_eav.limit(10).toPandas().set_index(['record', 'xmlId'])

        for view in cls.views:
            create_object(view, cls.script, spark)

        spark.catalog.cacheTable(cls.txform_view)

        # ISSUE: SQL goes in SQL files; move to data_char_sim.sql?
        stats = spark.sql('''
        select s.sectionId, rl.section, nom.*
        from data_agg_naaccr nom
        join (select xmlId, section from record_layout) rl
          on rl.xmlId = nom.xmlId
        join (select sectionId, section from section) s
          on s.section = rl.section
        ''').toPandas()
        return stats.set_index(['sectionId', 'section',
                                'itemnbr', 'xmlId', 'value']).sort_index()

    @classmethod
    def stack_nominals(cls, data, ty,
                       nominal_cd='@',
                       var_name='xmlId',
                       id_col='record'):
        value_vars = [row.xmlId
                      for row in ty.where(ty.valtype_cd == nominal_cd)
                      .collect()]
        df = melt(data.withColumn(id_col, func.monotonically_increasing_id()),
                  value_vars=value_vars, id_vars=[id_col], var_name=var_name)
        return df.where(func.trim(df.value) > '')


def melt(df: DataFrame,
         id_vars: Iterable[str], value_vars: Iterable[str],
         var_name: str = "variable", value_name: str = "value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""
    # ack: user6910411 Jan 2017 https://stackoverflow.com/a/41673644

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = func.array(*(
        func.struct(func.lit(c).alias(var_name), func.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", func.explode(_vars_and_vals))

    cols = id_vars + [
        func.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


if __name__ == '__main__':
    def _script() -> None:
        # Access ambient authority only when invoked as script,
        # as a form of ocap discipline.
        from sys import argv
        from pathlib import Path
        from pyspark.sql import SparkSession

        main(argv[:], Path('.'), SparkSession.builder)

    _script()
