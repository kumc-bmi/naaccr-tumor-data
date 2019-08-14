r"""tr_synthesize -- synthesize NAACCR tumor registry file

Usage::

  $ spark-submit tr_synthesize.py \
    data_agg_naaccr.csv ../naaccr_ddict test_data_out.flat.txt

where `data_agg_naaccr.csv` is from `tr_summarize.py`
and `../naaccr_ddict` includes `record_layout.csv` etc. from `scrape.py`.

You may need to use::

  $ PYTHONPATH=.. spark-submit tr_synthesize.py ...

due to an ISSUE with code organization.

And in case you have set `PYSPARK_DRIVER_PYTHON=jupyter` or the like,
be sure to override that::

  $ PYSPARK_DRIVER_PYTHON=python spark-submit ...

"""

from importlib import resources as res
from pathlib import Path as Path_T
from typing import List

from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql.dataframe import DataFrame
import pandas as pd  # type: ignore  # ISSUE: pandas stubs?

from tumor_reg_ont import create_object, DataDictionary  # ISSUE: relative import?
import flat_file
import heron_load  # ISSUE: relative import?


def main(argv: List[str], cwd: Path_T,
         builder: SparkSession_T.Builder,
         driver_memory: str = '16g') -> None:
    [stats_fn, ddict_dir, out_fn] = argv[1:4]

    spark = (builder
             .appName(__file__)
             .config('driver-memory', driver_memory)
             .getOrCreate())

    ndd = DataDictionary.make_in(spark, cwd / ddict_dir)
    job = SyntheticData(spark)
    stats = spark.read.csv(stats_fn,
                           header=True, inferSchema=True)
    records = job.synthesize_data(stats, ndd.record_layout)
    flat_file.naaccr_write_fwf_pd(cwd / out_fn, records, ndd.record_layout)


class SyntheticData(object):
    script = res.read_text(heron_load, 'data_char_sim.sql')
    views = ['data_char_naaccr', 'nominal_cdf', 'simulated_naaccr_nom']

    concepts_script = res.read_text(heron_load, 'naaccr_concepts_load.sql')
    t_item_view = 't_item'
    txform_script = res.read_text(heron_load, 'naaccr_txform.sql')
    txform_view = 'tumor_item_type'

    entity_view = 'simulated_entity'
    agg_view = 'data_agg_naaccr'

    def __init__(self, spark: SparkSession_T) -> None:
        self.__spark = spark

    def synthesize_data(self, stats_nom: DataFrame, record_layout: DataFrame,
                        qty: int = 500) -> pd.DataFrame:
        spark = self.__spark

        create_object(self.t_item_view, self.concepts_script, spark)
        create_object(self.txform_view, self.txform_script, spark)

        stats_nom.createOrReplaceTempView(self.agg_view)

        entity = spark.createDataFrame(
            [(ix,) for ix in range(1, qty)], ['case_index'])
        entity.createOrReplaceTempView(self.entity_view)
        # simulated_entity.limit(5).toPandas()

        for view in self.views:
            create_object(view, self.script, spark)
        spark.catalog.cacheTable(self.views[-1])

        # ISSUE: SQL goes in .sql files
        sim_records_nom = spark.sql('''
        select data.case_index, data.xmlId, data.value
        from simulated_naaccr_nom data
        join record_layout rl on rl.xmlId = data.xmlId
        join section on rl.section = section.section
        where sectionId = 1
        order by case_index, rl.start
        ''').toPandas()
        sim_records_nom = sim_records_nom.pivot(
            index='case_index', columns='xmlId', values='value')
        for col in sim_records_nom.columns:
            sim_records_nom[col] = sim_records_nom[col].astype('category')

        col_start = {row.xmlId: row.start for row in
                     record_layout.collect()}
        sim_records_nom = sim_records_nom[
            sorted(sim_records_nom.columns, key=lambda xid: col_start[xid])]
        return sim_records_nom


if __name__ == '__main__':
    def _script() -> None:
        # Encapsulate powerful objects.
        # See also ocap discipline section of CONTRIBUTING.md.
        from sys import argv
        from pathlib import Path
        from pyspark.sql import SparkSession

        main(argv[:], Path('.'), SparkSession.builder)

    _script()
