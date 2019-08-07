from pathlib import Path as Path_T  # for type only

from pyspark import SparkFiles

from sql_script import SqlScript


class DataDictionary(object):
    filenames = [
        'record_layout.csv',
        'data_descriptor.csv',
        'item_description.csv',
        'section.csv',
    ]

    def __init__(self, dfs4):
        [
            self.record_layout,
            self.data_descriptor,
            self.item_description,
            self.section,
        ] = dfs4

    @classmethod
    def make_in(cls, spark, data):
        # avoid I/O in constructor
        return cls([csv_view(spark, data / name)
                    for name in cls.filenames])


class NAACCR_I2B2(object):
    view_names = [
        't_item',
        'naaccr_ont_aux',
        'naaccr_ontology',
    ]

    script_name = 'naaccr_concepts_load.sql'

    @classmethod
    def ont_view_in(cls, spark, ddict: Path_T, scripts: Path_T):
        DataDictionary.make_in(spark, ddict)
        for view in cls.view_names:
            create_object(view, scripts / cls.script_name, spark)

        return spark.sql(f'select * from {cls.view_names[-1]}')


def create_object(name: str, path: Path_T, spark) -> None:
    ddl = SqlScript.find_ddl_in(name, path)
    spark.sql(ddl)


def csv_view(spark, path,
             name=None):
    spark.sparkContext.addFile(str(path))
    df = spark.read.csv(SparkFiles.get(path.name),
                        header=True, inferSchema=True)
    df.createOrReplaceTempView(name or path.stem)
    return df
