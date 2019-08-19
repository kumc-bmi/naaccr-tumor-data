from pathlib import Path as Path_T  # for type only
# ISSUE: new in 3.7; use importlib_resources to allow older python?
# https://stackoverflow.com/questions/6028000/how-to-read-a-static-file-from-inside-a-python-package
from importlib import resources as res
from typing import Iterable, Optional as Opt
from xml.etree import ElementTree as ET

from pyspark import SparkFiles
from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql import types as ty
from pyspark.sql.dataframe import DataFrame

from sql_script import SqlScript
import heron_load
# https://github.com/imsweb/naaccr-xml/blob/master/src/main/resources/
import naaccr_xml_res
# https://github.com/imsweb/naaccr-xml/blob/master/src/main/resources/xsd/
import naaccr_xml_xsd


class NaaccrXML:
    dd_xsd = ET.parse(res.open_text(naaccr_xml_xsd,
                                    'naaccr_dictionary_1.3.xsd'))
    dd_180 = ET.parse(res.open_text(naaccr_xml_res,
                                    'naaccr-dictionary-180.xml'))


def xmlDF(spark, schema, doc, path, ns):
    def typed(s2s):
        out = {k: int(v) if isinstance(schema[k].dataType, ty.IntegerType)
               else bool(v) if isinstance(schema[k].dataType, ty.BooleanType)
               else v
               for (k, v) in s2s.items()}
        # print("typed", s2s, out)
        return out
    data = (typed(elt.attrib) for elt in doc.iterfind(path, ns))
    return spark.createDataFrame(data, schema)


def eltSchema(xsd_complex_type,
              simpleContent=False):
    ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}
    decls = xsd_complex_type.findall('xsd:attribute', ns)
    fields = [
        ty.StructField(
            name=d.attrib['name'],
            dataType=ty.IntegerType() if d.attrib['type'] == 'xsd:integer'
            else ty.BooleanType() if d.attrib['type'] == 'xsd:boolean'
            else ty.StringType(),
            nullable=d.attrib.get('use') != 'required',
            # IDEA/YAGNI?: use pd.Categorical for xsd:enumeration
            # e.g. tns:parentType
            metadata=d.attrib)
        for d in decls]
    if simpleContent:
        fields = fields + ty.StructField('value', ty.StringType, False)
    return ty.StructType(fields)


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
    spark.sparkContext.addFile(str(path))
    df = spark.read.csv(SparkFiles.get(path.name),
                        header=True, inferSchema=True)
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
