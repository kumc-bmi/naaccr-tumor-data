# %% [markdown]
# # Wrangling NAACCR Cancer Tumor Registry Data
#
# The [NAACCR_ETL][gpc] process used at KUMC and other GPC sites to load
# [NAACCR tumor registry data][dno] into [i2b2][] is
# **outdated by NAACCR version 18**. Addressing this issue ([GPC ticket #739][739])
# presents an opportunity to reconsider our platform and approach,
# a goal since opening [#44][44] in Feb 2014:
#
#   - Portable to database engines other than Oracle
#   - Accomodate NAACCR migration from flat file format to XML
#   - Explicit tracking of data flow to facilitate parallelism
#   - Rich test data to facilitate development without access to private data
#   - Separate repository from HERON EMR ETL to avoid
#     *information blocking* friction
#
# [gpc]: https://informatics.gpcnetwork.org/trac/Project/wiki/NAACCR_ETL
# [dno]: http://datadictionary.naaccr.org/
# [i2b2]: https://transmartfoundation.org/
# [44]: https://informatics.gpcnetwork.org/trac/Project/ticket/44
# [739]: https://informatics.gpcnetwork.org/trac/Project/ticket/739
#
#
# ## About This Document
#
# This is both a jupyter notebook and a python module, sync'd using [jupytext][].
#
# Collaborators are expected to be aware of coding style etc. as discussed in [CONTRIBUTING](CONTRIBUTING.md).
#
# [jupytext]: https://github.com/mwouts/jupytext

# %% [markdown]
# ## Platform for v18: Spark SQL, PySpark, and luigi
#
#  - [python 3.7 standard library](https://docs.python.org/3/library/index.html)
#  - [pyspark.sql API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
#  - [pandas](https://pandas.pydata.org/pandas-docs/stable/index.html)
#
# _Using luigi is beyond the scope of this document; see [tumor_reg_tasks](tumor_reg_tasks.py)._

# %%
# python 3.7 stdlib
from gzip import GzipFile
from importlib import resources as res
from pathlib import Path as Path_T
from sys import stderr
from typing import Callable, ContextManager, Dict, Iterator, List
from typing import Optional as Opt, Union, cast
from xml.etree import ElementTree as XML
import datetime as dt
import logging


# %%
from pyspark.sql import SparkSession as SparkSession_T, Window
from pyspark.sql import types as ty, functions as func
from pyspark.sql.dataframe import DataFrame
from pyspark import sql as sq
import pandas as pd  # type: ignore
import pyspark

# %% [markdown]
# We incorporate materials from the
# [imsweb/naaccr-xml](https://github.com/imsweb/naaccr-xml) project,
# such as test data.

# %%
import naaccr_xml_samples
import bc_qa

# %%
from sql_script import SqlScript
import tumor_reg_ont as ont
import heron_load


# %%
log = logging.getLogger(__name__)


# %% [markdown]
# ### I/O Access and Integration Testing: local files, Spark / Hive metastore

# %% [markdown]
# In a notebook context, we have `__name__ == '__main__'`.
# Exceptions to **ocap discipline** (see CONTRIBUTING) are guarded with `IO_TESTING`.
# Notebook-level globals such as `_spark` and `_cwd` use a leading underscore.

# %%
IO_TESTING = __name__ == '__main__'

# %%
if IO_TESTING:
    logging.basicConfig(level=logging.INFO, stream=stderr)
    log.info('tumor_reg_data using: %s', dict(
        pandas=pd.__version__,
        pyspark=pyspark.__version__,  # type: ignore
    ))

    def _get_cwd() -> Path_T:
        from pathlib import Path
        return Path('.')

    _cwd = _get_cwd()
    log.info('cwd: %s', _cwd.resolve())


# %% [markdown]
# The `spark` global is available when we launch as
# `PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook pyspark ...`.
# Otherwise, we make it using techniques from
# [Spark SQL Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html).

# %%
_spark = cast(SparkSession_T, None)  # for static type checking outside IO_TESTING

if IO_TESTING:
    if 'spark' in globals():
        _spark = spark  # type: ignore  # noqa
        del spark       # type: ignore
    else:
        def _make_spark_session(appName: str = "tumor_reg_data") -> SparkSession_T:
            from pyspark.sql import SparkSession

            return SparkSession \
                .builder \
                .appName(appName) \
                .getOrCreate()
        _spark = _make_spark_session()

    log.info('spark web UI: %s', _spark.sparkContext.uiWebUrl)

IO_TESTING and _spark


# %%
def _to_spark(name: str, compute: Callable[[], pd.DataFrame],
              cache: bool = False) -> Opt[DataFrame]:
    """Compute pandas data locally and save as spark view"""
    if not IO_TESTING:
        return None
    df = _spark.createDataFrame(compute())
    df.createOrReplaceTempView(name)
    if cache:
        df.cache()
    return df


def _to_view(name: str, compute: Callable[[], DataFrame],
             cache: bool = False) -> Opt[DataFrame]:
    """Save Spark DF as spark view"""
    if not IO_TESTING:
        return None
    df = compute()
    df.createOrReplaceTempView(name)
    if cache:
        df.cache()
    return df


def _SQL(sql: str,
         index: Opt[str] = None,
         limit: Opt[int] = 5) -> pd.DataFrame:
    """Run Spark SQL query and display results using .toPandas()"""
    if not IO_TESTING:
        return None
    df = _spark.sql(sql)
    if limit is not None:
        df = df.limit(limit)
    pdf = df.toPandas()
    if index is not None:
        pdf = pdf.set_index(index)
    return pdf


# %% [markdown]
# Spark logs can be extremely verbose (especially when running this notebook in batch mode).

# %%
def quiet_logs(spark: SparkSession_T) -> None:
    sc = spark.sparkContext
    # ack: FDS Aug 2015 https://stackoverflow.com/a/32208445
    logger = sc._jvm.org.apache.log4j  # type: ignore
    logger.LogManager.getLogger("org"). setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)


if IO_TESTING:
    quiet_logs(_spark)

# %% [markdown]
# ## NAACCR Items and Data Types
#
# Each NAACCR tumor record consists of hundreds of "items" (aka fields).
# The **naaccr-xml** project has data dictionaries to support
# reformatting flat files to XML and back:

# %%
_to_spark('ndd180', lambda: ont.NAACCR1.items_180())
_SQL('''
select * from ndd180 where naaccrNum between 400 and 500
''', index='naaccrNum')

# %% [markdown]
# But the naaccr-xml data dictionaries don't have enough information
# to determine i2b2's notion of datatype called `valtype_cd`:

# %%
IO_TESTING and pd.DataFrame.from_records(
    [('@', 'nominal'), ('N', 'numeric'), ('D', 'date'), ('T', 'text')],
    columns=['valtype_cd', 'description'],
    index='valtype_cd')

# %% [markdown]
# So we curated the datatypes, incorporating datatype
# information from LOINC (`loinc_num`, `scale_typ`, `AnswerListId`)
# and from Werth / PA DoH (`scheme`):

# %%
_to_spark('tumor_item_type', lambda: ont.NAACCR_I2B2.tumor_item_type)
_SQL('''
select * from tumor_item_type
''')

# %% [markdown]
# ### Integrity check, modulo confidential sections
#
# Any extra curated items not in the **naaccr-xml** v18 data dictionary?

# %%
_SQL('''
select * from tumor_item_type ty
left join ndd180 nd
on nd.naaccrNum = ty.naaccrNum and nd.naaccrId = ty.naaccrId
where nd.naaccrNum is null
''')

# %% [markdown]
# Not every item in **naaccr-xml** has a curated `valtype_cd`:

# %%
_SQL('''
select count(*)
from ndd180 nd
left join tumor_item_type ty
on nd.naaccrNum = ty.naaccrNum and nd.naaccrId = ty.naaccrId
where ty.naaccrNum is null or ty.valtype_cd is null
''')

# %% [markdown]
# We didn't bother curating `valtype_cd` for items in confidential sections.
#
# Section information is not provided in **naaccr-xml** so
# we get it from **imsweb/layout**:

# %%
_to_spark('record_layout', lambda: ont.NAACCR_Layout.fields)
_SQL("select * from record_layout where section like '%Confidential'")

# %% [markdown]
# Now we can see that the un-curated items are all from confidential sections:

# %%
_SQL('''
select distinct rl.section from ndd180 nd
left join record_layout rl on rl.`naaccr-item-num` = nd.naaccrNum
left join tumor_item_type ty
on nd.naaccrNum = ty.naaccrNum and nd.naaccrId = ty.naaccrId
where ty.naaccrNum is null or ty.valtype_cd is null
''')

# %% [markdown]
# #### Ambiguous valtype_cd?

# %%
_SQL('''
select naaccrId, length, count(distinct valtype_cd), collect_list(valtype_cd)
from tumor_item_type
group by naaccrId, length
having count(distinct valtype_cd) > 1
''', limit=None)

# %% [markdown]
# ## NAACCR Ontology

# %%
IO_TESTING and ont.NAACCR_I2B2.ont_view_in(
    _spark, task_id='task1', update_date=dt.date(2019, 9, 13))
_SQL('''
select * from naaccr_top_concept
''')

# %%
_SQL('''
select * from section_concepts order by sectionId
''')

# %%
_SQL('''
select * from item_concepts where naaccrNum between 400 and 450 order by naaccrNum
''')


# %% [markdown]
# ## Coded Concepts

# %%
# TODO: use these in preference to LOINC, Werth codes.

def field_doc(xmlId,
              subdir: str = 'naaccr18'):
    with ont.NAACCR_Layout._fields_doc() as doc_dir: #@@cls.
        field_path = (doc_dir / subdir / xmlId).with_suffix('.html')
        return ont._parse_html_fragment(field_path)


def item_codes(doc):
    rows = [
        (tr.find('td[@class="code-nbr"]').text,
         ''.join(tr.find('td[@class="code-desc"]').itertext()))
        for tr in doc.findall('./div/table/tr[@class="code-row"]')]
    return [(code, desc) for (code, desc) in rows if code]

item_codes(field_doc('recordType'))

# %%
# if IO_TESTING:
#    ont.LOINC_NAACCR.answers_in(_spark)

IO_TESTING and _spark.table('loinc_naaccr_answers').where('code_value = 380').limit(5).toPandas()

# %% [markdown]
# #### Werth Code Values

# %%
if IO_TESTING:
    _spark.createDataFrame(ont.NAACCR_R.field_code_scheme).createOrReplaceTempView('field_code_scheme')
    _spark.createDataFrame(ont.NAACCR_R.code_labels()).createOrReplaceTempView('code_labels')
IO_TESTING and _spark.table('code_labels').limit(5).toPandas().set_index(['item', 'name', 'scheme', 'code'])

# %%
_SQL('''
select answer_code, c_hlevel, sectionId, c_basecode, c_name, c_visualattributes, c_fullname
from code_concepts where naaccrNum = 610
''', index='answer_code')

# %% [markdown]
# ### Oncology MetaFiles from the World Health Organization
#
# [materials](http://www.who.int/classifications/icd/adaptations/oncology/en/index.html)

# %%
if IO_TESTING:
    _who_topo = ont.OncologyMeta.read_table((_cwd / ',cache').resolve(), *ont.OncologyMeta.topo_info)

IO_TESTING and _who_topo.set_index('Kode').head()

# %%
_to_spark('icd_o_topo', lambda: ont.OncologyMeta.icd_o_topo(_who_topo))
_SQL('select * from icd_o_topo order by path')

# %%
ont.NAACCR_I2B2.cs_terms

# %%
if IO_TESTING:
    ont.NAACCR_I2B2.ont_view_in(_spark, task_id='task1', update_date=dt.date(2019, 9, 13), who_cache=_cwd / ',cache')

_SQL(r'''
select * from primary_site_concepts
''')

# %%
_SQL(r'''
select * from seer_recode_concepts
''')


# %% [markdown]
# ## NAACCR XML Data

# %%
class NAACCR2:
    s100x = XML.parse(GzipFile(fileobj=res.open_binary(  # type: ignore # typeshed/issues/2580  # noqa
        naaccr_xml_samples, 'naaccr-xml-sample-v180-incidence-100.xml.gz')))

    @classmethod
    def s100t(cls) -> ContextManager[Path_T]:
        """
        TODO: check in results of converting from XML sample
        using `java -jar ~/opt/naaccr-xml-utility-6.2/lib/naaccr-xml-utility.jar`  # noqa
        """
        return res.path(
            naaccr_xml_samples, 'naaccr-xml-sample-v180-incidence-100.txt')


# %%
def tumorDF(spark: SparkSession_T, doc: XML.ElementTree) -> DataFrame:
    rownum = 0
    ns = {'n': 'http://naaccr.org/naaccrxml'}

    to_parent = {c: p for p in doc.iter() for c in p}

    def tumorItems(tumorElt: XML.Element, schema: ty.StructType,
                   simpleContent: bool = True) -> Iterator[Dict[str, object]]:
        nonlocal rownum
        assert simpleContent
        rownum += 1
        patElt = to_parent[tumorElt]
        ndataElt = to_parent[patElt]
        for elt in [ndataElt, patElt, tumorElt]:
            for item in elt.iterfind('./n:Item', ns):
                for itemRecord in ont.eltDict(item, schema, simpleContent):
                    yield dict(itemRecord, rownum=rownum)

    itemSchema = ont.eltSchema(ont.NAACCR1.item_xsd, simpleContent=True)
    rownumField = ty.StructField('rownum', ty.IntegerType(), False)
    tumorItemSchema = ty.StructType([rownumField] + itemSchema.fields)
    data = ont.xmlDF(spark, schema=tumorItemSchema, doc=doc, path='.//n:Tumor',
                     eltRecords=tumorItems,
                     ns={'n': 'http://naaccr.org/naaccrxml'},
                     simpleContent=True)
    return data.drop('naaccrNum')


IO_TESTING and (tumorDF(_spark, NAACCR2.s100x)
                .toPandas().sort_values(['naaccrId', 'rownum']).head(5))


# %% [markdown]
# What columns are covered by the 100 tumor sample?

# %%
IO_TESTING and (tumorDF(_spark, NAACCR2.s100x)  # type: ignore
                .select('naaccrId').distinct().sort('naaccrId')
                .toPandas().naaccrId.values)


# %%
def naaccr_pivot(ddict: DataFrame, skinny: DataFrame, key_cols: List[str],
                 pivot_on: str = 'naaccrId', value_col: str = 'value',
                 start: str = 'startColumn') -> DataFrame:
    groups = skinny.select(pivot_on, value_col, *key_cols).groupBy(*key_cols)
    wide = groups.pivot(pivot_on).agg(func.first(value_col))
    start_by_id = {id: start
                   for (id, start) in ddict.select(pivot_on, start).collect()}
    sorted_cols = sorted(wide.columns, key=lambda id: start_by_id.get(id, -1))
    return wide.select(cast(List[Union[sq.Column, str]], sorted_cols))


IO_TESTING and (naaccr_pivot(ont.ddictDF(_spark),
                             tumorDF(_spark, NAACCR2.s100x),
                             ['rownum'])
                .limit(3).toPandas())

# %% [markdown]
# ## NAACCR Flat File v18

# %%
if IO_TESTING:
    with NAACCR2.s100t() as _tr_file:
        log.info('tr_file: %s', _tr_file)
        _naaccr_text_lines = _spark.read.text(str(_tr_file))
else:
    _naaccr_text_lines = cast(DataFrame, None)

# %%
IO_TESTING and _naaccr_text_lines.rdd.getNumPartitions()

# %%
IO_TESTING and _naaccr_text_lines.limit(5).toPandas()


# %%
def non_blank(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        col for col in df.columns
        if (df[col].str.strip() > '').any()
    ]]


# %%
def naaccr_read_fwf(flat_file: DataFrame, itemDefs: DataFrame,
                    value_col: str = 'value',
                    exclude_pfx: str = 'reserved') -> DataFrame:
    """
    @param flat_file: as from spark.read.text()
                      typically with .value
    @param itemDefs: see ddictDF. ISSUE: should just use static CSV data now.
    """
    fields = [
        func.substring(flat_file[value_col],
                       item.startColumn, item.length).alias(item.naaccrId)
        for item in itemDefs.collect()
        if not item.naaccrId.startswith(exclude_pfx)
    ]  # type: List[Union[sq.Column, str]]
    return flat_file.select(fields)


_extract = cast(DataFrame, None)  # for static analysis when not IO_TESTING
if IO_TESTING:
    _extract = naaccr_read_fwf(_naaccr_text_lines, ont.ddictDF(_spark)).cache()
    _extract.createOrReplaceTempView('naaccr_extract')
# _extract.explain()
IO_TESTING and non_blank(_extract.limit(5).toPandas())


# %%
def cancerIdSample(spark: SparkSession_T, cache: Path_T, tumors: DataFrame,
                   portion: float = 1.0, cancerID: int = 1) -> DataFrame:
    """Cancer Identification items from a sample

    """
    cols = ont.NAACCR_I2B2.tumor_item_type
    cols = cols[cols.sectionId == cancerID]
    colnames = cols.naaccrId.values.tolist()
    # TODO: test data for morphTypebehavIcdO2 etc.
    colnames = [cn for cn in colnames if cn in tumors.columns]
    return tumors.sample(False, portion).select(colnames)


if False and IO_TESTING:
    _cancer_id = cancerIdSample(_spark, _cwd / 'naaccr_ddict', _extract)

False and IO_TESTING and non_blank(_cancer_id.limit(15).toPandas())

# %%
False and IO_TESTING and _cancer_id.toPandas().describe()


# %% [markdown]
# ## NAACCR Dates
#
#  - **ISSUE**: hide date flags in i2b2? They just say why a date is missing, which doesn't seem worth the screenspace.

# %%
def naaccr_dates(df: DataFrame, date_cols: List[str],
                 keep: bool = False) -> DataFrame:
    orig_cols = df.columns
    for dtcol in date_cols:
        strcol = dtcol + '_'
        df = df.withColumnRenamed(dtcol, strcol)
        dt = func.substring(func.concat(func.trim(df[strcol]), func.lit('0101')), 1, 8)
        # df = df.withColumn(dtcol + '_str', dt)
        dt = func.to_date(dt, 'yyyyMMdd')
        df = df.withColumn(dtcol, dt)
    if not keep:
        df = df.select(cast(Union[sq.Column, str], orig_cols))
    return df


IO_TESTING and naaccr_dates(
    _extract.select(['dateOfDiagnosis', 'dateOfLastContact']),
    ['dateOfDiagnosis', 'dateOfLastContact'],
    keep=True).limit(10).toPandas()


# %% [markdown]
# ### Strange dates: TODO?

# %%
def strange_dates(extract: DataFrame) -> DataFrame:
    x = naaccr_dates(extract.select(['dateOfDiagnosis']),
                     ['dateOfDiagnosis'], keep=True)
    x = x.withColumn('dtlen', func.length(func.trim(x.dateOfDiagnosis_)))
    x = x.where(x.dtlen > 0)
    x = x.withColumn('cc', func.substring(func.trim(x.dateOfDiagnosis_), 1, 2))

    return x.where(
        ~(x.cc.isin(['19', '20'])) |
        ((x.dtlen < 8) & (x.dtlen > 0)))


IO_TESTING and (strange_dates(_extract)
                .toPandas().groupby(['dtlen', 'cc']).count())


# %% [markdown]
# ## Patients, Tumors, Unique key columns
#
#  - `patientSystemIdHosp` - "This provides a stable identifier to
#    link back to all reported tumors for a patient. It also serves as
#    a reliable linking identifier; useful when central registries
#    send follow-up information back to hospitals. Other identifiers
#    such as social security number and medical record number, while
#    useful, are subject to change and are thus less useful for this
#    type of record linkage."
#
#  - `tumorRecordNumber` - "Description: A system-generated number
#     assigned to each tumor. The number should never change even if
#     the tumor sequence is changed or a record (tumor) is deleted.
#     Rationale: This is a unique number that identifies a specific
#     tumor so data can be linked. "Sequence Number" cannot be used as
#     a link because the number is changed if a report identifies an
#     earlier tumor or if a tumor record is deleted."
#
# Turns out to be not enough:

# %%
def dups(df_spark: DataFrame, key_cols: List[str]) -> pd.DataFrame:
    df_pd = df_spark.toPandas().sort_values(key_cols)
    df_pd['dup'] = df_pd.duplicated(key_cols, keep=False)
    return df_pd[df_pd.dup]


_key1 = ['patientSystemIdHosp', 'tumorRecordNumber']

IO_TESTING and dups(_extract.select('sequenceNumberCentral',
                                    'dateOfDiagnosis', 'dateCaseCompleted',
                                    *_key1),
                    _key1).set_index(_key1).head(10)


# %%
class TumorKeys:
    # issue: accessionNumberHosp is not unique
    pat_ids = ['patientSystemIdHosp', 'patientIdNumber']
    pat_attrs = pat_ids + ['dateOfBirth', 'dateOfLastContact',
                           'sex', 'vitalStatus']
    tmr_ids = ['tumorRecordNumber']
    tmr_attrs = tmr_ids + [
        'dateOfDiagnosis',
        'sequenceNumberCentral', 'sequenceNumberHospital', 'primarySite',
        'ageAtDiagnosis', 'dateOfInptAdm', 'dateOfInptDisch', 'classOfCase',
        'dateCaseInitiated', 'dateCaseCompleted', 'dateCaseLastChanged',
    ]
    report_ids = ['naaccrRecordVersion', 'npiRegistryId']
    report_attrs = report_ids + ['dateCaseReportExported']

    dtcols = ['dateOfBirth', 'dateOfDiagnosis', 'dateOfLastContact',
              'dateCaseCompleted', 'dateCaseLastChanged']
    key4 = [
        'patientSystemIdHosp',  # NAACCR stable patient ID
        'tumorRecordNumber',    # NAACCR stable tumor ID
        'patientIdNumber',      # patient_mapping
        'abstractedBy',         # IDEA/YAGNI?: provider_id
    ]
    @classmethod
    def pat_tmr(cls, spark: SparkSession_T,
                naaccr_text_lines: DataFrame) -> DataFrame:
        return cls._pick_cols(spark, naaccr_text_lines,
                              cls.tmr_attrs + cls.pat_attrs + cls.report_attrs)

    @classmethod
    def patients(cls, spark: SparkSession_T,
                 naaccr_text_lines: DataFrame) -> DataFrame:
        pat = cls._pick_cols(spark, naaccr_text_lines,
                             cls.pat_ids + cls.pat_attrs +
                             cls.report_ids + cls.report_attrs)
        # distinct() wasn't fixed until the 3.x pre-release
        # https://github.com/zero323/pyspark-stubs/pull/138 623b0c0330ef
        return pat.distinct()  # type: ignore

    @classmethod
    def _pick_cols(cls, spark: SparkSession_T,
                   naaccr_text_lines: DataFrame,
                   cols: List[str]) -> DataFrame:
        dd = ont.ddictDF(spark)
        pat_tmr = naaccr_read_fwf(
            naaccr_text_lines,
            dd.where(dd.naaccrId.isin(cols)))
        pat_tmr = naaccr_dates(pat_tmr,
                               [c for c in pat_tmr.columns
                                if c.startswith('date')])
        return pat_tmr

    @classmethod
    def with_tumor_id(cls, data: DataFrame,
                      name: str = 'recordId',
                      extra: List[str] = ['dateOfDiagnosis',
                                          'dateCaseCompleted'],
                      # keep recordId length consistent
                      extra_default: Opt[sq.Column] = None) -> DataFrame:
        # ISSUE: performance: add encounter_num column here?
        if extra_default is None:
            extra_default = func.lit('0000-00-00')
        id_col = func.concat(data.patientSystemIdHosp,
                             data.tumorRecordNumber,
                             *[func.coalesce(data[col], extra_default)
                               for col in extra])
        return data.withColumn(name, id_col)

    @classmethod
    def with_rownum(cls, tumors: DataFrame,
                    start: int = 1,
                    new_col: str = 'encounter_num',
                    key_col: str = 'recordId') -> DataFrame:
        # ISSUE: deid encounter_num further?
        tumors = tumors.withColumn(
            new_col,
            func.lit(start) +
            func.row_number().over(Window.orderBy(key_col)))
        return tumors

    @classmethod
    def export_patient_ids(cls, df: DataFrame, spark: SparkSession_T,
                           cdw: 'Account', schema: str,
                           tmp_table: str = 'NAACCR_PMAP',
                           id_col: str = 'patientIdNumber') -> None:
        log.info('writing %s to %s', id_col, tmp_table)
        cdw.wr(df.select(id_col).distinct().write,  # type: ignore
               tmp_table, mode='overwrite')

    @classmethod
    def with_patient_num(cls, df: DataFrame, spark: SparkSession_T,
                         cdw: 'Account', schema: str,
                         source: str,  # assumed injection-safe
                         tmp_table: str = 'NAACCR_PMAP',
                         id_col: str = 'patientIdNumber') -> DataFrame:
        cls.export_patient_ids(df, spark, cdw, schema,
                               id_col=id_col, tmp_table=tmp_table)
        q = f'''(
            select ea."{id_col}", pmap.PATIENT_NUM
            from {tmp_table} ea
            join {schema}.PATIENT_MAPPING pmap
            on pmap.patient_ide_source = '{source}'
            and ltrim(pmap.patient_ide, '0') = ltrim(ea."{id_col}", '0')
        )'''
        src_map = cdw.rd(spark.read, q)
        out = df.join(src_map, df[id_col] == src_map[id_col], how='left')
        out = out.drop(src_map[id_col])
        out = out.withColumnRenamed('PATIENT_NUM', 'patient_num')
        return out


# pat_tmr.cache()
if IO_TESTING:
    _pat_tmr = TumorKeys.with_rownum(TumorKeys.with_tumor_id(
        TumorKeys.pat_tmr(_spark, _naaccr_text_lines)))
    _patients = TumorKeys.patients(_spark, _naaccr_text_lines)
IO_TESTING and (_pat_tmr, _patients)

# %%
IO_TESTING and _pat_tmr.limit(15).toPandas()

# %%
IO_TESTING and _patients.limit(10).toPandas()


# %% [markdown]
# ##  Observations

# %%
def melt(df: DataFrame,
         id_vars: List[str], value_vars: List[str],
         var_name: str = 'variable', value_name: str = 'value') -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""
    # ack: user6910411 Jan 2017 https://stackoverflow.com/a/41673644

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = func.array(*(
        func.struct(func.lit(c).alias(var_name), func.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", func.explode(_vars_and_vals))

    cols = [func.col(v) for v in id_vars] + [
        func.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


# %%
if IO_TESTING:
    _to_spark('tumor_item_type', lambda: ont.NAACCR_I2B2.tumor_item_type, cache=True)
    _ty = _spark.table('tumor_item_type')
IO_TESTING and _ty.limit(5).toPandas()


# %% [markdown]
# **ISSUE**: performance: whenever we change cardinality, consider persisting the data. e.g. stack_obs

# %%
def stack_obs(records: DataFrame, ty: DataFrame,
              known_valtype_cd: List[str] = ['@', 'D', 'N', 'Ni', 'T'],
              key_cols: List[str] = TumorKeys.key4 + TumorKeys.dtcols) -> DataFrame:
    ty = ty.select('naaccrNum', 'naaccrId', 'valtype_cd')
    ty = ty.where(ty.valtype_cd.isin(known_valtype_cd))
    value_vars = [row.naaccrId for row in ty.collect()]
    obs = melt(records, key_cols, value_vars, var_name='naaccrId', value_name='raw_value')
    obs = obs.where("trim(raw_value) != ''")
    obs = obs.join(ty, ty.naaccrId == obs.naaccrId).drop(ty.naaccrId)
    return obs


if IO_TESTING:
    _raw_obs = TumorKeys.with_tumor_id(naaccr_dates(stack_obs(_extract, _ty), TumorKeys.dtcols))
    _to_view('naaccr_obs_raw', lambda: _raw_obs, cache=True)

_SQL('select * from naaccr_obs_raw', limit=10)


# %%
class ItemObs:
    script = SqlScript('naaccr_txform.sql',
                       res.read_text(heron_load, 'naaccr_txform.sql'),
                       [
                           ('tumor_item_value', ['naaccr_obs_raw']),
                           ('tumor_reg_facts', ['record_layout', 'section']),
                       ])

    extract_id_view = 'naaccr_extract_id'

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        item_ty = spark.createDataFrame(ont.NAACCR_I2B2.tumor_item_type)

        raw_obs = TumorKeys.with_tumor_id(naaccr_dates(
            stack_obs(extract, item_ty),
            TumorKeys.dtcols))

        views = ont.create_objects(
            spark, cls.script,
            naaccr_obs_raw=raw_obs,
            # ISSUE: refactor item_views_in
            record_layout=spark.createDataFrame(ont.NAACCR_Layout.fields),
            section=spark.createDataFrame(ont.NAACCR_I2B2.per_section))

        return list(views.values())[-1]

    @classmethod
    def make_extract_id(cls,
                        spark: SparkSession_T,
                        extract: DataFrame) -> DataFrame:
        extract_id = TumorKeys.with_tumor_id(
            naaccr_dates(extract, TumorKeys.dtcols))
        extract_id.createOrReplaceTempView(cls.extract_id_view)
        return spark.table(cls.extract_id_view)


_to_view('naaccr_observations1', lambda: ItemObs.make(_spark, _extract), cache=True)
if IO_TESTING:
    _obs = _spark.table('naaccr_observations1')
_SQL('select * from naaccr_observations1')

# %%
_SQL("select * from tumor_reg_facts where valtype_cd != '@'")

# %% [markdown]
# #### dateOfBirth regression test
#
# I originally thought the field (item) names used in
# [imsweb/layout](https://github.com/imsweb/layout/) and
# [imsweb/naaccr-xml](https://github.com/imsweb/naaccr-xml/)
# were supposed to correspond. But they don't. As a result,
# all date of birth (item 240) data was lost.
#
# See also [imsweb/layout/issues/72](https://github.com/imsweb/layout/issues/72).

# %%
_SQL('''
select * from naaccr_observations1 where concept_cd = 'NAACCR|240:'
''')

# %%
IO_TESTING and ItemObs.make_extract_id(_spark, _extract).limit(5).toPandas()


# %% [markdown]
# ### Coded Concepts from Data Summary

# %%
class DataSummary:
    script = SqlScript('data_char_sim.sql',
                       res.read_text(heron_load, 'data_char_sim.sql'),
                       [('data_agg_naaccr', ['naaccr_extract', 'tumors_eav', 'tumor_item_type']),
                        ('data_char_naaccr_nom', ['record_layout'])])

    @classmethod
    def nominal_stats(cls, tumors_raw: DataFrame, spark: SparkSession_T) -> DataFrame:
        to_df = spark.createDataFrame

        ty = to_df(ont.NAACCR_I2B2.tumor_item_type)
        views = ont.create_objects(spark, cls.script,
                                   section=to_df(ont.NAACCR_I2B2.per_section),
                                   record_layout=to_df(ont.NAACCR_Layout.fields),
                                   tumor_item_type=ty,
                                   naaccr_extract=tumors_raw,
                                   tumors_eav=cls.stack_nominals(tumors_raw, ty))
        return list(views.values())[-1]

    @classmethod
    def stack_nominals(cls, data: DataFrame, ty: DataFrame,
                       nominal_cd: str = '@',
                       var_name: str = 'naaccrId',
                       id_col: str = 'recordId') -> DataFrame:
        value_vars = [row.naaccrId
                      for row in ty.where(ty.valtype_cd == nominal_cd)
                      .collect()]
        df = melt(data.withColumn(id_col, func.monotonically_increasing_id()),
                  value_vars=value_vars, id_vars=[id_col], var_name=var_name)
        return df.where(func.trim(df.value) > '')


# if IO_TESTING:
#     DataSummary.stack_nominals(_extract, _ty).createOrReplaceTempView('tumors_eav')
# _SQL('select * from tumors_eav')

# %%
if IO_TESTING:
    DataSummary.nominal_stats(_extract, _spark)
    _spark.table('data_char_naaccr_nom').cache()
    _spark.table('item_concepts').cache()

_SQL('select * from data_char_naaccr_nom order by sectionId, naaccrNum, value', limit=15)

# %%
if IO_TESTING:
    _SQL('select * from data_char_naaccr_nom order by sectionId, naaccrNum, value', limit=None).to_csv(
        _cwd / 'naaccr_export_stats.csv', index=False)


# %% [markdown]
# ## SEER Site Recode

# %%
class SEER_Recode:
    script = SqlScript('seer_recode.sql',
                       res.read_text(heron_load, 'seer_recode.sql'),
                       [('seer_recode_aux', ['naaccr_extract_id']),
                        ('seer_recode_facts', [])])

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        extract_id = ItemObs.make_extract_id(spark, extract)
        views = ont.create_objects(spark, cls.script,
                                   naaccr_extract_id=extract_id)
        return list(views.values())[-1]


_to_view('naaccr_obs_seer', lambda: SEER_Recode.make(_spark, _extract), cache=True)
_SQL('select * from naaccr_obs_seer')


# %%
class SiteSpecificFactors:
    sql = res.read_text(heron_load, 'csschema.sql')
    script1 = SqlScript('csschema.sql', sql,
                        [('tumor_cs_schema', ['naaccr_extract_id'])])
    script2 = SqlScript('csschema.sql', sql,
                        [('cs_site_factor_facts', ['cs_obs_raw'])])

    items = [it for it in ont.NAACCR1.items_180()
             if it['naaccrName'].startswith('CS Site-Specific Factor')]

    @classmethod
    def valtypes(cls) -> pd.DataFrame:
        factor_nums = [d['naaccrNum'] for d in cls.items]
        item_ty = ont.NAACCR_I2B2.tumor_item_type[['naaccrNum', 'naaccrId', 'valtype_cd']]
        item_ty = item_ty[item_ty.naaccrNum.isin(factor_nums)]
        return item_ty

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        with_schema = cls.make_tumor_schema(spark, extract)
        ty_df = spark.createDataFrame(cls.valtypes())

        raw_obs = stack_obs(with_schema, ty_df,
                            key_cols=TumorKeys.key4 + TumorKeys.dtcols + ['recordId', 'cs_schema_name'])

        views = ont.create_objects(spark, cls.script2,
                                   cs_obs_raw=raw_obs)
        return list(views.values())[-1]

    @classmethod
    def make_tumor_schema(cls,
                          spark: SparkSession_T,
                          extract: DataFrame) -> DataFrame:
        views = ont.create_objects(spark, cls.script1,
                                   naaccr_extract_id=ItemObs.make_extract_id(spark, extract))
        return list(views.values())[-1]


if IO_TESTING:
    _ssf_facts = SiteSpecificFactors.make(_spark, _extract)
    assert _obs.columns == _ssf_facts.columns

_to_view('naaccr_obs_ssf', lambda: _ssf_facts, cache=True)
_SQL('select * from naaccr_obs_ssf', limit=7)

# %%
_SQL('''
create or replace temporary view naaccr_observations as
    select * from naaccr_observations1
    union all
    select * from naaccr_obs_seer
    union all
    select * from naaccr_obs_ssf
    ''')

if IO_TESTING:
    _spark.table('naaccr_observations').toPandas().to_csv(_cwd / 'naaccr_observations.csv', index=False)


# %% [markdown]
# ## Concept stats

# %%
class ConceptStats:
    script = SqlScript('concept_stats.sql',
                       res.read_text(heron_load, 'concept_stats.sql'),
                       [('concept_stats', ['ontology', 'observations'])])

    @classmethod
    def make(cls, spark: SparkSession_T,
             ontology: DataFrame, observations: DataFrame) -> DataFrame:
        views = ont.create_objects(spark, cls.script,
                                   ontology=ontology.cache(),
                                   observations=observations.cache())
        return list(views.values())[-1]


# if IO_TESTING:
#    ConceptStats.make(_spark,
#                      _spark.table('naaccr_ontology').sample(False, 0.01),
#                      _spark.table('naaccr_observations1'))
# _SQL('select * from concept_stats order by c_fullname', limit=15)

# %% [markdown]
# ## Oracle DB Access

# %% [markdown]
# We use `PYSPARK_SUBMIT_ARGS` to get JDBC jar in both
# `spark.driver.extraClassPath` and `--jars`:

# %%
if IO_TESTING:
    from os import environ as _environ
    log.info(_environ['PYSPARK_SUBMIT_ARGS'])


# %%
IO_TESTING and _spark.sparkContext.getConf().get('spark.driver.extraClassPath')

# %%
if IO_TESTING:
    def _set_pw(name: str = 'ID CDW') -> None:
        from os import environ
        from getpass import getpass
        password = getpass(name)
        environ[name] = password

    # _set_pw()


# %%
class Account:
    def __init__(self, user: str, password: str,
                 url: str = 'jdbc:oracle:thin:@localhost:8621:nheronB2',
                 driver: str = "oracle.jdbc.OracleDriver") -> None:
        self.url = url
        db = url.split(':')[-1]
        self.label = f'{self.__class__.__name__}({user}@{db})'
        self.__properties = {"user": user,
                             "password": password,
                             "driver": driver}

    def __repr__(self) -> str:
        return self.label

    def rd(self, io: sq.DataFrameReader, table: str) -> DataFrame:
        return io.jdbc(self.url, table,
                       properties=self.__properties)

    def wr(self, io: sq.DataFrameWriter, table: str,
           mode: Opt[str] = None) -> None:
        io.jdbc(self.url, table,
                properties=self.__properties,
                mode=mode)


DB_TESTING = IO_TESTING and 'ID CDW' in _environ
if DB_TESTING:
    _cdw = Account(_environ['LOGNAME'], _environ['ID CDW'])

DB_TESTING and _cdw.rd(_spark.read, "global_name").toPandas()


# %% [markdown]
#
#   - **ISSUE**: column name capitalization: `concept_cd` vs.
#     `CONCEPT_CD`, `dateOfDiagnosis` vs. `DATEOFDIAGNOSIS`
#     vs. `DATE_OF_DIAGNOSIS`.

# %%
def case_fold(df: DataFrame) -> DataFrame:
    """Fold column names to upper-case, following (Oracle) SQL norms.

    See also: upper_snake_case in pcornet_cdm
    """
    return df.toDF(*[n.upper() for n in df.columns])


# %%
if DB_TESTING:
    _patients_mapped = TumorKeys.with_patient_num(_patients, _spark, _cdw, 'NIGHTHERONDATA', 'SMS@kumed.com')

DB_TESTING and _patients_mapped.limit(5).toPandas()


# %% [markdown]
# ## Use Case: GPC Breast Cancer Survey
#
# The NAACCR format has 500+ items. To provide initial focus, let's use
# the variables from the 2016 GPC breast cancer survey:

# %%
class CancerStudy:
    bc_variable = pd.read_csv(res.open_text(bc_qa, 'bc-variable.csv'))


IO_TESTING and _spark.createDataFrame(
    CancerStudy.bc_variable).limit(5).toPandas()


# %%
def itemNumOfPath(bc_var: DataFrame,
                  item: str = 'item') -> DataFrame:
    digits = func.regexp_extract('concept_path',
                                 r'\\i2b2\\naaccr\\S:[^\\]+\\(\d+)', 1)
    items = bc_var.select(digits.cast('int').alias(item))   # TODO: .dropna().distinct()
    return items.sort(item)


IO_TESTING and itemNumOfPath(_spark.createDataFrame(
    CancerStudy.bc_variable)).limit(5).toPandas()


# %%
def _selectedItems(ddict: DataFrame, items: DataFrame) -> DataFrame:
    selected = ddict.join(items,
                          ddict.naaccrNum == items.item).drop(items.item)
    return selected.sort(selected.length.desc(), selected.naaccrNum)


if IO_TESTING:
    _bc_ddict = _selectedItems(
        ont.ddictDF(_spark),
        itemNumOfPath(_spark.createDataFrame(CancerStudy.bc_variable)),
    ).select('naaccrId', 'naaccrNum', 'parentXmlElement', 'length')

IO_TESTING and (
    _bc_ddict.select('naaccrId', 'naaccrNum', 'parentXmlElement', 'length')
    .toPandas().set_index(['naaccrNum', 'naaccrId'])
)


# %% [markdown]
# ### Patients, Encounters, and Observations per Variable
#
#   - **ISSUE**: naaccr-xml test data has no data on classOfCase etc.
#     at least not the 100 tumor sample.

# %%
def bc_var_facts(coded_facts: DataFrame, ddict: DataFrame) -> DataFrame:
    return coded_facts.join(
        ddict.select('naaccrId'),
        coded_facts.naaccrId == ddict.naaccrId,
    ).drop(ddict.naaccrId)


def data_summary(spark: SparkSession_T, obs: DataFrame) -> DataFrame:
    obs.createOrReplaceTempView('summary_input')  # ISSUE: CLOBBER!
    return spark.sql('''
    select naaccrId as variable
         , count(distinct patientIdNumber) as pat_qty
         , count(distinct recordId) as enc_qty
         , count(*) as fact_qty
    from summary_input
    group by naaccrId
    order by 2 desc, 3 desc, 4 desc
    ''')


def bc_var_summary(spark: SparkSession_T,
                   obs: DataFrame, ddict: DataFrame) -> DataFrame:
    agg = data_summary(
        spark,
        bc_var_facts(obs, ddict)
    )
    dd = ddict.select('naaccrId').withColumnRenamed('naaccrId', 'variable')
    return (dd
            .join(agg, dd.variable == agg.variable, how='left_outer')
            .drop(agg.variable))


IO_TESTING and bc_var_summary(
    _spark, _obs, _bc_ddict).where(
        'fact_qty is null').toPandas()

# %%
IO_TESTING and bc_var_summary(
    _spark, _obs, _bc_ddict).where(
        'fact_qty is not null').toPandas()

# %%
IO_TESTING and _obs.where("naaccrId == 'dateOfDiagnosis'").limit(5).toPandas()


# %% [markdown]
# #### TODO: Code labels; e.g. 1 = Male; 2 = Female

# %%
def pivot_obs_by_enc(skinny_obs: DataFrame,
                     pivot_on: str = 'naaccrId',  # cheating... not really in i2b2 observation_fact
                     # TODO: nval_num etc. for value cols?
                     value_col: str = 'concept_cd',
                     key_cols: List[str] = ['recordId', 'patientIdNumber']) -> DataFrame:
    groups = skinny_obs.select(pivot_on, value_col, *key_cols).groupBy(*key_cols)
    wide = groups.pivot(pivot_on).agg(func.first(value_col))
    return wide


IO_TESTING and pivot_obs_by_enc(_obs.where(
    _obs.naaccrId.isin(['dateOfDiagnosis', 'primarySite', 'sex', 'dateOfBirth'])
)).limit(5).toPandas().set_index(['recordId', 'patientIdNumber'])
