# %% [markdown]
# # NAACCR Tumor Registry Data
#
# This is both a notebook and a module, sync'd using [jupytext][]. See also
#
#   - README for motivation and usage
#   - CONTRIBUTING for coding style etc.
#     - Note especially **ISSUE**, **TODO** and **IDEA** markers
#
# [jupytext]: https://github.com/mwouts/jupytext

# %% [markdown]
# ### Preface: PyData Tools: Pandas, PySpark
#
#

# %%
# python stdlib
from gzip import GzipFile
from importlib import resources as res
from pathlib import Path as Path_T
from pprint import pformat
from sys import stderr
from typing import Callable, ContextManager, Dict, Iterator, List, NoReturn
from typing import Optional as Opt, Union, cast
from xml.etree import ElementTree as XML
import logging


# %%
# 3rd party code: PyData
from pyspark.sql import SparkSession as SparkSession_T, Window
from pyspark.sql import types as ty, functions as func
from pyspark.sql.dataframe import DataFrame
from pyspark import sql as sq
import numpy as np   # type: ignore
import pandas as pd  # type: ignore

# %%
# 3rd party: naaccr-xml
import naaccr_xml_res  # ISSUE: symlink noted above
import naaccr_xml_samples
import naaccr_xml_xsd

import bc_qa

# %%
# this project
#from test_data.flat_file import naaccr_read_fwf  # ISSUE: refactor
from sql_script import SqlScript
from tumor_reg_ont import NAACCR_Layout, create_objects
import heron_load


# %%
log = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=stderr)
    log.info('NAACCR exploration...')


# %%
log.info('%s', dict(pandas=pd.__version__))

# %% [markdown]
# ## I/O Access: local files, Spark / Hive metastore

# %% [markdown]
# In a notebook context, we have `__name__ == '__main__'`.
#
# Otherwise, we maintain ocap discipline (see CONTRIBUTING)
# and don't import powerful objects.

# %% [markdown]
#  - **TODO/WIP**: use `_spark` and `_cwd`; i.e. be sure not to "export" ambient authority.

# %%
IO_TESTING = __name__ == '__main__'
_spark = cast(SparkSession_T, None)
if IO_TESTING:
    if 'spark' in globals():
        _spark = spark  # type: ignore  # noqa
        del spark       # type: ignore
    else:
        def _make_spark_session(appName: str = "tumor_reg_data") -> SparkSession_T:
            """
            ref:
            https://spark.apache.org/docs/latest/sql-getting-started.html
            """
            from pyspark.sql import SparkSession

            return SparkSession \
                .builder \
                .appName(appName) \
                .getOrCreate()
        _spark = _make_spark_session()

    def _get_cwd() -> Path_T:
        # ISSUE: ambient
        from pathlib import Path
        return Path('.')

    _cwd = _get_cwd()
    log.info('cwd: %s', _cwd.resolve())

# %% [markdown]
# The `spark` global is available when we launch as
# `PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook
#    pyspark ...`.

# %%
IO_TESTING and _spark

# %%
if IO_TESTING:
    log.info('spark web UI: %s', _spark.sparkContext.uiWebUrl)

# %% [markdown]
# ## `naaccr-xml` Data Dictionary

# %%
from tumor_reg_ont import XSD, NAACCR1, LOINC_NAACCR, ddictDF, NAACCR_I2B2

if IO_TESTING:
    ddictDF(_spark).createOrReplaceTempView('ndd180')
IO_TESTING and _spark.table('ndd180').limit(5).toPandas().set_index('naaccrId')

# %%
if IO_TESTING:
    _spark.sql("""create or replace temporary view current_task as select 'abc' task_id from (values('X'))""")
    _ont = NAACCR_I2B2.ont_view_in(_spark)  ## TODO: seer recode
    # _ont = NAACCR_I2B2.ont_view_in(_spark, _cwd / 'naaccr_ddict', _cwd / ',seer_site_recode.txt')  ## TODO: seer recode
IO_TESTING and _ont.limit(5).toPandas()

# %% [markdown]
# ## tumor_item_type: numeric /  date / nominal / text; identifier?

# %%
IO_TESTING and _spark.sql('''
select *
from ndd180 as idef
''').limit(8).toPandas()

# %%
from tumor_reg_ont import NAACCR_R

if IO_TESTING:
    NAACCR_R.field_info_in(_spark)
IO_TESTING and _spark.table('field_info').limit(5).toPandas()

# %% [markdown]
# `WerthPADOH/naaccr` has complete coverage:

# %%
# TODO: turn this into a doctest, independent of Spark
IO_TESTING and _spark.sql('''
with check as (
select case when r.item is null then 0 else 1 end as has_r
from ndd180 v18
left join field_info r on r.item = v18.naaccrNum
)
select has_r, count(*) from check
group by has_r

order by has_r
''').toPandas()

# %% [markdown]
# Werth assigns a `type` to each item:

# %%
IO_TESTING and _spark.sql('''
select rl.section, type, nd.length, count(*), collect_list(rl.`naaccr-item-num`), collect_list(naaccrId)
from ndd180 nd
left join field_info f on f.item = nd.naaccrNum
left join record_layout rl on rl.`naaccr-item-num` = nd.naaccrNum
group by section, type, nd.length
order by section, type, nd.length
''').toPandas()

# %% [markdown]
# ### Mixednaaccr-xml, LOINC, and Werth

# %%
if IO_TESTING:
    _spark.createDataFrame(NAACCR_I2B2.tumor_item_type).createOrReplaceTempView('tumor_item_type')

# %% [markdown]
# #### Any missing?

# %%
IO_TESTING and _spark.sql('''
select *
from tumor_item_type
where valtype_cd is null or  scale_typ is null
''').toPandas().sort_values(['sectionId', 'naaccrNum']).reset_index(drop=True)

# %% [markdown]
# #### Ambiguous valtype_cd?

# %%
IO_TESTING and _spark.sql('''
select naaccrId, length, count(distinct valtype_cd), collect_list(valtype_cd)
from tumor_item_type
group by naaccrId, length
having count(distinct valtype_cd) > 1
''').toPandas()

# %% [markdown]
# **ISSUE: LOINC mapping is ambiguous!**

# %%
IO_TESTING and _spark.sql('''
select naaccrId, count(distinct valtype_cd), collect_list(valtype_cd), collect_list(loinc_num)
from tumor_item_type
group by naaccrId
having count(*) > 1
''').toPandas()


# %%
def _save_mix(spark):
    (spark.table('tumor_item_type')
     .toPandas()
     .sort_values(['sectionId', 'naaccrNum'])
     .set_index('naaccrNum')
     .to_csv('tumor_item_type.csv')
    )


# %%
def csv_meta(dtypes: Dict[str, np.dtype], path: str,
             context: str = 'http://www.w3.org/ns/csvw') -> Dict[str, object]:
    # ISSUE: dead code? obsolete in favor of _fixna()?
    def xlate(dty: np.dtype) -> str:
        if dty.kind == 'i':
            return 'number'
        elif dty.kind == 'O':
            return 'string'
        raise NotImplementedError(dty.kind)

    cols = [
        {"titles": name,
         "datatype": xlate(dty)}
        for name, dty in dtypes.items()
    ]
    return {"@context": context,
            "url": path,
            "tableSchema": {
                "columns": cols
             }}

#@@ csv_meta(x.dtypes, 'tumor_item_type.csv')


# %%
def csv_spark_schema(columns: List[Dict[str, str]]) -> ty.StructType:
    """
    Note: limited to exactly 1 titles per column
    IDEA: expand to boolean
    IDEA: nullable / required
    """
    def oops(what: object) -> NoReturn:
        raise NotImplementedError(what)
    fields = [
        ty.StructField(
            name=col['titles'],
            dataType=ty.IntegerType() if col['datatype'] == 'number'
            else ty.StringType() if col['datatype'] == 'string'
            else oops(col))
        for col in columns]
    return ty.StructType(fields)

#@@ csv_spark_schema(csv_meta(x.dtypes, 'tumor_item_type.csv')['tableSchema']['columns'])


# %% [markdown]
# ## Coded Concepts

# %%
if IO_TESTING:
    LOINC_NAACCR.answers_in(_spark)

IO_TESTING and _spark.table('loinc_naaccr_answers').where('code_value = 380').limit(5).toPandas()

# %% [markdown]
# #### Werth Code Values

# %%
if IO_TESTING:
    _spark.createDataFrame(NAACCR_R.field_code_scheme).createOrReplaceTempView('field_code_scheme')
    _spark.createDataFrame(NAACCR_R.code_labels()).createOrReplaceTempView('code_labels')
IO_TESTING and _spark.table('code_labels').limit(5).toPandas().set_index(['item', 'name', 'scheme', 'code'])


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
from tumor_reg_ont import eltSchema, xmlDF, eltDict


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
                for itemRecord in eltDict(item, schema, simpleContent):
                    yield dict(itemRecord, rownum=rownum)

    itemSchema = eltSchema(NAACCR1.item_xsd, simpleContent=True)
    rownumField = ty.StructField('rownum', ty.IntegerType(), False)
    tumorItemSchema = ty.StructType([rownumField] + itemSchema.fields)
    data = xmlDF(spark, schema=tumorItemSchema, doc=doc, path='.//n:Tumor',
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


IO_TESTING and (naaccr_pivot(ddictDF(_spark),
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
    _extract = naaccr_read_fwf(_naaccr_text_lines, ddictDF(_spark))
    _extract.createOrReplaceTempView('naaccr_extract')
# _extract.explain()
IO_TESTING and non_blank(_extract.limit(5).toPandas())


# %%
def cancerIdSample(spark: SparkSession_T, cache: Path_T, tumors: DataFrame,
                   portion: float = 1.0, cancerID: int = 1) -> DataFrame:
    """Cancer Identification items from a sample

    """
    cols = NAACCR_I2B2.tumor_item_type
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
                    _key1).set_index(_key1)


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
        dd = ddictDF(spark)
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
    _ty = _spark.read.csv('heron_load/tumor_item_type.csv',
                          header=True, inferSchema=True)
    _ty.cache()
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
    _raw_obs.createOrReplaceTempView('naaccr_obs_raw')
IO_TESTING and _raw_obs.limit(10).toPandas()

# %%
if IO_TESTING:
    _script1 = SqlScript('naaccr_txform.sql',
                             res.read_text(heron_load, 'naaccr_txform.sql'),
                             [('tumor_item_value', ['naaccr_obs_raw'])])
    create_objects(_spark, _script1, naaccr_obs_raw=_raw_obs)
IO_TESTING and _spark.table('tumor_item_value').limit(10).toPandas()

# %%
if IO_TESTING:
    _script1 = SqlScript('naaccr_txform.sql',
                             res.read_text(heron_load, 'naaccr_txform.sql'),
                             [('tumor_reg_facts', ['record_layout', 'section'])])
    create_objects(_spark, _script1,
                   record_layout=_spark.createDataFrame(NAACCR_Layout.fields),
                   section=_spark.createDataFrame(NAACCR_I2B2.per_section))
IO_TESTING and _spark.table('tumor_reg_facts').limit(10).toPandas()

# %%
IO_TESTING and _spark.table('tumor_reg_facts').where("valtype_cd != '@'").limit(5).toPandas()


# %%
class ItemObs:
    script = SqlScript('naaccr_txform.sql',
                       res.read_text(heron_load, 'naaccr_txform.sql'),
                       [('tumor_item_value', ['naaccr_obs_raw']),
                        ('tumor_reg_facts', ['record_layout', 'section']),
                       ])

    extract_id_view = 'naaccr_extract_id'

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        item_ty = NAACCR_I2B2.item_views_in(spark)

        raw_obs = TumorKeys.with_tumor_id(naaccr_dates(
            stack_obs(extract, item_ty),
            TumorKeys.dtcols))

        views = create_objects(
            spark, cls.script,
            naaccr_obs_raw=raw_obs,
            # ISSUE: refactor item_views_in
            record_layout=spark.createDataFrame(NAACCR_Layout.fields),
            section=spark.createDataFrame(NAACCR_I2B2.per_section))

        return list(views.values())[-1]

    @classmethod
    def make_extract_id(cls,
                        spark: SparkSession_T,
                        extract: DataFrame) -> DataFrame:
        extract_id = TumorKeys.with_tumor_id(
            naaccr_dates(extract, TumorKeys.dtcols))
        extract_id.createOrReplaceTempView(cls.extract_id_view)
        return spark.table(cls.extract_id_view)

if IO_TESTING:
    _obs = ItemObs.make(_spark, _extract)
IO_TESTING and _obs.limit(5).toPandas()

# %%
IO_TESTING and ItemObs.make_extract_id(_spark, _extract).limit(5).toPandas()


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
        views = create_objects(spark, cls.script,
                               naaccr_extract_id=extract_id)
        return list(views.values())[-1]

IO_TESTING and SEER_Recode.make(_spark, _extract).limit(5).toPandas()


# %%
class SiteSpecificFactors:
    sql = res.read_text(heron_load, 'csschema.sql')
    script1 = SqlScript('csschema.sql', sql,
                        [('tumor_cs_schema', ['naaccr_extract_id'])])
    script2 = SqlScript('csschema.sql', sql,
                        [('cs_site_factor_facts', ['cs_obs_raw'])])

    items = [it for it in NAACCR1.items_180()
             if it['naaccrName'].startswith('CS Site-Specific Factor')]

    @classmethod
    def valtypes(cls) -> pd.DataFrame:
        factor_nums = [d['naaccrNum'] for d in cls.items]
        item_ty = NAACCR_I2B2.tumor_item_type[['naaccrNum', 'naaccrId', 'valtype_cd']]
        item_ty = item_ty[item_ty.naaccrNum.isin(factor_nums)]
        return item_ty

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        with_schema = cls.make_tumor_schema(spark, extract)
        ty_df = spark.createDataFrame(cls.valtypes())

        raw_obs = stack_obs(with_schema, ty_df,
                            key_cols=TumorKeys.key4 + TumorKeys.dtcols + ['recordId', 'cs_schema_name'])

        views = create_objects(spark, cls.script2,
                               cs_obs_raw=raw_obs)
        return list(views.values())[-1]

    @classmethod
    def make_tumor_schema(cls,
                          spark: SparkSession_T,
                          extract: DataFrame) -> DataFrame:
        views = create_objects(spark, cls.script1,
                               naaccr_extract_id=ItemObs.make_extract_id(spark, extract))
        return list(views.values())[-1]


if IO_TESTING:
    _ssf_facts = SiteSpecificFactors.make(_spark, _extract)
    assert _obs.columns == _ssf_facts.columns

IO_TESTING and _ssf_facts.limit(7).toPandas()

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
    items = bc_var.select(digits.cast('int').alias(item))   #@@.dropna().distinct()
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
        ddictDF(_spark),
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

# %% [markdown]
# **TODO**: date observations; treating `dateOfDiagnosis` as a coded observation leads to `concept_cd = 'NAACCR|390:20080627'`

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
