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
from typing import Callable, Dict, Iterator, List
from typing import Optional as Opt, Union, cast
from xml.etree import ElementTree as XML
import logging


# %%
# 3rd party code: PyData
from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql import types as ty, functions as func
from pyspark.sql.dataframe import DataFrame
from pyspark import sql as sq
import pandas as pd  # type: ignore

# %% [markdown]
#  - **ISSUE**: naaccr_xml stuff is currently symlink'd to a git
#    clone; `naaccr_xml_res` corresponds to
#    https://github.com/imsweb/naaccr-xml/blob/master/src/main/resources/

# %%
# 3rd party: naaccr-xml
import naaccr_xml_res  # ISSUE: symlink noted above
import naaccr_xml_samples
import naaccr_xml_xsd

import bc_qa

# %%
# this project
#from test_data.flat_file import naaccr_read_fwf  # ISSUE: refactor
from tumor_reg_ont import create_object, DataDictionary, tumor_item_type
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
class XSD:
    uri = 'http://www.w3.org/2001/XMLSchema'
    ns = {'xsd': uri}

    @classmethod
    def the(cls, elt: XML.Element, path: str,
            ns: Dict[str, str] = ns) -> XML.Element:
        """Get _the_ match for an XPath expression on an Element.

        The XML.Element.find() method may return None, but when
        we're dealing with static data that we know matches,
        we can refine the type by asserting that there's a match.

        """
        found = elt.find(path, ns)
        assert found, (elt, path)
        return found


class NAACCR1:
    """NAACCR XML assets

    Data dictionaries such as `ndd180` (version 18) contain `ItemDefs`:

    >>> def show(elt):
    ...     print(XML.tostring(elt).decode('utf-8').strip())
    >>> show(NAACCR1.ndd180.find('n:ItemDefs/n:ItemDef[3]', NAACCR1.ns))
    ... # doctest: +NORMALIZE_WHITESPACE
    <ns0:ItemDef xmlns:ns0="http://naaccr.org/naaccrxml"
      dataType="digits" length="3" naaccrId="naaccrRecordVersion"
      naaccrName="NAACCR Record Version" naaccrNum="50"
      parentXmlElement="NaaccrData" recordTypes="A,M,C,I" startColumn="17" />

    The XML schema for an `ItemDef` is:

    >>> show(NAACCR1.ItemDef)
    ... # doctest: +NORMALIZE_WHITESPACE
    <xs:element xmlns:xs="http://www.w3.org/2001/XMLSchema" name="ItemDef">
      <xs:complexType>
        <xs:attribute name="naaccrId" type="xsd:ID" use="required" />
        <xs:attribute name="naaccrNum" type="xsd:integer" use="required" />
        <xs:attribute name="naaccrName" type="xsd:string" use="optional" />
        <xs:attribute name="parentXmlElement" type="tns:parentType" use="required" />
        <xs:attribute default="text" name="dataType" type="tns:datatypeType" use="optional" />
        <xs:attribute default="rightBlank" name="padding" type="tns:paddingType" use="optional" />
        <xs:attribute default="all" name="trim" type="tns:trimType" use="optional" />
        <xs:attribute name="startColumn" type="xsd:integer" use="optional" />
        <xs:attribute name="length" type="xsd:integer" use="required" />
            <xs:attribute name="allowUnlimitedText" type="xsd:boolean" use="optional" />
        <xs:attribute name="sourceOfStandard" type="xsd:string" use="optional" />
        <xs:attribute name="recordTypes" type="xsd:string" use="optional" />
      </xs:complexType>
    </xs:element>
    """
    dd13_xsd = XML.parse(res.open_text(
        naaccr_xml_xsd, 'naaccr_dictionary_1.3.xsd'))

    ndd180 = XML.parse(res.open_text(
        naaccr_xml_res, 'naaccr-dictionary-180.xml'))

    data_xsd = XML.parse(res.open_text(
        naaccr_xml_xsd, 'naaccr_data_1.3.xsd'))

    s100x = XML.parse(GzipFile(fileobj=res.open_binary(  # type: ignore # typeshed/issues/2580
        naaccr_xml_samples, 'naaccr-xml-sample-v180-incidence-100.xml.gz')))

    @classmethod
    def s100t(cls):
        """
        TODO: check in results of converting from XML sample
        using `java -jar ~/opt/naaccr-xml-utility-6.2/lib/naaccr-xml-utility.jar`
        """
        return res.path(
            naaccr_xml_samples, 'naaccr-xml-sample-v180-incidence-100.txt')

    item_xsd = XSD.the(
        data_xsd.getroot(),
        './/xsd:complexType[@name="itemType"]/xsd:simpleContent/xsd:extension')

    ItemDef = XSD.the(dd13_xsd.getroot(), './/xsd:element[@name="ItemDef"]')

    uri = 'http://naaccr.org/naaccrxml'
    ns = {'n': uri}


# %%
def eltSchema(xsd_complex_type: XML.Element,
              simpleContent: bool = False) -> ty.StructType:
    decls = xsd_complex_type.findall('xsd:attribute', XSD.ns)
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
        fields = fields + [ty.StructField('value', ty.StringType(), False)]
    return ty.StructType(fields)

RecordMaker = Callable[[XML.Element, ty.StructType, bool],
                       Iterator[Dict[str, object]]]


def xmlDF(spark: SparkSession_T, schema: ty.StructType,
          doc: XML.ElementTree, path: str, ns: Dict[str, str],
          eltRecords: Opt[RecordMaker] = None,
          simpleContent: bool = False) -> DataFrame:
    getRecords = eltRecords or eltDict
    data = (record
            for elt in doc.iterfind(path, ns)
            for record in getRecords(elt, schema, simpleContent))
    return spark.createDataFrame(data, schema)  # type: ignore


def eltDict(elt: XML.Element, schema: ty.StructType,
            simpleContent: bool = False) -> Iterator[Dict[str, object]]:
    out = {k: int(v) if isinstance(schema[k].dataType, ty.IntegerType)
           else bool(v) if isinstance(schema[k].dataType, ty.BooleanType)
           else v
           for (k, v) in elt.attrib.items()}
    if simpleContent:
        out['value'] = elt.text
    # print("typed", s2s, out)
    yield out


def ddictDF(spark: SparkSession_T) -> DataFrame:
    return xmlDF(spark,
                 schema=eltSchema(XSD.the(NAACCR1.ItemDef, '*')),
                 doc=NAACCR1.ndd180,
                 path='./n:ItemDefs/n:ItemDef',
                 ns=NAACCR1.ns)


IO_TESTING and ddictDF(_spark).limit(5).toPandas().set_index('naaccrId')

# %% [markdown]
# ## NAACCR XML Data

# %%
eltSchema(NAACCR1.item_xsd, simpleContent=True)


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


IO_TESTING and (tumorDF(_spark, NAACCR1.s100x)
                .toPandas().sort_values(['naaccrId', 'rownum']).head(5))

# %%
#@@@@
# IO_TESTING and (tumorDF(_spark, NAACCR1.s100x)
#                 .select('naaccrId').distinct().sort('naaccrId')
#                 .toPandas().naaccrId.values)


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
                             tumorDF(_spark, NAACCR1.s100x),
                             ['rownum'])
                .limit(3).toPandas())

# %% [markdown]
# ## tumor_item_type: numeric /  date / nominal / text; identifier?
#
#  - **ISSUE**: emulating `t_item` from the 2012 MDB is awkward;
#    better to rewrite queries that use it in terms
#    of record_layout etc.

# %%
_spark.sql('''
select *
from ndd180 as idef
''').limit(8).toPandas()

# %% [markdown]
# Add year implemented, retired from chapter 10...

# %%
DataDictionary.make_in(_spark, _cwd / 'naaccr_ddict')

_spark.sql('''
select yr_retired, count(*)
from item_description idesc
where xmlId is null
group by yr_retired
order by yr_retired
''').toPandas()

# %% [markdown]
# #### gisCoordinateQuality Year Implemented = 366? srsly?
#
# Well, that's what [the spec](http://datadictionary.naaccr.org/default.aspx?c=10) says.

# %%
_spark.sql('''
select *
from item_description idesc
where yr_impl = '366'
''').toPandas()


# %% [markdown]
# ### LOINC SCALE_TYP should help

# %%
class LOINC_NAACCR:
    # spark CSV parser doesn't seem to be up to parsing these.
    measure = pd.read_csv('relma/loinc_naaccr.csv')
    measure = measure.where(measure.notnull(), None)
    answer = pd.read_csv('relma/loinc_naaccr_answer.csv')

_spark.createDataFrame(
    LOINC_NAACCR.measure[['LOINC_NUM', 'CODE_VALUE', 'SCALE_TYP', 'AnswerListId']],
    ty.StructType([ty.StructField(n, ty.StringType())
                   for n in ['LOINC_NUM', 'CODE_VALUE', 'SCALE_TYP', 'AnswerListId']])
).createOrReplaceTempView('loinc_naaccr')

LOINC_NAACCR.measure[['LOINC_NUM', 'CODE_VALUE', 'COMPONENT', 'SCALE_TYP', 'AnswerListId']].set_index(['LOINC_NUM', 'COMPONENT']).head()

# %%
LOINC_NAACCR.measure.groupby('SCALE_TYP')[['COMPONENT']].count()

# %% [markdown]
# **ISSUE: no xmlId** for 80+ retired items such as 2770 CS PRERX EXTENSION

# %% [markdown]
# ### LOINC crosswalk only covers about half of the v18 items
#
# Ideally I'd check by facts as well, but...

# %%
_spark.sql('''
with check as (
select idesc.yr_impl, idesc.yr_retired
     , case when ln.code_value is null then 0 else 1 end as has_loinc
from ndd180 v18
left join item_description idesc on idesc.item = v18.naaccrNum
left join loinc_naaccr ln on ln.code_value = v18.naaccrNum
)
select yr_impl, yr_retired, has_loinc, count(*) from check
group by yr_impl, yr_retired, has_loinc

union all select -1, -1, has_loinc, count(*) from check
group by has_loinc

order by has_loinc, yr_impl, yr_retired
''').toPandas()

# %% [markdown]
# How does LOINC do dates?

# %%
_spark.sql('''
select *
from loinc_naaccr ln
where ln.code_value = 390
''').toPandas()

# %% [markdown]
# ### Try Werth PADoH curation

# %%
import naaccr_r_raw

class NAACCR_R:
    # ISSUE: design-time or run-time? license?

    field_info = pd.read_csv(res.open_text(naaccr_r_raw, 'field_info.csv'))
    field_code_scheme = pd.read_csv(res.open_text(naaccr_r_raw, 'field_code_scheme.csv'))

    @classmethod
    def _code_labels(cls):
        return res.path(naaccr_r_raw, 'code-labels')

    @classmethod
    def field_info_in(cls, spark,
                      name='field_info'):
        info = spark.createDataFrame(cls.field_info)
        info.createOrReplaceTempView(name)

    @classmethod
    def code_labels(cls,
                    implicit=['iso_country']):
        found = []
        with cls._code_labels() as cl_dir:
            for scheme in cls.field_code_scheme.scheme.unique():
                if scheme in implicit:
                    continue
                info = (cl_dir / scheme).with_suffix('.csv')
                skiprows = 0
                if info.open().readline().startswith('#'):
                    skiprows = 1
                codes = pd.read_csv(info, skiprows=skiprows,
                                    na_filter=False,
                                    dtype={'code': str, 'label': str})
                codes['scheme'] = info.stem
                if 'code' not in codes.columns or 'label' not in codes.columns:
                    raise ValueError((info, codes.columns))
                found.append(codes)
        all_schemes = pd.concat(found)
        with_fields = cls.field_code_scheme.merge(all_schemes)
        with_field_info = cls.field_info[['item', 'name']].merge(with_fields)
        return with_field_info

NAACCR_R.field_info_in(_spark)
_spark.table('field_info').limit(5).toPandas()

# %% [markdown]
# `WerthPADOH/naaccr` has complete coverage:

# %%
_spark.sql('''
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
_spark.sql('''
select rl.section, type, nd.length, count(*), collect_list(rl.item), collect_list(naaccrId)
from ndd180 nd
left join field_info f on f.item = nd.naaccrNum
left join record_layout rl on rl.item = nd.naaccrNum
group by section, type, nd.length
order by section, type, nd.length
''').toPandas()

# %% [markdown]
# #### Werth Code Values

# %%
_spark.createDataFrame(NAACCR_R.field_code_scheme).createOrReplaceTempView('field_code_scheme')
_spark.createDataFrame(NAACCR_R.code_labels()).createOrReplaceTempView('code_labels')
_spark.table('code_labels').limit(5).toPandas().set_index(['item', 'name', 'scheme', 'code'])

# %% [markdown]
# ### Mix naaccr-xml, LOINC, and Werth

# %%
_spark.sql('''
create or replace temporary view tumor_item_type as
with src as (
select s.sectionId, rl.section, nd.parentXmlElement, nd.naaccrNum, nd.naaccrId
     , nd.dataType, nd.length, nd.allowUnlimitedText
     , idesc.source
     , ln.scale_typ, ln.AnswerListId
     , r.type r_type
     , case
         when r.type in ('city', 'census_tract', 'census_block', 'county', 'postal') then 'geo'
         when (naaccrId like 'date%Flag' or naaccrId like '%DateFlag') and nd.length = 2 then 'dateFlag'
         when AnswerListId is not null then AnswerListId
         when naaccrId in ('patientIdNumber', 'accessionNumberHosp', 'patientSystemIdHosp') then 'patientIdNumber'
         when naaccrId like 'pathOrderPhysLicNo%' and nd.length = 20 then 'physician'
         when naaccrId like 'pathReportNumber%' and nd.length = 20 then 'pathReportNumber'
         when naaccrId in ('censusIndCode2010', 'censusOccCode2010',
                           'schemaId', 'ajccId', 'primarySite', 'histologyIcdO2', 'histologicTypeIcdO3') then naaccrId
         when naaccrId in ('npiRegistryId') then naaccrId
         when naaccrId in ('reportingFacility', 'npiReportingFacility', 'npiArchiveFin')
           or naaccrId like 'pathReportingFacId%'
           or (naaccrId like '%FacNo%' and nd.length = 25)
           then 'facility'
         when naaccrId in ('vendorName') then naaccrId
         when (naaccrId like 'stateAtDxGeocode%') then 'state'
         when (naaccrId like 'secondaryDiagnosis%' and nd.length = 7) then 'ICD10'
         when (naaccrId like 'comorbidComplication%' and nd.length = 5) then 'ICD9'
         when naaccrId in ('diagnosticProc7387', 'gradeIcdO1') then '?'
         when naaccrId like 'subsqRx%RegLnRem' then 'LnRem?' -- ISSUE: Nom vs Ord?
         when naaccrId like 'subsqRx%ScopeLnSu' or naaccrId like 'subsqRx%SurgOth' then 'Surg?'
         when
           (source in ('SEER', 'AJCC', 'NPCR') and nd.length in (5, 13, 15) and dataType is null)
           or
           naaccrId in ('tnmPathDescriptor', 'tnmClinDescriptor')
           then 'staging'
         when naaccrId like 'csVersion%' then 'version'
         when s.section like 'Stage%' and sectionId = 11 and dataType is null and nd.length <= 5 then naaccrId
         when r.type = 'factor' and nd.length <= 5 then naaccrId
         when r.type in ('override', 'facility', 'boolean01') then r.type
       end nom_scheme
from ndd180 nd
left join record_layout rl on rl.item = nd.naaccrNum
left join item_description idesc on idesc.item = nd.naaccrNum
left join section s on s.section = rl.section
left join loinc_naaccr ln on ln.code_value = nd.naaccrNum
left join field_info r on r.item = nd.naaccrNum
)
,
with_scale as (
select sectionId, section, parentXmlElement, naaccrNum, naaccrId
     , dataType, length, allowUnlimitedText, source
     , AnswerListId
     , r_type
     , nom_scheme
     , case
       when scale_typ is not null then scale_typ
       when allowUnlimitedText then 'Nar'
       when nom_scheme is not null then 'Nom'
       when
         (dataType = 'date' and r_type = 'Date')
         or
         (r_type in ('integer', 'sentineled_integer', 'sentineled_numeric'))
       then 'Qn'
       end scale_typ
from src
)
, with_valtype as (
select with_scale.*
     , case  -- LOINC scale_typ -> i2b2 valtype_cd, identifier flag
       when naaccrId = 'ageAtDiagnosis' then 'Ni'
       when
         (scale_typ = 'Nar' and length >= 10)
         or
         (scale_typ = 'Nom' and
          (length >= 20
           or
           nom_scheme in ('patientIdNumber', 'facility', 'geo')))
       then 'Ti'
       when scale_typ = 'Nar' and AnswerListId is not null and length <= 2 then '@'
       when scale_typ = 'Qn' and (
         naaccrId like '%LabValue'
         or
         (dataType = 'digits' and length <= 6)
       ) then 'N'
       when dataType = 'date' and scale_typ = 'Qn' and length in (8, 14) then 'D'
       when nom_scheme in ('dateFlag', 'staging') then '@'
       when scale_typ in ('Nom', 'Ord') and (
         naaccrId in ('primarySite', 'histologyIcdO2', 'histologicTypeIcdO3') -- lists from WHO
         or
         naaccrId in ('censusIndCode2010', 'censusOccCode2010') -- Werth has these organized
         or
         nom_scheme in ('ICD9', 'ICD10', 'registryId', 'npiRegistryId', 'vendorName', 'version')
         or
         (AnswerListId is not null and length <= 4)
         or
         length <= 5
       ) then '@'
       when naaccrId in ('gradeIcdO1', 'siteIcdO1', 'histologyIcdO1', 'diagnosticProc7387',
                         'crcChecksum', 'unusualFollowUpMethod')
                         or
            (source in ('SEER', 'AJCC', 'NPCR') and length in (13, 15) and dataType is null)
        then '?'
       end as valtype_cd
from with_scale
)
select sectionId, section, parentXmlElement
     , naaccrNum, naaccrId
     -- , dataType
     , length
     , source
     , AnswerListId
     , scale_typ
     , valtype_cd
     , nom_scheme

from with_valtype
where section not like '%Confidential'
''')

(_spark.table('tumor_item_type')
 .toPandas()
 .sort_values(['sectionId', 'naaccrNum'])
 .set_index('naaccrNum')
 .to_csv('tumor_item_type.csv')
)

_spark.sql('''
select *
from tumor_item_type
where valtype_cd is null or  scale_typ is null
''').toPandas().sort_values(['sectionId', 'naaccrNum']).set_index('naaccrNum')

# %% [markdown]
# ### Curate item type rules spreadsheet?

# %%
_r = _spark.read.csv('heron_load/item_type_rules.csv', inferSchema=True, header=True)
_r.createOrReplaceTempView('item_type_rules')
_spark.table('item_type_rules').limit(5).toPandas()

# %%
ddictDF(_spark).createOrReplaceTempView('ndd180')
_spark.sql('''
create or replace temporary view tumor_item_type as
select rl.section
     , idef.naaccrNum
     , idef.naaccrId
     , idef.dataType
     , ln.scale_typ
     , idef.length
     , ir.valtype_cd
     , rulenum
     -- , max(ir.valtype_cd) as valtype_cd
from ndd180 as idef
left join loinc_naaccr ln on ln.code_value = idef.naaccrNum
left join (select item, section from record_layout) rl on rl.item = idef.naaccrNum
left join item_type_rules ir on
    (ir.naaccrId is null or ir.naaccrId = idef.naaccrId) and
    (ir.naaccrId_pat is null or idef.naaccrId like ir.naaccrId_pat) and
    (ir.dataType is null or ir.dataType = idef.dataType
     or (ir.dataType = '$NULL' and idef.dataType is null)) and
    (ir.scale_typ is null or ir.scale_typ = ln.scale_typ
     or (ir.scale_typ = '$NULL' and ln.scale_typ is null)) and
    ((ir.section_pat is null and rl.section not like '%Confidential')
      or rl.section like ir.section_pat) and
    (ir.length_min is null or idef.length >= ir.length_min) and
    (ir.length_max is null or idef.length <= ir.length_max)
/* group by ln.scale_typ
     , rl.section
     , idef.dataType
     , idef.naaccrId
     , idef.naaccrNum
     , idef.length */
order by section, naaccrNum
''')

_spark.sql('select * from tumor_item_type where valtype_cd is not null order by section, naaccrNum').toPandas()

# %% [markdown]
# #### Any Items with missing valtype_cd?

# %%
_spark.sql('''
select *
from tumor_item_type
where valtype_cd is null
order by section, length desc''').toPandas()

# %% [markdown]
# #### Ambiguios valtype_cd?

# %%
_spark.sql('''
select naaccrId, length, count(distinct valtype_cd), collect_list(valtype_cd), collect_list(rulenum)
from tumor_item_type
group by naaccrId, length
having count(distinct valtype_cd) > 1
''').toPandas()

# %%
_spark.sql('''
select *
from tumor_item_type
where naaccrId = 'countyCurrent'
order by section, length desc''').toPandas()

# %%
_spark.sql('''
select *
from item_type_rules
where rulenum in (31, 41)
''').toPandas()

# %%
_spark.sql('''
select *
from tumor_item_type
where scale_typ is not null and dataType is null and length >= 11
order by length
''').toPandas()

# %%
ddictDF(_spark).createOrReplaceTempView('ndd180')
_spark.sql('''
select dataType, length, ln.scale_typ, section
     , count(*), collect_list(naaccrNum), collect_list(naaccrId)
from ndd180 idef
left join loinc_naaccr ln on ln.code_value = idef.naaccrNum
left join (select item, section from record_layout) rl on rl.item = idef.naaccrNum
where naaccrNum not in (select naaccrNum from tumor_item_type where valtype_cd is not null)
group by dataType, length, ln.scale_typ, section
order by length desc, dataType, ln.scale_typ
''').toPandas()

# %%
_spark.sql('''
select *
from ndd180 idef
where length = 5 and dataType = 'digits' -- naaccrName like '%Regional Dose%'
order by idef.length desc
''').toPandas()

# %% [markdown]
# ISSUE
# phase1DosePerFraction
# Codes
# 00000	Radiation therapy was not administered
# 00001-99997	Record the actual Phase I dose delivered in cGy
# 99998	Not applicable, brachytherapy or radioisotopes administered to the patient
# 99999	Regional radiation therapy was administered but dose is unknown, it is unknown whether radiation therapy was administered. Death Certificate only.
#

# %%
# tumor item type from heron_load
ty_hl = pd.read_csv('heron_load/tumor_item_type.csv').rename(columns=lambda c: c.lower())
print(len(ty_hl.drop_duplicates()))
ty_hl.head()

# %%
ty2 = tumor_item_type(_spark, _cwd / 'naaccr_ddict')
print(ty2.distinct().count())
ty2.limit(5).toPandas()

# %%
ty_cmp = ty2.toPandas().merge(ty_hl, left_on='ItemNbr', right_on='itemnbr',
                              how='outer', suffixes=['_18', '_hl'])
ty_cmp = ty_cmp[['ItemNbr', 'valtype_cd_18', 'valtype_cd_hl', 'itemnbr', 'section', 'xmlId', 'itemname', 'FieldLength']]
ty_cmp['same'] = ty_cmp.valtype_cd_18.fillna('') == ty_cmp.valtype_cd_hl.fillna('')
ty_cmp[~ty_cmp.same & ~ty_cmp.valtype_cd_18.isnull() & ~ty_cmp.valtype_cd_hl.isnull()]

# %%
ty_cmp[~ty_cmp.valtype_cd_18.isnull() & ty_cmp.valtype_cd_hl.isnull()]

# %%
ty_cmp[ty_cmp.valtype_cd_18.isnull() & ~ty_cmp.valtype_cd_hl.isnull()]

# %%
_hl_missing = ty_cmp[ty_cmp.valtype_cd_18.isnull() & ~ty_cmp.valtype_cd_hl.isnull()].itemname
print(' or \n'.join(f'"{n}" is not null'
                    for n in _hl_missing))


# %%
IO_TESTING and (tumor_item_type(_spark, _cwd / 'naaccr_ddict')
                .limit(5).toPandas().set_index(['ItemNbr', 'xmlId']))

# %%
IO_TESTING and _spark.sql('''
select valtype_cd, count(*)
from tumor_item_type
group by valtype_cd
''').toPandas().set_index('valtype_cd')


# %%
def coded_items(tumor_item_type: DataFrame) -> DataFrame:
    return tumor_item_type.where("valtype_cd = '@'")


IO_TESTING and (coded_items(tumor_item_type(_spark, _cwd / 'naaccr_ddict'))
                .toPandas().tail())

# %% [markdown]
# ## NAACCR Flat File v18

# %%
if IO_TESTING:
    with NAACCR1.s100t() as _tr_file:
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
    @param itemDefs: see ddictDF
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

    TODO: remove limitation to coded items
    """
    cols = coded_items(tumor_item_type(spark, cache)).toPandas()
    cols = cols[cols.sectionid == cancerID]
    colnames = cols.xmlId.values.tolist()
    # TODO: test data for morphTypebehavIcdO2 etc.
    colnames = [cn for cn in colnames if cn in tumors.columns]
    return tumors.sample(False, portion).select(colnames)


if IO_TESTING:
    _cancer_id = cancerIdSample(_spark, _cwd / 'naaccr_ddict', _extract)

IO_TESTING and non_blank(_cancer_id.limit(15).toPandas())

# %%
_cancer_id.toPandas().describe()


# %% [markdown]
# ## NAACCR Dates

# %%
def naaccr_dates(df: DataFrame, date_cols: List[str],
                 keep: bool = False) -> DataFrame:
    orig_cols = df.columns
    for dtcol in date_cols:
        strcol = dtcol + '_'
        df = df.withColumnRenamed(dtcol, strcol)
        dt = func.to_date(df[strcol], 'yyyyMMdd')
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
# ## Unique key columns
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
    pat_ids = ['patientSystemIdHosp', 'patientIdNumber', 'accessionNumberHosp']
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
        dd = ddictDF(spark)
        pat_tmr = naaccr_read_fwf(
            naaccr_text_lines,
            dd.where(dd.naaccrId.isin(
                cls.tmr_attrs + cls.pat_attrs + cls.report_attrs)))
        # pat_tmr.createOrReplaceTempView('pat_tmr')
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
        if extra_default is None:
            extra_default = func.lit('0000-00-00')
        id_col = func.concat(data.patientSystemIdHosp,
                             data.tumorRecordNumber,
                             *[func.coalesce(data[col], extra_default)
                               for col in extra])
        return data.withColumn(name, id_col)


# pat_tmr.cache()
if IO_TESTING:
    _pat_tmr = TumorKeys.with_tumor_id(
        TumorKeys.pat_tmr(_spark, _naaccr_text_lines))
IO_TESTING and _pat_tmr

# %%
IO_TESTING and _pat_tmr.limit(15).toPandas()


# %% [markdown]
# ## Coded observations

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
def naaccr_coded_obs(records: DataFrame, ty: DataFrame) -> DataFrame:
    value_vars = [row.xmlId for row in
                  ty.where(ty.valtype_cd == '@').collect()]
    # TODO: test data for morphTypebehavIcdO2 etc.
    value_vars = [xmlId for xmlId in value_vars if xmlId in records.columns]

    dated = naaccr_dates(records, TumorKeys.dtcols)
    df = melt(dated,
              TumorKeys.key4 + TumorKeys.dtcols,
              value_vars, var_name='xmlId', value_name='code')
    return df.where(func.trim(df.code) > '')


if IO_TESTING:
    _coded = naaccr_coded_obs(_extract.sample(True, 0.02),
                              tumor_item_type(_spark, _cwd / 'naaccr_ddict'))
    _coded = TumorKeys.with_tumor_id(_coded)

    _coded.createOrReplaceTempView('tumor_coded_value')
# coded.explain()
IO_TESTING and _coded.limit(10).toPandas().set_index(['recordId', 'xmlId'])


# %%
def naaccr_coded_obs2(spark: SparkSession_T, items: DataFrame,
                      rownum: str = 'rownum',
                      naaccrId: str = 'naaccrId') -> DataFrame:
    # TODO: test data for dateCaseCompleted, dateCaseLastChanged,
    #       patientSystemIdHosp? abstractedBy?
    # TODO: or: test that patientIdNumber, tumorRecordNumber is unique?
    key_items = items.where(
        items[naaccrId].isin(TumorKeys.key4 + TumorKeys.dtcols))
    # return key_items
    key_rows = naaccr_pivot(ddictDF(spark), key_items, [rownum])
    # TODO: test data for dateCaseCompleted?
    key_rows = naaccr_dates(key_rows, [c for c in TumorKeys.dtcols
                                       if c in key_rows.columns])
    coded_obs = (key_rows
                 .join(items, items[rownum] == key_rows[rownum])
                 .drop(items[rownum]))
    coded_obs = (coded_obs
                 # ISSUE: naaccrId vs. xmlId
                 .withColumnRenamed('naaccrId', 'xmlId')
                 .withColumnRenamed('rownum', 'recordId')
                 # ISSUE: test data for these? make them optiona?
                 .withColumn('abstractedBy', func.lit('@@'))
                 .withColumn('dateCaseLastChanged', func.lit('@@')))
    return coded_obs.withColumnRenamed('value', 'code')


if IO_TESTING:
    _tumor_coded_value = naaccr_coded_obs2(_spark,
                                           tumorDF(_spark, NAACCR1.s100x))

IO_TESTING and _tumor_coded_value.limit(15).toPandas().set_index(
    ['recordId', 'xmlId'])

# %%
naaccr_txform = res.read_text(heron_load, 'naaccr_txform.sql')
if IO_TESTING:
    _tumor_coded_value.createOrReplaceTempView('tumor_coded_value')  # ISSUE: CLOBBER!
    create_object('tumor_reg_coded_facts', naaccr_txform, _spark)
    _tumor_reg_coded_facts = _spark.table('tumor_reg_coded_facts')
    _tumor_reg_coded_facts.printSchema()

# TODO: check for nulls in update_date, start_date, end_date, etc.
IO_TESTING and _tumor_reg_coded_facts.limit(5).toPandas()

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

    _set_pw()


# %%
class Account:
    def __init__(self, user: str, password: str,
                 url: str = 'jdbc:oracle:thin:@localhost:1521:nheronA1',
                 driver: str = "oracle.jdbc.OracleDriver") -> None:
        self.url = url
        self.__properties = {"user": user,
                             "password": password,
                             "driver": driver}

    def rd(self, io: sq.DataFrameReader, table: str) -> DataFrame:
        return io.jdbc(self.url, table,
                       properties=self.__properties)

    def wr(self, io: sq.DataFrameWriter, table: str,
           mode: Opt[str] = None) -> None:
        io.jdbc(self.url, table,
                properties=self.__properties,
                mode=mode)


if IO_TESTING:
    _cdw = Account(_environ['LOGNAME'], _environ['ID CDW'])

IO_TESTING and _cdw.rd(_spark.read, "global_name").toPandas()

# %% [markdown]
#
#   - **ISSUE**: column name capitalization: `concept_cd` vs.
#     `CONCEPT_CD`, `dateOfDiagnosis` vs. `DATEOFDIAGNOSIS`
#     vs. `DATE_OF_DIAGNOSIS`.

# %%
if IO_TESTING:
    _cdw.wr(_tumor_reg_coded_facts.write, "TUMOR_REG_CODED_FACTS",
             mode='overwrite')


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
        coded_facts.xmlId == ddict.naaccrId,
    ).drop(ddict.naaccrId)


def data_summary(spark: SparkSession_T, obs: DataFrame) -> DataFrame:
    obs.createOrReplaceTempView('summary_input')  # ISSUE: CLOBBER!
    return spark.sql('''
    select xmlId as variable
           -- ISSUE: rename MRN back to patientIdNumber?
         , count(distinct MRN) as pat_qty
         , count(distinct encounter_ide) as enc_qty
         , count(*) as fact_qty
    from summary_input
    group by xmlId
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
    _spark, _tumor_reg_coded_facts, _bc_ddict).where(
        'fact_qty is null').toPandas()

# %%
IO_TESTING and bc_var_summary(
    _spark, _tumor_reg_coded_facts, _bc_ddict).where(
        'fact_qty is not null').toPandas()

# %% [markdown]
# **TODO**: date observations; treating `dateOfDiagnosis` as a coded observation leads to `concept_cd = 'NAACCR|390:20080627'`

# %%
IO_TESTING and  _tumor_reg_coded_facts.where("xmlId == 'dateOfDiagnosis'").limit(5).toPandas()


# %% [markdown]
# #### TODO: Code labels; e.g. 1 = Male; 2 = Female

# %%
def pivot_obs_by_enc(skinny_obs: DataFrame,
                     pivot_on: str = 'xmlId',  # cheating... not really in i2b2 observation_fact
                     # TODO: nval_num etc. for value cols?
                     value_col: str = 'concept_cd',
                     key_cols: List[str] = ['encounter_ide', 'MRN']) -> DataFrame:
    groups = skinny_obs.select(pivot_on, value_col, *key_cols).groupBy(*key_cols)
    wide = groups.pivot(pivot_on).agg(func.first(value_col))
    return wide

IO_TESTING and pivot_obs_by_enc(_tumor_reg_coded_facts.where(
    _tumor_reg_coded_facts.xmlId.isin(['dateOfDiagnosis', 'primarySite', 'sex', 'dateOfBirth'])
)).limit(5).toPandas().set_index(['encounter_ide', 'MRN'])


# %% [markdown]
# ## Synthesizing Data
#
# Let's take stats gathered about a NAACCR file and synthesize data with similar characteristics.
#
# **ISSUE**: combine with OMOP cohort based on syn-puf?

# %%
def define_simulated_naaccr(spark: SparkSession_T, data_agg_naaccr: DataFrame) -> SparkSession_T:
    data_agg_naaccr.createOrReplaceTempView('data_agg_naaccr')
    simulated_entity = spark.createDataFrame([(ix,) for ix in range(1, 500)], ['case_index'])
    simulated_entity.createOrReplaceTempView('simulated_entity')
    # simulated_entity.limit(5).toPandas()
    create_object('data_char_naaccr',
              res.read_text(heron_load, 'data_char_sim.sql'),
              spark)
    create_object('nominal_cdf',
              res.read_text(heron_load, 'data_char_sim.sql'),
              spark)
    create_object('simulated_naaccr_nom',
              res.read_text(heron_load, 'data_char_sim.sql'),
              spark)
    spark.catalog.cacheTable('simulated_naaccr_nom')
    return spark


IO_TESTING and (
    define_simulated_naaccr(_spark,
                            _spark.read.csv('test_data/,data_agg_naaccr_all.csv',
                                            header=True, inferSchema=True))
    .table('nominal_cdf')
    .limit(10).toPandas()
)


# %% [markdown]
# For **nominal data**, what's the prevalence of each value of each variable?
#
# Let's compare observed with synthesized:

# %%
def codedObservedDistribution(spark: SparkSession_T, naaccrId: str) -> pd.DataFrame:
    stats = spark.table('data_agg_naaccr').toPandas()
    byval = stats[stats.xmlId == naaccrId].set_index('value')
    return byval[['itemnbr', 'freq', 'tumor_qty', 'pct']]


def codedSyntheticDistribution(spark: SparkSession_T, itemnbr: int) -> pd.DataFrame:
    itemnbr = int(itemnbr)  # prevent SQL injection
    obs_sim = spark.sql(f'''
    select *
    from simulated_naaccr_nom
    where itemnbr = {itemnbr}
    ''').toPandas().set_index('case_index')
    sim_by_val = obs_sim.groupby('value').count()
    pct = sim_by_val.itemnbr * 100 / len(obs_sim)
    return pct

IO_TESTING and (
    codedObservedDistribution(_spark, 'sequenceNumberCentral')
    .assign(pct_syn=codedSyntheticDistribution(_spark, 380)))

# %%
IO_TESTING and (
    codedObservedDistribution(_spark, 'sequenceNumberCentral')
    .assign(pct_syn=codedSyntheticDistribution(_spark, 380))[['pct', 'pct_syn']]
    .plot.pie(figsize=(12, 8), subplots=True)
);

# %% [markdown]
# **TODO**: For dates, how long before/after diagnosis?
#
# For diagnosis, how long ago?

# %% [markdown]
# ### checking synthetic data

# %%
if IO_TESTING:
    _syn_records = pd.read_pickle('test_data/,syn_records_TMP.pkl')
    _coded_items = coded_items(tumor_item_type(_spark, _cwd / 'naaccr_ddict')).toPandas()
    non_blank(_syn_records[_coded_items[
        (_coded_items.sectionid == 1) &
        (_coded_items.xmlId.isin(_syn_records.columns))].xmlId.values.tolist()]).tail(15)
    ###

    stuff = pd.read_pickle('test_data/,test-stuff.pkl')
    stuff.iloc[0]['lines']

    ###

    ndd = DataDictionary.make_in(_spark, _cwd / 'naaccr_ddict')
    _test_data_coded = naaccr_read_fwf(_spark.read.text('test_data/,test_data.flat.txt'), ndd.record_layout)
    _test_data_coded.limit(5).toPandas()

    ###

    xp = _test_data_coded.select(_coded_items[_coded_items.sectionid == 1].xmlId.values.tolist()).limit(15).toPandas()

    xp[[
        col for col in xp.columns
        if (xp[col].str.strip() > '').any()
    ]]

# %% [markdown]
# ## Diagnosed before born??

# %%
if IO_TESTING:
    x = naaccr_dates(_pat_tmr, ['dateOfDiagnosis', 'dateOfBirth']).toPandas()
    x['ddx_orig'] = _pat_tmr.select('dateOfDiagnosis', 'dateOfDiagnosisFlag').toPandas().dateOfDiagnosis
    x = x[x.ageAtDiagnosis.str.startswith('-')]
    x['age2'] = (x.dateOfDiagnosis - x.dateOfBirth).dt.days / 365.25
    x[['ageAtDiagnosis', 'age2', 'ddx_orig', 'dateOfDiagnosis', 'dateOfDiagnosisFlag', 'dateOfBirth']].sort_values('ddx_orig')


# %%
if IO_TESTING:
    _dx_age = _pat_tmr.toPandas().groupby('ageAtDiagnosis') #@@pd?
    _dx_age[['dateOfBirth']].count()
    #dx_age = dx_age[dx_age != '999']
    #dx_age.unique()

    #dx_age = dx_age.astype(int)
    #dx_age.describe()
