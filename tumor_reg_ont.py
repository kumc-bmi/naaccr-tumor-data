from importlib import resources as res
from pathlib import Path as Path_T  # for type only
from typing import (
    Callable, ContextManager, Dict, Iterator, List, Optional as Opt,
    Tuple, Union, NoReturn,
    cast,
)
from xml.etree import ElementTree as XML
import datetime as dt
import logging
import zipfile

from mypy_extensions import TypedDict  # ISSUE: dependency?
from pyspark.sql import SparkSession as SparkSession_T
from pyspark.sql import types as ty
from pyspark.sql.dataframe import DataFrame
import pandas as pd  # type: ignore
import numpy as np   # type: ignore

from sql_script import SqlScript
import heron_load
import loinc_naaccr  # included with permission
import naaccr_layout
import naaccr_r_raw
import naaccr_xml_res
import naaccr_xml_xsd

log = logging.getLogger(__name__)


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

        IDEA: move this to an XPath class
        """
        found = elt.find(path, ns)
        assert found is not None, (elt, path)
        return found

    types: Dict[str, Callable[[str], Union[int, bool]]] = {
        'xsd:integer': int,
        'xsd:boolean': bool,
    }


Field0 = TypedDict('Field0', {
    'short-label': str,
    'start': int, 'end': int,
    'naaccr-item-num': int,
    'section': str,
    'grouped': bool,
})


def to_field0(elt: XML.Element) -> Field0:
    attr = elt.attrib
    return {
        'short-label': attr['short-label'],
        'start': int(attr['start']), 'end': int(attr['end']),
        'naaccr-item-num': int(attr['naaccr-item-num']),
        'section': attr['section'],
        'grouped': len(elt) > 0,
    }


Field = TypedDict('Field', {
    'short-label': str,
    'start': int, 'end': int,
    'length': int,
    'naaccr-item-num': int,
    'section': str,
    'grouped': bool,
})


def to_field(f: Field0) -> Field:
    return {
        'short-label': f['short-label'],
        'start': f['start'], 'end': f['end'],
        'length': f['end'] + 1 - f['start'],
        'naaccr-item-num': f['naaccr-item-num'],
        'section': f['section'],
        'grouped': f['grouped'],
    }


class NAACCR_Layout:
    """NAACCR Record Layout XML assets.

    ack: https://github.com/imsweb/layout
    56eaf0c on Jul 26
    ref https://github.com/imsweb/naaccr-xml/issues/156
    https://github.com/imsweb/layout/blob/master/src/main/resources/layout/fixed/naaccr/naaccr-18-layout.xml

    IDEA: specify column types for CSV data with
          https://www.w3.org/TR/tabular-data-primer/#datatypes

    >>> NAACCR_Layout.fields[:3]
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [{'short-label': 'Rec Type', 'start': 1, 'end': 1, 'length': 1, ...},
     {'short-label': 'Reg Type', 'start': 2, ..., 'section': 'Record ID', ...},
     {'short-label': 'NAACCR Rec Ver', ... 'naaccr-item-num': 50, ...}]

    >>> for info in list(NAACCR_Layout.fields_source())[:3]:
    ...     print(info)
    (570, 'abstractedBy', 'CoC')
    (550, 'accessionNumberHosp', 'CoC')
    (70, 'addrAtDxCity', 'CoC')


    >>> def check_fkey(src, k, dest):
    ...    range = set(dest)
    ...    return [rec for rec in src if rec[k] not in range]

    >>> check_fkey(NAACCR_Layout.fields, 'section', NAACCR_I2B2.per_section.section.values)
    []

    >>> check_fkey(NAACCR1.items_180(), 'naaccrNum',
    ...            [f['naaccr-item-num'] for f in NAACCR_Layout.fields])
    []

    ISSUE: Not sure about these two oddballs...

    >>> check_fkey([f for f in NAACCR_Layout.fields if not f['grouped']],
    ...            'naaccr-item-num',  ## TODO
    ...            [i['naaccrNum'] for i in NAACCR1.items_180()])
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [{'short-label': 'NPCR Spec Fld', ..., 'naaccr-item-num': 3720, ..., 'grouped': False},
     {'short-label': 'State Req Item', ..., 'naaccr-item-num': 2220, ..., 'grouped': False}]
    """
    layout_180 = XML.parse(res.open_text(
        naaccr_layout, 'naaccr-18-layout.xml'))

    # All but reserved fields (both groups and parts)
    fields_raw = [to_field0(f)
                  for f in layout_180.findall('.//field')
                  if f.attrib.get('naaccr-item-num') and
                  not f.attrib['name'].startswith('reserved')]

    # include length
    fields: List[Field] = [to_field(f) for f in fields_raw]

    @classmethod
    def _fields_doc(cls) -> ContextManager[Path_T]:
        return res.path(naaccr_layout, 'doc')

    @classmethod
    def fields_source(cls) -> Iterator[Tuple[int, str, Opt[str]]]:
        for info in cls.fields_doc():
            if info['xmlId'].startswith('reserved'):
                continue
            yield (int(info['Item #']), info['xmlId'],
                   info.get('Source of Standard'))

    @classmethod
    def fields_doc(cls,
                   subdir: str = 'naaccr18') -> Iterator[Dict[str, str]]:
        with cls._fields_doc() as doc_dir:
            for field_path in sorted((doc_dir / subdir).glob('*.html')):
                doc = cls.field_doc(field_path)
                if doc:
                    yield doc

    @classmethod
    def field_doc(cls, path: Path_T) -> Opt[Dict[str, str]]:
        doc = _parse_html_fragment(path)
        naaccr_xml_elt = doc.find('./strong')
        if naaccr_xml_elt is None:
            return None
        naaccr_xml = (naaccr_xml_elt.tail or '')[2:].strip()  # </strong>: ...
        [parentElement, xmlId] = naaccr_xml.split('.')
        summary_table = XSD.the(
            doc, './table[@class="naaccr-summary-table naaccr-borders"]', {})
        [hd, detail] = summary_table.findall('tr')
        summary = dict(zip([cast(str, th.text) for th in hd.findall('th')],
                           [cast(str, td.text) for td in detail.findall('td')]))
        return dict(summary,
                    parentElement=parentElement,
                    xmlId=xmlId)


def _parse_html_fragment(path: Path_T) -> XML.Element:
    markup = path.open().read()
    markup = '''
    <!DOCTYPE html [
    <!ENTITY nbsp "&#160;" >
    ]>
    <html>''' + markup + '</html>'
    return XML.fromstring(markup)


ItemDef_T = TypedDict('ItemDef_T', {
    'naaccrId': str,
    'naaccrNum': int,
    'naaccrName': str,
    'parentXmlElement': str,
    'startColumn': int,
    'length': int,
})


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
        <xs:attribute name="parentXmlElement" type="tns:parentType"
                      use="required" />
        <xs:attribute default="text" name="dataType" type="tns:datatypeType"
                      use="optional" />
        <xs:attribute default="rightBlank" name="padding"
                      type="tns:paddingType" use="optional" />
        <xs:attribute default="all" name="trim" type="tns:trimType"
                      use="optional" />
        <xs:attribute name="startColumn" type="xsd:integer" use="optional" />
        <xs:attribute name="length" type="xsd:integer" use="required" />
            <xs:attribute name="allowUnlimitedText" type="xsd:boolean"
                      use="optional" />
        <xs:attribute name="sourceOfStandard" type="xsd:string"
                      use="optional" />
        <xs:attribute name="recordTypes" type="xsd:string" use="optional" />
      </xs:complexType>
    </xs:element>

    >>> list(NAACCR1.items_180())[:3]
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [{'naaccrId': 'recordType', 'naaccrNum': 10,
      'naaccrName': 'Record Type',
      'startColumn': 1, 'length': 1,
      'parentXmlElement': 'NaaccrData'},
     {'naaccrId': 'registryType', 'naaccrNum': 30, ...},
     {'naaccrId': 'naaccrRecordVersion', 'naaccrNum': 50, ...}]
    """
    dd13_xsd = XML.parse(res.open_text(
        naaccr_xml_xsd, 'naaccr_dictionary_1.3.xsd'))

    ndd180 = XML.parse(res.open_text(
        naaccr_xml_res, 'naaccr-dictionary-180.xml'))

    data_xsd = XML.parse(res.open_text(
        naaccr_xml_xsd, 'naaccr_data_1.3.xsd'))

    item_xsd = XSD.the(
        data_xsd.getroot(),
        './/xsd:complexType[@name="itemType"]/xsd:simpleContent/xsd:extension')

    ItemDef = XSD.the(dd13_xsd.getroot(), './/xsd:element[@name="ItemDef"]')

    uri = 'http://naaccr.org/naaccrxml'
    ns = {'n': uri}

    @classmethod
    def itemDef(cls, naaccrId: str) -> XML.Element:
        """
        >>> NAACCR1.itemDef('npiRegistryId').attrib['startColumn']
        '20'
        """
        ndd = cls.ndd180.getroot()
        defPath = f'./n:ItemDefs/n:ItemDef[@naaccrId="{naaccrId}"]'
        return XSD.the(ndd, defPath, cls.ns)

    @classmethod
    def items_180(cls) -> Iterator[ItemDef_T]:
        def decode(f: Dict[str, str]) -> ItemDef_T:
            return {
                'naaccrId': f['naaccrId'],
                'naaccrNum': int(f['naaccrNum']),
                'naaccrName': f['naaccrName'],
                'startColumn': int(f['startColumn']),
                'length': int(f['length']),
                'parentXmlElement': f['parentXmlElement'],
            }

        defs = cls.ndd180.iterfind('./n:ItemDefs/n:ItemDef', cls.ns)
        return (decode(elt.attrib) for elt in defs)


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


RawRecordMaker = Callable[[XML.Element, bool],
                          Iterator[Dict[str, str]]]

RecordMaker = Callable[[XML.Element, ty.StructType, bool],
                       Iterator[Dict[str, object]]]


def xmlDF(spark: SparkSession_T, schema: ty.StructType,
          doc: XML.ElementTree, path: str, ns: Dict[str, str],
          eltRecords: Opt[RecordMaker] = None,
          simpleContent: bool = False) -> DataFrame:
    data = xmlRecords(schema, doc, path, ns, eltRecords, simpleContent)
    return spark.createDataFrame(data, schema)  # type: ignore


def xmlRecords(schema: ty.StructType,
               doc: XML.ElementTree, path: str, ns: Dict[str, str],
               eltRecords: Opt[RecordMaker] = None,
               simpleContent: bool = False) -> Iterator[Dict[str, object]]:
    """
    >>> schema = eltSchema(XSD.the(NAACCR1.ItemDef, '*'))
    >>> ea = xmlRecords(doc=NAACCR1.ndd180, schema=schema,
    ...                 path='./n:ItemDefs/n:ItemDef',
    ...                 ns=NAACCR1.ns)
    >>> list(ea)[:3]
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [{'naaccrId': 'recordType', 'naaccrNum': 10, ...},
     {'naaccrId': 'registryType', 'naaccrNum': 30, ...},
     {'naaccrId': 'naaccrRecordVersion', 'naaccrNum': 50, ...}]
    """
    getRecords = eltRecords or eltDict
    data = (record
            for elt in doc.iterfind(path, ns)
            for record in getRecords(elt, schema, simpleContent))
    return data


def eltDict(elt: XML.Element, schema: ty.StructType,
            simpleContent: bool = False) -> Iterator[Dict[str, object]]:
    # ISSUE: schema should be replace by function decode(k, v)
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


def fixna(df: pd.DataFrame) -> pd.DataFrame:
    """
    avoid string + double errors from spark.createDataFrame(pd.read_csv())
    """
    return df.where(df.notnull(), None)


class LOINC_NAACCR:
    measure = fixna(pd.read_csv(res.open_text(
        loinc_naaccr, 'loinc_naaccr.csv')))
    measure_cols = ['LOINC_NUM', 'CODE_VALUE', 'SCALE_TYP', 'AnswerListId']
    measure_struct = ty.StructType([
        ty.StructField(n, ty.StringType()) for n in measure_cols])

    answer = fixna(pd.read_csv(res.open_text(
        loinc_naaccr, 'loinc_naaccr_answer.csv')))
    answer_struct = ty.StructType([
        ty.StructField(n.lower(),
                       ty.IntegerType() if n.lower() == 'sequence_no'
                       else ty.StringType())
        for n in answer.columns
    ])


class NAACCR_R:
    # Names assumed by naaccr_txform.sql

    field_info = pd.read_csv(res.open_text(
        naaccr_r_raw, 'field_info.csv'))
    field_code_scheme = pd.read_csv(res.open_text(
        naaccr_r_raw, 'field_code_scheme.csv'))

    @classmethod
    def _code_labels(cls) -> ContextManager[Path_T]:
        return res.path(naaccr_r_raw, 'code-labels')

    @classmethod
    def field_info_in(cls, spark: SparkSession_T) -> None:
        info = spark.createDataFrame(cls.field_info)
        info.createOrReplaceTempView(NAACCR_I2B2_Mix.r_field_info)
        to_scheme = spark.createDataFrame(cls.field_code_scheme)
        to_scheme.createOrReplaceTempView(NAACCR_I2B2_Mix.r_code_scheme)

    @classmethod
    def code_labels(cls,
                    implicit: List[str] = ['iso_country']) -> pd.DataFrame:
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


class OncologyMeta:
    morph3_info = ('ICD-O-2_CSV.zip', 'icd-o-3-morph.csv', ['code', 'label', 'notes'])
    topo_info = ('ICD-O-2_CSV.zip', 'Topoenglish.txt', None)
    encoding = 'ISO-8859-1'

    @classmethod
    def read_table(cls, cache: Path_T,
                   zip: str, item: str, names: Opt[List[str]]) -> pd.DataFrame:
        archive = zipfile.ZipFile(cache / zip)

        return pd.read_csv(archive.open(item),
                           header=None if names else 0,
                           delimiter=',' if item.endswith('.csv') else '\t',
                           encoding=cls.encoding, names=names)


class NAACCR_I2B2(object):
    tumor_item_type = fixna(pd.read_csv(res.open_text(
        heron_load, 'tumor_item_type.csv')))

    seer_recode_terms = fixna(pd.read_csv(res.open_text(
        heron_load, 'seer_recode_terms.csv')))

    tx_script = SqlScript(
        'naaccr_txform.sql',
        res.read_text(heron_load, 'naaccr_txform.sql'),
        [])

    layout_view_name = 'record_layout'
    measure_view_name = 'loinc_naaccr'
    answer_view_name = 'loinc_naaccr_answers'
    per_item_view = 'tumor_item_type'

    per_section = pd.read_csv(res.open_text(
        heron_load, 'section.csv'))

    ont_script = SqlScript(
        'naaccr_concepts_load.sql',
        res.read_text(heron_load, 'naaccr_concepts_load.sql'),
        [
            ('i2b2_path_concept', []),
            ('naaccr_top_concept', ['naaccr_top', 'current_task']),
            ('section_concepts', ['section', 'naaccr_top']),
            ('item_concepts', [per_item_view]),
            ('code_concepts', [per_item_view, 'loinc_naaccr_answers', 'code_labels']),
            ('icd_o_topo', ['who_topo']),
            ('primary_site_concepts', []),
            # TODO: morphology
            ('seer_recode_concepts', ['seer_site_terms', 'naaccr_top']),
            ('naaccr_ontology', []),
        ])

    @classmethod
    def ont_view_in(cls, spark: SparkSession_T, task_id: str, update_date: dt.date,
                    c_hlevel: int = 1,
                    c_fullname: str = r'\i2b2\naaccr\x'[:-1],
                    c_name: str = 'Cancer Cases (NAACCR Hierarchy)',
                    sourcesystem_cd: str = 'heron-admin@kumc.edu',
                    who_cache: Opt[Path_T] = None) -> DataFrame:
        to_df = spark.createDataFrame
        if who_cache:
            who_topo = to_df(OncologyMeta.read_table(who_cache, *OncologyMeta.topo_info))
        else:
            log.warn('skipping WHO Topology terms')
            who_topo = spark.sql('''
              select 'C' Kode, 'incl' Lvl, 'LIP' Title where 1 = 0
            ''')

        top = pd.DataFrame([dict(c_hlevel=c_hlevel,
                                 c_fullname=c_fullname,
                                 c_name=c_name,
                                 update_date=update_date,
                                 sourcesystem_cd=sourcesystem_cd)])
        answers = to_df(LOINC_NAACCR.answer,
                        LOINC_NAACCR.answer_struct).cache()
        views = create_objects(spark, cls.ont_script,
                               current_task=to_df([dict(task_id=task_id)]),
                               naaccr_top=to_df(top),
                               section=to_df(cls.per_section),
                               tumor_item_type=to_df(cls.tumor_item_type),
                               loinc_naaccr_answers=answers,
                               code_labels=to_df(NAACCR_R.code_labels()),
                               who_topo=who_topo,
                               seer_site_terms=to_df(cls.seer_recode_terms))

        name, _, _ = cls.ont_script.objects[-1]
        return views[name]

    @classmethod
    def item_views_in(cls, spark: SparkSession_T) -> DataFrame:
        # Assign i2b2 valtype_cd to each NAACCR item.
        item_ty = spark.createDataFrame(cls.tumor_item_type)
        item_ty.createOrReplaceTempView(cls.per_item_view)

        sec = spark.createDataFrame(cls.per_section)
        sec.createOrReplaceTempView('section')

        rl = spark.createDataFrame(NAACCR_Layout.fields)
        rl.createOrReplaceTempView(cls.layout_view_name)

        return item_ty


class NAACCR_I2B2_Mix(NAACCR_I2B2):
    # ISSUE: this code is out of date; it's only needed when upstream
    # standards change.

    txform_script = res.read_text(heron_load, 'naaccr_txform.sql')
    # script inputs:
    v18_dict_view_name = 'ndd180'
    r_field_info = 'field_info'
    r_code_scheme = 'field_code_scheme'
    r_code_labels = 'code_labels'
    # outputs

    @classmethod
    def tumor_item_type_mix(cls, spark: SparkSession_T) -> DataFrame:
        ddictDF(spark).createOrReplaceTempView(cls.v18_dict_view_name)
        # @@ LOINC_NAACCR.measure_in(spark)
        # @@ LOINC_NAACCR.answers_in(spark)

        sec = spark.createDataFrame(cls.per_section)
        rl = (spark.createDataFrame(NAACCR_Layout.fields)
              .withColumnRenamed('naaccr-item-num', 'item'))
        desc = spark.createDataFrame([dict(item=item, source=source)
                                      for (item, _, source)
                                      in NAACCR_Layout.fields_source()])
        for name, data in [
                ('record_layout', rl),
                ('item_description', desc),
                ('section', sec),
        ]:
            data.createOrReplaceTempView(name)

        NAACCR_R.field_info_in(spark)

        # create_object(cls.per_item_view,
        #               cls.txform_script,
        #               spark)
        spark.catalog.cacheTable(cls.per_item_view)
        return spark.table(cls.per_item_view)


def create_objects(spark: SparkSession_T, script: SqlScript,
                   **kwargs: DataFrame) -> Dict[str, DataFrame]:
    """Run SQL create ... statements given DFs for input views.

    >>> spark = MockCTX()
    >>> create_objects(spark, NAACCR_I2B2.ont_script,
    ...     current_task=MockDF(spark, 'current_task'),
    ...     naaccr_top=MockDF(spark, 'naaccr_top'),
    ...     section=MockDF(spark, 'section'),
    ...     loinc_naaccr_answers=MockDF(spark, 'lna'),
    ...     code_labels=MockDF(spark, 'code_labels'),
    ...     who_topo=MockDF(spark, 'who_topo'),
    ...     seer_site_terms=MockDF(spark, 'site_terms'),
    ...     tumor_item_type=MockDF(spark, 'ty'))
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    {'i2b2_path_concept': MockDF(i2b2_path_concept),
     'naaccr_top_concept': MockDF(naaccr_top_concept),
     'section_concepts': MockDF(section_concepts),
     ...}
    """
    # IDEA: use a contextmanager for temp views
    for key, df in kwargs.items():
        df = kwargs[key]
        log.info('%s: %s = %s', script.name, key, df)
        df.createOrReplaceTempView(key)  # ISSUE: don't replace?
    provided = set(kwargs.keys())
    out = {}
    for name, ddl, inputs in script.objects:
        missing = set(inputs) - provided
        if missing:
            raise TypeError(missing)
        log.info('%s: create %s', script.name, name)
        spark.sql(ddl)
        df = spark.table(name)
        df.createOrReplaceTempView(name)  # ISSUE: don't replace?
        out[name] = df
    return out


def csv_view(spark: SparkSession_T, path: Path_T,
             name: Opt[str] = None) -> DataFrame:
    df = spark.read.csv(str(path),
                        header=True, escape='"', multiLine=True,
                        inferSchema=True, mode='FAILFAST')
    df.createOrReplaceTempView(name or path.stem)
    return df


def csv_meta(dtypes: Dict[str, np.dtype], path: str,
             context: str = 'http://www.w3.org/ns/csvw') -> Dict[str, object]:
    # ISSUE: dead code? obsolete in favor of fixna()?
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

# IDEA: csv_spark_schema(csv_meta(x.dtypes, 'tumor_item_type.csv')['tableSchema']['columns'])


class MockCTX(SparkSession_T):
    def __init__(self) -> None:
        self._tables = {}  # type: Dict[str, DataFrame]

    def __setitem__(self, k: str, v: DataFrame) -> None:
        self._tables[k] = v

    def table(self, k: str) -> DataFrame:
        return self._tables[k]

    def sql(self, code: str) -> DataFrame:
        _c, _o, _r, _t, _v, name, _ = code.split(None, 6)
        df = MockDF(self, name)
        self._tables[name] = df
        return df


class MockDF(DataFrame):
    def __init__(self, ctx: MockCTX, label: str) -> None:
        self.__ctx = ctx
        self.label = label

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.label})'

    def createOrReplaceTempView(self, name: str) -> None:
        self.__ctx[name] = self
