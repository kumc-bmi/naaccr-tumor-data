from importlib import resources as res
from pathlib import Path as Path_T  # for type only
from typing import (
    Callable, ContextManager, Dict, Iterator, List, Optional as Opt,
    Tuple, Union,
    cast,
)
from xml.etree import ElementTree as XML
import datetime as dt
import logging
import zipfile

from mypy_extensions import TypedDict  # ISSUE: dependency?

from sql_script import create_objects, SqlScript, DataFrame, DBSession as SparkSession_T
import tabular as tab
import heron_load
import loinc_naaccr  # included with permission
import naaccr_layout
import naaccr_r_raw
import naaccr_xml_res
import naaccr_xml_xsd

log = logging.getLogger(__name__)


class XPath:
    @classmethod
    def the(cls, elt: XML.Element, path: str,
            ns: Dict[str, str]) -> XML.Element:
        """Get _the_ match for an XPath expression on an Element.

        The XML.Element.find() method may return None, but when
        we're dealing with static data that we know matches,
        we can refine the type by asserting that there's a match.
        """
        found = elt.find(path, ns)
        assert found is not None, (elt, path)
        return found


class XSD:
    uri = 'http://www.w3.org/2001/XMLSchema'
    ns = {'xsd': uri}

    @classmethod
    def the(cls, elt: XML.Element, path: str,
            ns: Dict[str, str] = ns) -> XML.Element:
        return XPath.the(elt, path, ns)

    types: Dict[str, Callable[[str], Union[int, bool]]] = {
        'xsd:integer': int,
        'xsd:boolean': bool,
    }


"""naaccr_layout fields"""
Field0 = TypedDict('Field0', {
    'long-label': str,
    'start': int, 'end': int,
    'naaccr-item-num': int,
    'section': str,
    'grouped': bool,
})


def to_field0(elt: XML.Element) -> Field0:
    attr = elt.attrib
    return {
        'long-label': attr['long-label'],
        'start': int(attr['start']), 'end': int(attr['end']),
        'naaccr-item-num': int(attr['naaccr-item-num']),
        'section': attr['section'],
        'grouped': len(elt) > 0,
    }


Field = TypedDict('Field', {
    'long-label': str,
    'start': int, 'end': int,
    'length': int,
    'naaccr-item-num': int,
    'section': str,
    'grouped': bool,
})


def to_field(f: Field0) -> Field:
    return {
        'long-label': f['long-label'],
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

    >>> NAACCR_Layout.fields[:3]
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [{'long-label': 'Record Type', 'start': 1, 'end': 1, 'length': 1, ...},
     {'long-label': 'Registry Type', 'start': 2, ..., 'section': 'Record ID', ...},
     {'long-label': 'NAACCR Record Version', ... 'naaccr-item-num': 50, ...}]

    >>> for info in list(NAACCR_Layout.fields_source())[:3]:
    ...     print(info)
    (570, 'abstractedBy', 'CoC')
    (550, 'accessionNumberHosp', 'CoC')
    (70, 'addrAtDxCity', 'CoC')


    >>> list(NAACCR_Layout.iter_codes())[:3]
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [[70, 'addrAtDxCity', 'UNKNOWN', 'City at diagnosis unknown'],
     [102, 'addrAtDxCountry', 'ZZN', 'North America NOS'],
     [102, 'addrAtDxCountry', 'ZZC', 'Central America NOS']]

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
    [{'long-label': 'NPCR Specific Field', ..., 'naaccr-item-num': 3720, ..., 'grouped': False},
     {'long-label': 'State/Requestor Items', ..., 'naaccr-item-num': 2220, ..., 'grouped': False}]

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
    def item_codes(cls) -> tab.DataFrame:
        ea = cls.iter_codes()
        return tab.DataFrame(ea, schema=tab.Schema(columns=[
            tab.Column(number=1, name='naaccrNum', datatype='number', null=[]),
            tab.Column(number=2, name='naaccrId', datatype='string', null=[]),
            tab.Column(number=3, name='code', datatype='string', null=[]),
            tab.Column(number=4, name='desc', datatype='string', null=[''])]))

    @classmethod
    def iter_codes(cls) -> Iterator[List[Opt[tab.Value]]]:
        for path, doc in cls._field_docs():
            qname = cls._xmlId(doc)
            if not qname:
                continue
            _parent, xmlId = qname
            info = cls._field_info(doc)
            naaccrNum = int(info['Item #'])
            for code, desc in cls._item_codes(doc):
                yield [naaccrNum, xmlId, code, desc]

    @classmethod
    def _item_codes(cls, doc: XML.Element) -> List[Tuple[str, str]]:
        rows = [
            (codeElt.text,
             ''.join(descElt.itertext()))
            for tr in doc.findall('./div/table/tr[@class="code-row"]')
            for (codeElt, descElt) in [(tr.find('td[@class="code-nbr"]'),
                                        tr.find('td[@class="code-desc"]'))]
            if codeElt is not None and descElt is not None]
        return [(code, desc) for (code, desc) in rows if code]

    @classmethod
    def iter_description(cls) -> Iterator[Tuple[int, str, str]]:
        for _p, doc in cls._field_docs():
            qname = cls._xmlId(doc)
            if not qname:
                continue
            _parent, xmlId = qname
            if xmlId.startswith('reserved'):
                continue
            num = int(cls._field_info(doc)['Item #'])
            hd = None
            for div in doc.findall('./div'):
                text = ''.join(div.itertext())
                if hd == 'Description':
                    yield num, xmlId, text
                    break
                if text == 'Description':
                    hd = text

    @classmethod
    def fields_source(cls) -> Iterator[Tuple[int, str, Opt[str]]]:
        for _p, doc in cls._field_docs():
            qname = cls._xmlId(doc)
            if not qname:
                continue
            _parent, xmlId = qname
            if xmlId.startswith('reserved'):
                continue
            info = cls._field_info(doc)
            yield (int(info['Item #']), xmlId,
                   info.get('Source of Standard'))

    @classmethod
    def _fields_doc(cls) -> ContextManager[Path_T]:
        return res.path(naaccr_layout, 'doc')

    @classmethod
    def _field_docs(cls,
                    subdir: str = 'naaccr18') -> Iterator[Tuple[Path_T, XML.Element]]:
        with cls._fields_doc() as doc_dir:
            for field_path in sorted((doc_dir / subdir).glob('*.html')):
                doc = _parse_html_fragment(field_path)
                yield field_path, doc

    @classmethod
    def _xmlId(cls, doc: XML.Element) -> Opt[Tuple[str, str]]:
        naaccr_xml_elt = doc.find('./strong')
        if naaccr_xml_elt is None:
            return None
        naaccr_xml = (naaccr_xml_elt.tail or '')[2:].strip()  # </strong>: ...
        [parentElement, xmlId] = naaccr_xml.split('.')
        return parentElement, xmlId

    @classmethod
    def _field_info(cls, doc: XML.Element) -> Dict[str, str]:
        summary_table = XSD.the(
            doc, './table[@class="naaccr-summary-table naaccr-borders"]', {})
        [hd, detail] = summary_table.findall('tr')
        return dict(zip([cast(str, th.text) for th in hd.findall('th')],
                        [cast(str, td.text) for td in detail.findall('td')]))


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
              simpleContent: bool = False) -> tab.Schema:
    decls = xsd_complex_type.findall('xsd:attribute', XSD.ns)
    fields = [
        tab.Column(
            name=d.attrib['name'],
            datatype='number' if d.attrib['type'] == 'xsd:integer'
            else 'boolean' if d.attrib['type'] == 'xsd:boolean' else 'string',
            # IDEA/YAGNI?: use pd.Categorical for xsd:enumeration
            # e.g. tns:parentType
            # IDEA: metadata=d.attrib
            null=[] if d.attrib.get('use') == 'required' else [''])
        for d in decls]
    if simpleContent:
        fields = fields + [tab.Column(name='value', datatype='string')]
    return tab.Schema(columns=fields)


RawRecordMaker = Callable[[XML.Element, bool],
                          Iterator[Dict[str, str]]]

RecordMaker = Callable[[XML.Element, tab.Schema, bool],
                       Iterator[Dict[str, Opt[tab.Value]]]]


def xmlDF(spark: SparkSession_T, schema: tab.Schema,
          doc: XML.ElementTree, path: str, ns: Dict[str, str],
          eltRecords: Opt[RecordMaker] = None,
          simpleContent: bool = False) -> DataFrame:
    data = xmlRecords(schema, doc, path, ns, eltRecords, simpleContent)
    return spark.createDataFrame(data, schema)  # type: ignore


def xmlRecords(schema: tab.Schema,
               doc: XML.ElementTree, path: str, ns: Dict[str, str],
               eltRecords: Opt[RecordMaker] = None,
               simpleContent: bool = False) -> Iterator[Dict[str, Opt[tab.Value]]]:
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


def eltDict(elt: XML.Element, schema: tab.Schema,
            simpleContent: bool = False) -> Iterator[Dict[str, Opt[tab.Value]]]:
    byName = {col['name']: col for col in schema['columns']}
    out = {k: tab.Seq.decoder(byName[k])(v)
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


def _with_path(gen, f: Callable[[Path_T], List]) -> List:
    # note: Any should be 't
    with gen as p:
        return f(p)


class LOINC_NAACCR:
    measure = _with_path(res.path(loinc_naaccr, 'loinc_naaccr.csv'),
                         tab.read_csv)
    measure_cols = ['LOINC_NUM', 'CODE_VALUE', 'SCALE_TYP', 'AnswerListId']
    measure_struct = tab.Schema(columns=[
        tab.Column(name=n, datatype='string') for n in measure_cols])

    answer = _with_path(res.path(loinc_naaccr, 'loinc_naaccr_answer.csv'),
                        tab.read_csv)
    answer_struct = tab.Schema([
        tab.Column(name=n.lower(),
                   datatype='number' if n.lower() == 'sequence_no'
                   else 'string')
        for n in answer.columns
    ])


class NAACCR_R:
    # Names assumed by naaccr_txform.sql

    field_info = _with_path(res.path(naaccr_r_raw, 'field_info.csv'),
                            tab.read_csv)
    field_code_scheme = _with_path(res.path(naaccr_r_raw, 'field_code_scheme.csv'),
                                   tab.read_csv)

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
                    implicit: List[str] = ['iso_country']) -> tab.DataFrame:
        found = []
        schema = tab.Schema(columns=[
            tab.Column(number=1, name="code", datatype='string', null=[]),
            tab.Column(number=2, name="label", datatype='string', null=[]),
            tab.Column(number=3, name="means_missing", datatype='boolean', null=[]),
            tab.Column(number=4, name="description", datatype='string', null=[''])])
        with cls._code_labels() as cl_dir:
            for scheme in cls.field_code_scheme.scheme.unique():
                if scheme in implicit:
                    continue
                info = (cl_dir / scheme).with_suffix('.csv')
                skiprows = 0
                if info.open().readline().startswith('#'):
                    skiprows = 1
                codes = tab.read_csv(info, schema=schema, skiprows=skiprows)
                codes['scheme'] = info.stem
                if 'code' not in codes.columns or 'label' not in codes.columns:
                    raise ValueError((info, codes.columns))
                found.append(codes)
        all_schemes = tab.concat(found)
        with_fields = cls.field_code_scheme.merge(all_schemes)
        with_field_info = cls.field_info[['item', 'name']].merge(with_fields)
        return with_field_info


class OncologyMeta:
    morph3_info = ('ICD-O-2_CSV.zip', 'icd-o-3-morph.csv', ['code', 'label', 'notes'])
    topo_info = ('ICD-O-2_CSV.zip', 'Topoenglish.txt', None)
    encoding = 'ISO-8859-1'

    @classmethod
    def read_table(cls, cache: Path_T,
                   zip: str, item: str, names: Opt[List[str]]) -> tab.DataFrame:
        archive = zipfile.ZipFile(cache / zip)

        return tab.read_csv(archive.open(item),
                            header=None if names else 0,
                            delimiter=',' if item.endswith('.csv') else '\t',
                            encoding=cls.encoding, names=names)

    @classmethod
    def icd_o_topo(cls, topo: tab.DataFrame) -> tab.DataFrame:
        major = topo[topo.Lvl == '3']
        minor = topo[(topo.Lvl == '4')].copy()
        minor['major'] = minor.Kode.apply(lambda s: s.split('.')[0])
        out3 = tab.DataFrame(dict(
            lvl=3,
            concept_cd=major.Kode,
            c_visualattributes='FA',
            path=major.Kode + '\\',
            concept_name=major.Title
        ))
        out4 = tab.DataFrame(dict(
            lvl=4,
            concept_cd=minor.Kode.str.replace('.', ''),
            c_visualattributes='LA',
            path=minor.major + '\\' + minor.Kode + '\\',
            concept_name=minor.Title
        ))
        return tab.concat([out3, out4], sort=False)


class NAACCR_I2B2(object):
    top_folder = r'\i2b2\naaccr\x'[:-1]
    c_name = 'Cancer Cases (NAACCR Hierarchy)'
    sourcesystem_cd = 'heron-admin@kumc.edu'

    tumor_item_type = _with_path(res.path(heron_load, 'tumor_item_type.csv'),
                                 tab.read_csv)

    seer_recode_terms = _with_path(res.path(heron_load, 'seer_recode_terms.csv'),
                                   tab.read_csv)

    cs_terms = _with_path(res.path(heron_load, 'cs-terms.csv'),
                          tab.read_csv).drop(['update_date', 'sourcesystem_cd'])

    tx_script = SqlScript(
        'naaccr_txform.sql',
        res.read_text(heron_load, 'naaccr_txform.sql'),
        [])

    per_item_view = 'tumor_item_type'

    per_section = _with_path(res.path(heron_load, 'section.csv'),
                             tab.read_csv)

    ont_script = SqlScript(
        'naaccr_concepts_load.sql',
        res.read_text(heron_load, 'naaccr_concepts_load.sql'),
        [
            ('i2b2_path_concept', []),
            ('naaccr_top_concept', ['naaccr_top', 'current_task']),
            ('section_concepts', ['section', 'naaccr_top']),
            ('item_concepts', [per_item_view]),
            ('code_concepts', [per_item_view, 'loinc_naaccr_answers', 'code_labels']),
            ('primary_site_concepts', ['icd_o_topo']),
            # TODO: morphology
            ('seer_recode_concepts', ['seer_site_terms', 'naaccr_top']),
            ('site_schema_concepts', ['cs_terms']),
            ('naaccr_ontology', []),
        ])

    @classmethod
    def ont_view_in(cls, spark: SparkSession_T, task_id: str, update_date: dt.date,
                    who_cache: Opt[Path_T] = None) -> DataFrame:
        to_df = spark.createDataFrame
        if who_cache:
            who_topo = OncologyMeta.read_table(who_cache, *OncologyMeta.topo_info)
            icd_o_topo = to_df(OncologyMeta.icd_o_topo(who_topo))
        else:
            log.warn('skipping WHO Topology terms')
            icd_o_topo = spark.sql('''
              select 3 lvl, 'C00' concept_cd, 'FA' c_visualattributes
                   , 'abc' path, 'LIP' concept_path, 'x' concept_name
            ''')

        top = tab.DataFrame([dict(c_hlevel=1,
                                  c_fullname=cls.top_folder,
                                  c_name=cls.c_name,
                                  update_date=update_date,
                                  sourcesystem_cd=cls.sourcesystem_cd)])
        answers = to_df(LOINC_NAACCR.answer,
                        LOINC_NAACCR.answer_struct).cache()
        views = create_objects(spark, cls.ont_script,
                               current_task=to_df([dict(task_id=task_id)]),
                               naaccr_top=to_df(top),
                               section=to_df(cls.per_section).cache(),
                               tumor_item_type=to_df(cls.tumor_item_type),
                               loinc_naaccr_answers=answers,
                               code_labels=to_df(NAACCR_R.code_labels()),
                               icd_o_topo=icd_o_topo,
                               cs_terms=to_df(cls.cs_terms),
                               seer_site_terms=to_df(cls.seer_recode_terms))

        name, _, _ = cls.ont_script.objects[-1]
        return views[name]


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
