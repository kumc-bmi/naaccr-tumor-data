# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:hydrogen
#     text_representation:
#       extension: .py
#       format_name: hydrogen
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
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

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## About This Document
#
# This is both a jupyter notebook and a python module, sync'd using [jupytext][].
#
# Collaborators are expected to be aware of coding style etc. as discussed in [CONTRIBUTING](CONTRIBUTING.md).
#
# [jupytext]: https://github.com/mwouts/jupytext

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Platform for v18: python3, JVM (WIP)
#
# **ISSUE:** Spark SQL is overkill for O(100,000) records x O(1000) columns.
# Even pandas is probably not necessary other than notebook formatting.
# Let's see if we can do all the pivoting and ontology building with sqlite3.
# Then we can make a little ant job with a groovy or JS script to shove
# the results into the DB with JDBC.
#
#  - [python 3.7 standard library](https://docs.python.org/3/library/index.html)
#  - [pyspark.sql API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
#  - [pandas](https://pandas.pydata.org/pandas-docs/stable/index.html)
#
# _Using luigi is beyond the scope of this document; see [tumor_reg_tasks](tumor_reg_tasks.py)._

# %% {"slideshow": {"slide_type": "skip"}}
# python 3.7 stdlib
from functools import reduce
from gzip import GzipFile
from importlib import resources as res
from pathlib import Path as Path_T
from sqlite3 import connect as connect_mem, PARSE_COLNAMES
from subprocess import Popen as Popen_T, PIPE
from sys import stderr
from typing import Dict, List, Optional as Opt, Tuple
from typing import ContextManager, Callable, Iterable, cast
from xml.etree import ElementTree as XML
import datetime as dt
import itertools
import json
import logging
import re

# %% [markdown]
# The tabular module provides a pandas-like DataFrame API using
# the [W3C Tabular Data model](https://www.w3.org/TR/tabular-data-primer/).

# %%
import tabular as tab

# %% [markdown]
# We also have utilities for running SQL from files. The resulting DataFrames "remember" which SQL DB they came from.

# %%
from sql_script import SqlScript, DBSession as SparkSession_T, DataFrame, insert_stmt


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# We incorporate materials from the
# [imsweb/naaccr-xml](https://github.com/imsweb/naaccr-xml) project,
# such as test data.

# %% {"slideshow": {"slide_type": "-"}}
import naaccr_xml_samples
import bc_qa

# %% {"slideshow": {"slide_type": "fragment"}}
import tumor_reg_ont as ont
import heron_load


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Integration Testing: local files, ... (WIP)

# %% [markdown]
# For integration testing (`IO_TESTING`)
# we set aside [ocap discipline](CONTRIBUTING.md#OCap-discipline).
# Notebook-level globals such as `_cwd` use a leading underscore.

# %% {"slideshow": {"slide_type": "skip"}}
# In a notebook context, we have `__name__ == '__main__'`.
IO_TESTING = __name__ == '__main__'

log = logging.getLogger(__name__)

if IO_TESTING:
    import pandas as pd  # type: ignore

    logging.basicConfig(level=logging.INFO, stream=stderr)
    log.info('tumor_reg_data using: %s', dict(
        pandas=pd.__version__,
    ))

    def _get_cwd() -> Path_T:
        from pathlib import Path
        return Path('.')

    _cwd = _get_cwd()
    log.info('cwd: %s', _cwd.resolve())

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ### In-memory SQL
#
# The `_SQL` utility uses an in-memory sqlite3 database.

# %%
if IO_TESTING:
    _conn = connect_mem(':memory:', detect_types=PARSE_COLNAMES)
    _spark = SparkSession_T(_conn)


# %% {"slideshow": {"slide_type": "skip"}}
def _to_spark(name: str, compute: Callable[[], tab.DataFrame],
              cache: bool = False) -> Opt[DataFrame]:
    """Compute data locally and save as SQL view"""
    if not IO_TESTING:
        return None
    df = _spark.load_data_frame(name, compute())
    return df


def _to_view(name: str, compute: Callable[[], DataFrame],
             cache: bool = False) -> Opt[DataFrame]:
    """Save Spark DF as spark view"""
    if not IO_TESTING:
        return None
    df = compute()
    df._ctx.load_data_frame(name, df)
    return df


def _to_pd(df: tab.Relation,
           index: Opt[str] = None):
    if not IO_TESTING:
        return None
    pdf = pd.DataFrame.from_records((row for (_, row) in df.iterrows()),
                                    columns=df.columns)
    if index is not None:
        pdf = pdf.set_index(index)
    return pdf


def _SQL(sql: str,
         index: Opt[str] = None,
         limit: Opt[int] = 5):
    """Run SQL query and display results using Pandas dataframe"""
    if not IO_TESTING:
        return None
    if limit and limit is not None:
        sql = f'select * from ({sql}) limit {limit}'
    df = _spark.sql(sql)
    return _to_pd(df, index)


IO_TESTING and _to_spark('section', lambda: ont._with_path(res.path(heron_load, 'section.csv'), tab.read_csv))
_SQL('select * from section', index='sectionid', limit=4)

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## NAACCR Data Dictionary Items
#
# Each NAACCR tumor record consists of hundreds of "items" (aka fields).
# The **naaccr-xml** project has data dictionaries to support
# reformatting flat files to XML and back. The **imsweb/layout** project provides
# grouping of items into sections:

# %%
_to_spark('ndd180', lambda: tab.DataFrame.from_records(ont.NAACCR1.items_180()))
_SQL('''select * from ndd180 where naaccrNum between 400 and 500''',
     index='naaccrNum')

# %%
_to_spark('record_layout',
          lambda: tab.DataFrame.from_records(ont.NAACCR_Layout.fields))
_SQL("select * from record_layout where `naaccr-item-num` between 400 and 500",
     index='naaccr-item-num')

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## i2b2 datatypes: `valtype_cd`
#
# But the naaccr-xml data dictionaries don't have enough information
# to determine i2b2's notion of datatype called `valtype_cd`:

# %% {"slideshow": {"slide_type": "-"}}
IO_TESTING and _to_pd(tab.DataFrame.from_records(
    dict(valtype_cd=cd, description=desc)
    for (cd, desc) in
    [('@', 'nominal'), ('N', 'numeric'), ('D', 'date'), ('T', 'text')]))

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Curated `valtype_cd`
#
# So we curated the datatypes, incorporating datatype
# information from LOINC (`loinc_num`, `scale_typ`, `AnswerListId`)
# and from Werth / PA DoH (`scheme`):

# %% {"slideshow": {"slide_type": "-"}}
_to_spark('tumor_item_type', lambda: ont.NAACCR_I2B2.tumor_item_type)
_SQL('''select * from tumor_item_type''', index='naaccrNum')

# %% [markdown] {"slideshow": {"slide_type": "-"}}
# ## Confidential Items Skipped
#
# Not every item in **naaccr-xml** has a curated `valtype_cd`:

# %% {"slideshow": {"slide_type": "-"}}
_SQL('''select count(*) from ndd180 nd
left join tumor_item_type ty on nd.naaccrNum = ty.naaccrNum and nd.naaccrId = ty.naaccrId
where ty.naaccrNum is null or ty.valtype_cd is null''')

# %% [markdown] {"slideshow": {"slide_type": "-"}}
# The un-curated items are all from confidential sections:

# %%
_SQL('''
select distinct rl.section from ndd180 nd
left join record_layout rl on rl.`naaccr-item-num` = nd.naaccrNum
left join tumor_item_type ty
on nd.naaccrNum = ty.naaccrNum and nd.naaccrId = ty.naaccrId
where ty.naaccrNum is null or ty.valtype_cd is null
''')

# %% [markdown] {"slideshow": {"slide_type": "subslide"}}
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

# %% [markdown] {"slideshow": {"slide_type": "subslide"}}
# ### Ambiguous valtype_cd?

# %% {"slideshow": {"slide_type": "-"}}
_SQL('''
select naaccrId, length, count(distinct valtype_cd), group_concat(valtype_cd, ',')
from tumor_item_type
group by naaccrId, length
having count(distinct valtype_cd) > 1
''', limit=None)


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## PCORNet CDM Field Types
#
# We can generate PCORNet CDM style type information from `valtype_cd` and such.

# %%
def upper_snake_case(camel: str) -> str:
    """
    >>> upper_snake_case('dateOfBirth')
    'DATE_OF_BIRTH'
    """
    return re.sub('([A-Z])', r'_\1', camel).upper()


if IO_TESTING:
    _conn.create_function('upper_snake_case', 1, upper_snake_case)


_to_spark('item_codes', lambda: ont.NAACCR_Layout.item_codes())
_SQL('''
select upper_snake_case(naaccrId) as FIELD_NAME
from tumor_item_type
order by naaccrNum
limit 3
''')

# %%
IO_TESTING and _to_spark('ch10', lambda: tab.DataFrame(
    ont.NAACCR_Layout.iter_description(),
    schema={'columns': [
        {'number': 1, 'name': 'naaccrNum', 'datatype': 'number', 'null': []},
        {'number': 3, 'name': 'description', 'datatype': 'string', 'null': ['']}]}
))
_SQL('select * from ch10')

# %%
_to_spark('item_codes', lambda: ont.NAACCR_Layout.item_codes())
_SQL('select * from item_codes')


# %%
class TumorTableSpec:
    script = SqlScript('naaccr_concepts_load.sql',
                       res.read_text(heron_load, 'naaccr_concepts_load.sql'),
                       [('pcornet_tumor_fields', ['tumor_item_type', 'ch10', 'item_codes'])])

    @classmethod
    def fields(cls, spark):
        ch10 = tab.DataFrame(
            ont.NAACCR_Layout.iter_description(),
            schema={'columns': [
                {'number': 1, 'name': 'naaccrNum', 'datatype': 'number', 'null': []},
                {'number': 3, 'name': 'description', 'datatype': 'string', 'null': ['']}]}
        )
        views = ont.create_objects(spark, cls.script,
                                   tumor_item_type=ont.NAACCR_I2B2.tumor_item_type,
                                   ch10=ch10,
                                   item_codes=ont.NAACCR_Layout.item_codes())
        return list(views.values())[-1]


if IO_TESTING:
    _fields = TumorTableSpec.fields(_spark)
IO_TESTING and _SQL('select * from pcornet_tumor_fields')

# %%
# TumorTable.valuesets()
if IO_TESTING:
    _to_pd(_fields).set_index('item').to_csv('pcornet_cdm/fields.csv')
else:
    _fields = tab.DataFrame.from_records([dict(x=1)])
IO_TESTING and _to_pd(_fields, index='item').loc[380:].head()

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## NAACCR Ontology

# %% {"slideshow": {"slide_type": "skip"}}
IO_TESTING and ont.NAACCR_I2B2.ont_view_in(
    _spark, who_cache=(_cwd / ',cache').resolve(), task_id='task1', update_date=dt.date(2019, 9, 13))

# %% {"slideshow": {"slide_type": "-"}}
_SQL('''
select * from naaccr_top_concept
''')

# %% {"slideshow": {"slide_type": "subslide"}}
_SQL('''
select * from section_concepts order by sectionId
''')

# %% {"slideshow": {"slide_type": "subslide"}}
_SQL('''
select * from item_concepts where naaccrNum between 400 and 450 order by naaccrNum
''')

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Coded Concepts

# %%
# TODO: use these in preference to LOINC, Werth codes.
IO_TESTING and _to_pd(ont.NAACCR_Layout.item_codes()).query('naaccrNum == 380')

# %% {"slideshow": {"slide_type": "subslide"}}
if IO_TESTING:
    _to_spark('loinc_naaccr_answer', lambda: ont.LOINC_NAACCR.answer)

IO_TESTING and _SQL('select * from loinc_naaccr_answer where code_value = 380', limit=5)

# %% [markdown] {"slideshow": {"slide_type": "subslide"}}
# #### Werth Code Values

# %%
if IO_TESTING:
    _spark.load_data_frame('field_code_scheme', ont.NAACCR_R.field_code_scheme)
    _spark.load_data_frame('code_labels', ont.NAACCR_R.code_labels())
IO_TESTING and _SQL('select * from code_labels', limit=5).set_index(['item', 'name', 'scheme', 'code'])

# %% {"slideshow": {"slide_type": "subslide"}}
_SQL('''
select answer_code, c_hlevel, sectionId, c_basecode, c_name, c_visualattributes, c_fullname
from code_concepts where naaccrNum = 610
''', index='answer_code', limit=10)

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Oncology MetaFiles from the World Health Organization
#
# [materials](http://www.who.int/classifications/icd/adaptations/oncology/en/index.html)

# %% {"slideshow": {"slide_type": "subslide"}}
if IO_TESTING:
    _who_topo = ont.OncologyMeta.read_table((_cwd / ',cache').resolve(), *ont.OncologyMeta.topo_info)

IO_TESTING and _to_pd(_who_topo).set_index('Kode').head()

# %% {"slideshow": {"slide_type": "subslide"}}
_to_spark('icd_o_topo', lambda: ont.OncologyMeta.icd_o_topo(_who_topo))
_SQL('select * from icd_o_topo order by path')

# %% {"slideshow": {"slide_type": "slide"}}
_SQL(r'''select * from primary_site_concepts''')

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Site-Specific Factor Terms

# %% {"slideshow": {"slide_type": "-"}}
IO_TESTING and _to_pd(ont.NAACCR_I2B2.cs_terms).head()

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## SEER Recode Concepts

# %% {"slideshow": {"slide_type": "-"}}
_SQL(r'''
select * from seer_recode_concepts
''')


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## NAACCR XML Data

# %% {"slideshow": {"slide_type": "skip"}}
class NAACCR2:
    ns = {'n': 'http://naaccr.org/naaccrxml'}

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
def simple_schema(records: Iterable[Dict[str, str]], order: Dict[str, object],
                  max_length: int = 15) -> tab.Schema:
    field_set = set(k for r in records for k in r.keys())
    field_list = sorted(field_set, key=lambda k: order[k])
    blanks = [' ' * w for w in range(max_length)]
    nums = itertools.count(start=1)
    return tab.Schema(columns=[tab.Column(number=next(nums), name=name, datatype='string', null=blanks)
                               for name in field_list])


def tumorDF(doc: XML.ElementTree,
            items=ont.NAACCR_I2B2.tumor_item_type) -> tab.DataFrame:
    rownum = 0
    ns = NAACCR2.ns

    to_parent = {c: p for p in doc.iter() for c in p}

    order = {id: (sec, num)
             for (_, [id, sec, num]) in
             items.select('naaccrId', 'sectionId', 'naaccrNum').iterrows()}
    order = dict(order, rownum=(0, 0))
    blank = {id: None
             for id in items.naaccrId.values}

    def tumorRecord(tumorElt: XML.Element) -> Dict[str, str]:
        nonlocal rownum
        rownum += 1
        patElt = to_parent[tumorElt]
        ndataElt = to_parent[patElt]
        items = {item.attrib['naaccrId']: item.text
                 for elt in [ndataElt, patElt, tumorElt]
                 for item in elt.iterfind('./n:Item', ns)}
        record = blank.copy()
        record.update(items)
        record = dict(record, rownum=rownum)
        return record

    tumor_elts = doc.iterfind('./*/n:Tumor', ns)
    records = [tumorRecord(e) for e in tumor_elts]
    schema = simple_schema(records, order)
    names = [field['name'] for field in schema['columns']]
    data = [[record[key] for key in names]
            for record in records]
    return tab.DataFrame(data, schema)


def without_empty_cols(df: tab.DataFrame) -> tab.DataFrame:
    non_empty = [seq.column['name']
                 for name in df.columns
                 for seq in [getattr(df, name)]
                 if not set(seq.values) <= set([None])]
    return df.select(*non_empty)


##
IO_TESTING and _to_pd(
    without_empty_cols(tumorDF(NAACCR2.s100x)), index='rownum'
).sort_values(['naaccrId', 'rownum']).head(5)


# %% {"slideshow": {"slide_type": "skip"}}
# What columns are covered by the 100 tumor sample?

if IO_TESTING:
    print(without_empty_cols(tumorDF(NAACCR2.s100x)).columns)


# %% [markdown]
# ## Synthetic Tumor Data
#
# Since the naaccr-xml test data doesn't supply important items
# such as class of case, let's synthesize some data.

# %%
class SyntheticTumors:
    script = SqlScript('data_char_sim.sql',
                       res.read_text(heron_load, 'data_char_sim.sql'),
                       [('nominal_cdf', []),
                        ('simulated_case_year', ['data_agg_naaccr', 'simulated_entity']),
                        ('simulated_naaccr_nom', ['data_agg_naaccr', 'simulated_entity'])])

    @classmethod
    def make_eav(cls, spark: SparkSession_T, agg: tab.DataFrame,
                 qty: int = 500,
                 years: Tuple[int, int] = (2004, 2018)) -> DataFrame:
        simulated_entity = tab.DataFrame.from_records([dict(case_index=ix) for ix in range(1, qty)])

        views = ont.create_objects(spark, cls.script,
                                   data_agg_naaccr=agg,
                                   record_layout=tab.DataFrame.from_records(ont.NAACCR_Layout.fields),
                                   section=ont.NAACCR_I2B2.per_section,
                                   tumor_item_type=ont.NAACCR_I2B2.tumor_item_type,
                                   simulated_entity=simulated_entity)
        return list(views.values())[-1]


if IO_TESTING:
    SyntheticTumors.make_eav(
        _spark,
        tab.read_csv(_cwd / 'test_data/naaccr_nom_stats.csv'),
        qty=2000)

# %%
_SQL('select * from simulated_naaccr_nom limit 50', index=['case_index', 'sectionId', 'naaccrNum'], limit=40)

# %%
_SQL("select * from data_agg_naaccr where dx_yr = 2017 and naaccrId like 'date%'")


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## NAACCR Flat File v18

# %%
class TextFile(tab.Relation):
    def __init__(self, schema, access: Path_T) -> None:
        tab.Relation.__init__(self, schema)
        self.__access = access

    @classmethod
    def simple(cls, access: Path_T,
               value_col='value'):
        col = tab.Seq.column_of('', name=value_col)
        return cls(tab.Schema(columns=[col]), access)

    def iterrows(self):
        return zip(itertools.count(),
                   ([line] for line in self.__access.open().readlines()))


if IO_TESTING:
    with NAACCR2.s100t() as _tr_file:
        log.info('tr_file: %s', _tr_file)
        _spark.load_data_frame('s100t', TextFile.simple(_tr_file))
else:
    _naaccr_text_lines = cast(DataFrame, None)

_SQL('select * from s100t')


# %% {"slideshow": {"slide_type": "skip"}}
class TumorTable:
    lines_table = 'naaccr_lines'
    raw_view = 'naaccr_fields_raw'
    typed_view = 'naaccr_fields'
    value_col = 'value'
    exclude_pfx = 'reserved'
    pat_id_cols = ['patientSystemIdHosp', 'patientIdNumber']
    key_cols = ['dateOfDiagnosis', 'dateOfBirth', 'dateOfLastContact',
                'dateCaseLastChanged', 'dateCaseCompleted', 'dateCaseReportExported']

    @classmethod
    def fields_raw(cls, table: str, itemDefs: tab.DataFrame,
                   value_col: str = 'value',
                   exclude_pfx: str = 'reserved') -> str:
        """
        @param itemDefs: see ddictDF
        """
        fields = itemDefs.apply(
            'string', lambda startColumn, length, naaccrId, **_:
            f'substr({value_col}, {startColumn}, {length}) as {naaccrId}').values
        fieldsep = "\n     , "
        return f"""
        select {fieldsep.join(fields)}
        from {table}"""

    @classmethod
    def fields_typed(cls, ty: tab.DataFrame):
        def _to_date(col):
            return f"""case when trim({col}) > '' then date(substr({col}, 1, 4) || '-' || coalesce(substr({col}, 5, 2), '01') || '-'
                     || coalesce(substr({col}, 7, 2), '01')) end as {col} """

        def _to_text(col: str) -> str:
            return f"case when trim({col}) > '' then trim({col}) end as {col}"  # ISSUE: left / right

        def _to_num(col: str) -> str:
            return f"case when trim({col}) > '' then 0 + {col} end as {col}"

        decode = {'T': _to_text, '@': _to_text, 'D': _to_date, 'N': _to_num}
        ty = ty.filter(lambda valtype_cd, **_: valtype_cd in decode.keys())
        cols = ty.apply('string', lambda naaccrId, valtype_cd, **_: decode[valtype_cd](naaccrId))
        sep = '\n  , '
        return f"""select row_number() over (order by {', '.join(cls.key_cols)}) tumor_id
             , {', '.join(cls.pat_id_cols)}
             , {sep.join(cols.values)}
        from {cls.raw_view}"""


# %% {"slideshow": {"slide_type": "skip"}}
def naaccr_read_fwf(text_lines: DataFrame, itemDefs: tab.DataFrame) -> DataFrame:
    """
    @param text_lines: see also class TextFile
    @param itemDefs: see ddictDF.
    """
    log.info('flat_file lines: %s', text_lines)
    viewdef = TumorTable.fields_raw(text_lines.table, itemDefs,
                                    TumorTable.value_col, TumorTable.exclude_pfx)
    spark = text_lines._ctx
    spark.sql(f'drop view if exists {TumorTable.raw_view}')
    spark.sql(f'create view {TumorTable.raw_view} as {viewdef}')
    return spark.table(TumorTable.raw_view)


_extract = cast(DataFrame, None)  # for static analysis when not IO_TESTING
if IO_TESTING:
    _naaccr_text_lines = _spark.load_data_frame('naaccr_lines', TextFile.simple(_tr_file))
    _extract = naaccr_read_fwf(_naaccr_text_lines, ont.ddictDF())

_SQL('select * from naaccr_fields_raw')


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## NAACCR Dates
#
#  - **ISSUE**: hide date flags in i2b2? They just say why a date is missing, which doesn't seem worth the screenspace.

# %% [markdown] {"slideshow": {"slide_type": "subslide"}}
# ### Strange dates: TODO?

if IO_TESTING:
    _conn.executescript(TumorTable.fields_typed(ont.NAACCR_I2B2.tumor_item_type))

_SQL('select * from naaccr_fields limit 10')


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
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
def dups(df_spark: DataFrame, key_cols: List[str]) -> DataFrame:
    key_list = ', '.join(key_cols)
    dup_q = f'''
        select count(*) qty, {key_list} from {df_spark.table}
        group by {key_list}
        having count(*) > 1
    '''
    return df_spark._ctx.sql(dup_q)


_key1 = ['patientSystemIdHosp', 'tumorRecordNumber']

IO_TESTING and _to_pd(
    dups(_extract.select('sequenceNumberCentral',
                         'dateOfDiagnosis', 'dateCaseCompleted',
                         *_key1),
         _key1)
).set_index(_key1).head(10)

# %%
logging.getLogger('sql_script').setLevel(logging.DEBUG)


# %% {"slideshow": {"slide_type": "skip"}}
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
    def with_tumor_id(cls, data: DataFrame,
                      name: str = 'recordId',
                      extra: List[str] = ['dateOfDiagnosis',
                                          'dateCaseCompleted'],
                      # keep recordId length consistent
                      extra_default: Opt[str] = None) -> DataFrame:
        # ISSUE: performance: add encounter_num column here?
        if extra_default is None:
            extra_default = '0000-00-00'
        typed = lambda n: f'"{n} [date]"' if n.startswith('date') else n
        extra_col = ' || '.join(f"coalesce({typed(col)}, '{extra_default}')" for col in extra)
        id_col = 'patientSystemIdHosp || tumorRecordNumber || ' + extra_col
        log.info('@@@id_col: %s', id_col)
        return data.withColumn(name, id_col)

    @classmethod
    def with_rownum(cls, tumors: DataFrame,
                    start: int = 1,
                    new_col: str = 'encounter_num',
                    key_col: str = 'recordId') -> DataFrame:
        # ISSUE: deid encounter_num further?
        tumors = tumors.withColumn(
            new_col,
            f'1 + row_number() over (order by {key_col})')
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

# %% {"slideshow": {"slide_type": "slide"}}
IO_TESTING and _to_pd(_pat_tmr).head(15)

# %% {"slideshow": {"slide_type": "slide"}}
IO_TESTING and _patients.limit(10).toPandas()


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ##  Observations

# %%
def tumor_item_value(ty: DataFrame,
                     tumor_id: str = 'tumor_id'):
    keys = "patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis"  # ISSUE: refactor
    ty = ty[ty.valtype_cd.isin(['@', 'N', 'D', 'T'])]
    ty_q = ty.withColumn(
        'q', ty.apply('string', lambda naaccrId, naaccrNum, valtype_cd, **_:
                      f'''
                      select {tumor_id}, {keys}
                           , '{naaccrId}' as naaccrId, {naaccrNum} as naaccrNum
                           , {1 if valtype_cd in ('Ni', 'Ti') else 0} as identified_only
                           , {naaccrId if valtype_cd == '@' else 'null'} as code_value
                           , {naaccrId if valtype_cd in ('N', 'Ni') else 'null'} as numeric_value
                           , {naaccrId if valtype_cd == 'D' else 'null'} as date_value
                           , {naaccrId if valtype_cd in ('T', 'Ti') else 'null'} as text_value
                      from {TumorTable.typed_view}
                      ''').apply(lambda s: ' '.join(s.split())))
    bySection = {(sid, s): ty_q[ty_q.sectionId.isin([sid])].q.values
                 for (_, [sid, s]) in ty_q.select('sectionId', 'section').iterrows()}
    nl = '\n'
    sections = {f'section_{sid}':
                f'''-- {sid}: {s}{nl}
                {(nl + 'union all' + nl).join(queries)}'''
                for ((sid, s), queries) in bySection.items()}
    section_all = '\nunion all\n'.join(f'select * from section_{i}' for (i, _) in bySection.keys())
    return dict(sections, section_all=section_all)


if IO_TESTING:
    for sql in tumor_item_value(ont.NAACCR_I2B2.tumor_item_type):
        print()
        print(sql)
        _conn.executescript(sql)

# %%
_SQL('select * from section_all where date_value is not null order by tumor_id, naaccrNum', limit=30)


# %% {"slideshow": {"slide_type": "skip"}}
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

        raw_obs = TumorKeys.with_tumor_id(naaccr_dates(  # noqa @@@
            stack_obs(extract, item_ty), # noqa @@@
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
            naaccr_dates(extract, TumorKeys.dtcols))  # noqa @@@
        extract_id.createOrReplaceTempView(cls.extract_id_view)
        return spark.table(cls.extract_id_view)


# %% {"slideshow": {"slide_type": "-"}}
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
                       [('data_agg_naaccr', ['naaccr_extract', 'tumors_eav', 'tumor_item_type'])])

    @classmethod
    def stats(cls, tumors: DataFrame, spark: SparkSession_T) -> DataFrame:
        to_df = spark.createDataFrame

        ty = to_df(ont.NAACCR_I2B2.tumor_item_type)
        views = ont.create_objects(spark, cls.script,
                                   section=to_df(ont.NAACCR_I2B2.per_section),
                                   record_layout=to_df(ont.NAACCR_Layout.fields),
                                   tumor_item_type=ty,
                                   naaccr_extract=tumors,
                                   tumors_eav=cls.stack_obs(tumors, ty, ['dateOfDiagnosis']))
        return list(views.values())[-1]

    @classmethod
    def stack_obs(cls, data: DataFrame, ty: DataFrame,
                  id_vars: List[str] = [],
                  valtype_cds: List[str] = ['@', 'D', 'N'],
                  var_name: str = 'naaccrId',
                  id_col: str = 'recordId') -> DataFrame:
        value_vars = [row.naaccrId
                      for row in ty.where(ty.valtype_cd.isin(valtype_cds))
                      .collect()
                      if row.naaccrId not in id_vars]
        df = melt(data.withColumn(id_col, func.monotonically_increasing_id()),  # noqa @@@
                  value_vars=value_vars, id_vars=[id_col] + id_vars, var_name=var_name)
        return df.where(func.trim(df.value) > '')  # noqa @@@


# if IO_TESTING:
#     DataSummary.stack_nominals(_extract, _ty).createOrReplaceTempView('tumors_eav')
# _SQL('select * from tumors_eav')

# %%
if IO_TESTING:
    DataSummary.stats(_extract, _spark)
    _spark.table('data_char_naaccr').cache()
    _spark.table('item_concepts').cache()

_SQL('select * from data_char_naaccr order by sectionId, naaccrNum, value', limit=15)

# %%
if IO_TESTING:
    _SQL('select * from data_char_naaccr order by sectionId, naaccrNum, value', limit=None).to_csv(
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
    def valtypes(cls) -> tab.DataFrame:
        factor_nums = [d['naaccrNum'] for d in cls.items]
        item_ty = ont.NAACCR_I2B2.tumor_item_type.select('naaccrNum', 'naaccrId', 'valtype_cd')
        item_ty = item_ty[item_ty.naaccrNum.isin(factor_nums)]
        return item_ty

    @classmethod
    def make(cls, spark: SparkSession_T, extract: DataFrame) -> DataFrame:
        with_schema = cls.make_tumor_schema(spark, extract)
        ty_df = spark.createDataFrame(cls.valtypes())

        raw_obs = stack_obs(with_schema, ty_df,  # noqa @@@@
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


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
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

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Oracle DB Access

# %% [markdown]
# We use `PYSPARK_SUBMIT_ARGS` to get JDBC jar in both
# `spark.driver.extraClassPath` and `--jars`:

# %%
if IO_TESTING:
    from os import environ as _environ


# %%
if IO_TESTING:
    def _set_pw(name: str = 'ID_PASSWORD') -> None:
        from os import environ
        from getpass import getpass
        password = getpass(name)
        environ[name] = password

    # _set_pw()


# %%
class Account:
    def __init__(self, name: str, user: str, password: str,
                 popen: Callable[..., Popen_T],
                 fatjar: str = './build/libs/naaccr-tumor-data.jar',
                 jdbcjar: str = '/home/dconnolly/Downloads/ojdbc8.jar',
                 url: str = 'jdbc:oracle:thin:@localhost:8621:nheronB2',
                 driver: str = "oracle.jdbc.OracleDriver") -> None:
        self.name = name
        self.url = url
        db = url.split(':')[-1]
        self.label = f'{self.__class__.__name__}({user}@{db})'
        self.__popen = popen
        self.classpath = ':'.join([jdbcjar, fatjar])
        pfx = f'{name}_'
        self.__env = {
            pfx + 'URL': url,
            pfx + 'DRIVER': driver,
            pfx + 'USER': user,
            pfx + 'PASSWORD': password}

    def __repr__(self) -> str:
        return self.label

    def rd(self, table: str) -> tab.DataFrame:
        args = ['java', '-cp', self.classpath, 'Loader',
                '--account', self.name, '--query', f'select * from {table}']
        log.info("Account subprocess: %s", args)
        # ISSUE: stream results?
        with self.__popen(args, env=self.__env, stdout=PIPE, encoding='utf-8') as proc:
            records = json.load(proc.stdout)
            status = proc.wait()
            if status != 0:
                raise IOError(proc.stderr.read())
            return tab.DataFrame.from_records(records)

    def wr(self, table: str, data: tab.Relation,
           mode: Opt[str] = None) -> None:
        args = ['java', '-cp', self.classpath, 'Loader',
                '--account', self.name, '--load']
        log.info("Account subprocess: %s", args)
        stmt = insert_stmt(table, data.columns)
        log.info('Account insert SQL: %s', stmt)
        header = {'sql': stmt}
        with self.__popen(args, env=self.__env, stdin=PIPE, stderr=PIPE, encoding='utf-8') as proc:
            dest = proc.stdin
            json.dump(header, dest)
            dest.write('\n')
            for _, record in data.iterrows():
                json.dump(record, dest)
                dest.write('\n')
            proc.stdin.close()
            status = proc.wait()
            if status != 0:
                raise IOError(proc.stderr.read())


DB_TESTING = IO_TESTING and 'ID_PASSWORD' in _environ
if DB_TESTING:
    from subprocess import Popen as _Popen
    _cdw = Account('ID', _environ['LOGNAME'], _environ['ID_PASSWORD'], _Popen)

DB_TESTING and _to_pd(_cdw.rd("global_name"))


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
    parts = [f'{n} as "{n.upper()}"' for n in df.columns]
    q = f"select {', '.join(parts)} from {df.table}"
    return df._ctx.sql(q)


DB_TESTING and case_fold(_spark.table('section'))

# %%
_SQL('select tumor_id, dateOfDiagnosis, primarySite from naaccr_fields')

# %%
DB_TESTING and _cdw.wr(
    'NAACCR_TUMORS', case_fold(_spark.sql('select tumor_id, dateOfDiagnosis, primarySite from naaccr_fields')))

# %%
if DB_TESTING:
    _patients_mapped = TumorKeys.with_patient_num(_patients, _spark, _cdw, 'NIGHTHERONDATA', 'SMS@kumed.com')

DB_TESTING and _patients_mapped.limit(5).toPandas()

# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Use Case: GPC Breast Cancer Survey
#
# The NAACCR format has 500+ items. To provide initial focus, let's use
# the variables from the 2016 GPC breast cancer survey:

# %%
_SQL('''
create table concept_dimension as
select c_fullname as concept_path, c_basecode as concept_cd
from naaccr_ontology
where c_basecode is not null
''')
if IO_TESTING:
    _spark.table('concept_dimension').cache()


# %%
def concept_queries(generated_sql: str) -> tab.DataFrame:
    stmts = generated_sql.split('<*>')
    qs = pd.DataFrame(dict(stmt=stmts))
    qs['subq'] = qs.stmt.str.extract(r'\((\s*select concept[^)]*)\)')
    qs.subq = qs.subq.str.replace('i2b2demodata.', '', regex=False)
    qs['patt'] = qs.subq.str.extract(r"like '([^']+)'")
    qs['path'] = qs.patt.str.replace('\\\\', '\\', regex=False).str.replace('%', '')
    qs.subq = qs.subq.str.replace('\\', '\\\\', regex=False)
    return qs[~qs.path.isnull()]


if IO_TESTING:
    _qs = concept_queries(res.read_text(bc_qa, 'bc204_generated.sql'))
IO_TESTING and _qs[['subq', 'path']].head()

# %%
if IO_TESTING:
    _to_spark('bc_terms', lambda: _qs[['path']])
    _qs['df'] = _qs.subq.apply(_spark.sql)
    _bigq = reduce(lambda acc, next: acc.union(next), _qs.df.values[:3])
    _qterms = _bigq.toPandas()

IO_TESTING and _qterms


# %% [markdown]
# @@ really? `\\0610 Class of Case\\30 \\%'` with a space after 30?
#
# and morph...

# %% {"slideshow": {"slide_type": "skip"}}
def cancerIdSample(spark: SparkSession_T, tumors: DataFrame,
                   portion: float = 1.0, cancerID: int = 1) -> DataFrame:
    """Cancer Identification items from a sample

    """
    cols = ont.NAACCR_I2B2.tumor_item_type
    cols = cols[cols.sectionId == cancerID]
    colnames = cols.naaccrId.values.tolist()
    # TODO: test data for morphTypebehavIcdO2 etc.
    colnames = [cn for cn in colnames if cn in tumors.columns]
    return tumors.sample(False, portion).select(colnames)


if IO_TESTING:
    _cancer_id = cancerIdSample(_spark, _extract)

# IO_TESTING and non_blank(_cancer_id.limit(15).toPandas())  # @@@

# %% {"slideshow": {"slide_type": "skip"}}
IO_TESTING and _cancer_id.toPandas().describe()


# %%
class CancerStudy:
    bc_variable = ont._with_path(res.path(bc_qa, 'bc-variable.csv'), tab.read_csv)


IO_TESTING and _spark.createDataFrame(
    CancerStudy.bc_variable).limit(5).toPandas()


# %%
def itemNumOfPath(bc_var: DataFrame,
                  item: str = 'item') -> DataFrame:
    digits = func.regexp_extract('concept_path',  # noqa @@@
                                 r'\\i2b2\\naaccr\\S:[^\\]+\\(\d+)', 1)
    items = bc_var.select(digits.cast('int').alias(item)).dropna().distinct()  # type: ignore
    return items.sort(item)  # type: ignore


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


# %% [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Patients, Encounters, and Observations per Variable
#
#   - **ISSUE**: naaccr-xml test data has no data on classOfCase etc.
#     at least not the 100 tumor sample.

# %% {"slideshow": {"slide_type": "skip"}}
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


# %%
IO_TESTING and bc_var_summary(
    _spark, _obs, _bc_ddict).where(
        'fact_qty is null').toPandas()

# %%
IO_TESTING and bc_var_summary(
    _spark, _obs, _bc_ddict).where(
        'fact_qty is not null').toPandas()

# %%
IO_TESTING and _obs.where("naaccrId == 'dateOfDiagnosis'").limit(5).toPandas()


# %% [markdown] {"slideshow": {"slide_type": "subslide"}}
# #### TODO: Code labels; e.g. 1 = Male; 2 = Female

# %%
def pivot_obs_by_enc(skinny_obs: DataFrame,
                     pivot_on: str = 'naaccrId',  # cheating... not really in i2b2 observation_fact
                     # TODO: nval_num etc. for value cols?
                     value_col: str = 'concept_cd',
                     key_cols: List[str] = ['recordId', 'patientIdNumber']) -> DataFrame:
    groups = skinny_obs.select(pivot_on, value_col, *key_cols).groupBy(*key_cols)
    wide = groups.pivot(pivot_on).agg(func.first(value_col))  # noqa @@@
    return wide


IO_TESTING and pivot_obs_by_enc(_obs.where(
    _obs.naaccrId.isin(['dateOfDiagnosis', 'primarySite', 'sex', 'dateOfBirth'])
)).limit(5).toPandas().set_index(['recordId', 'patientIdNumber'])
