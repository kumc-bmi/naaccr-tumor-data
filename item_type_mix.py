# TODO: prune pyspark stuff incl. this item_type_mix
# %% [markdown]
# ## tumor_item_type: numeric /  date / nominal / text; identifier?

# %%
IO_TESTING and _spark.sql('''
select *
from ndd180 as idef
''').limit(8).toPandas()

# %% [markdown]
# Add year implemented, retired from chapter 10...

# %%
False and _spark.sql('''
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
False and _spark.sql('''
select *
from item_description idesc
where yr_impl = '366'
''').toPandas()

# %% [markdown]
# ### LOINC SCALE_TYP should help

# %%
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
if IO_TESTING:
    LOINC_NAACCR.measure_in(_spark)

False and  _spark.sql('''
with check as (
select idesc.yr_impl, idesc.yr_retired
     , case when ln.code_value is null then 0 else 1 end as has_loinc
from ndd180 v18
left join record_layout rl on rl.`naaccr-item-num` = v18.naaccrNum
left join loinc_naaccr ln on ln.code_value = v18.naaccrNum
)
select yr_impl, yr_retired, has_loinc, count(*) from check
group by yr_impl, yr_retired, has_loinc

union all select -1, -1, has_loinc, count(*) from check
group by has_loinc

order by has_loinc, yr_impl, yr_retired
''').toPandas()

# %% [markdown]
# #### LOINC scale_typ for dates: Qn

# %%
IO_TESTING and _spark.sql('''
select *
from loinc_naaccr ln
where ln.code_value = 390
''').toPandas()

# %% [markdown]
# ### Try Werth PADoH curation

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
# ### Mix naaccr-xml, LOINC, and Werth

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
def csv_meta(dtypes, path,
             context='http://www.w3.org/ns/csvw'):
    # ISSUE: dead code? obsolete in favor of _fixna()?
    def xlate(dty):
        if dty.kind == 'i':
            return 'number'
        elif dty.kind == 'O':
            return 'string'
        raise NotImplementedError(dty.kind)

    cols = [
        { "titles": name,
          "datatype": xlate(dty) }
        for name, dty in dtypes.items()
    ]
    return { "@context": context,
             "url": path,
             "tableSchema": {
                 "columns": cols
             }}

#@@ csv_meta(x.dtypes, 'tumor_item_type.csv')


# %%
def csv_spark_schema(columns):
    """
    Note: limited to exactly 1 titles per column
    IDEA: expand to boolean
    IDEA: nullable / required
    """
    def oops(what):
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
# ### Compare with `tumor_item_type` from heron_load (TODO?)

# %%
# ty_hl = pd.read_csv('heron_load/tumor_item_type.csv').rename(columns=lambda c: c.lower())
if IO_TESTING:
    ty_hl = _spark.table('ty1').toPandas()
    print(len(ty_hl.drop_duplicates()))
IO_TESTING and ty_hl.head()

# %%
if IO_TESTING:
    ty2 = NAACCR_I2B2.tumor_item_type(_spark, _cwd / 'naaccr_ddict')
    print(ty2.distinct().count())
IO_TESTING and ty2.limit(5).toPandas()

# %%
if IO_TESTING:
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
