# TODO: prune pyspark jupyter nb stuff incl. synthetic_data.py

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

    ndd = ScrapedChapters.make_in(_spark, _cwd / 'naaccr_ddict')
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
