# NAACCR Tumor Data v18 in Datamarts such as i2b2 (WIP)

The [NAACCR_ETL][gpc] process used at KUMC and other GPC sites to load
tumor registry data into [i2b2][] is outdated by version 18 of the NAACCR
standard. We are taking this opportunity to reconsider our platform
and approach:

  - Separate from the main HERON EMR ETL for easier sharing
  - Portable to database engines other than Oracle
  - Explicit tracking of data flow to facilitate parallelism
  - Rich test data to facilitate development without access to private data

This is very much work in progress.


## Previous work: HERON NAACCR ETL, GPC Breast Cancer Survey

  - [Using the NAACCR Cancer Registry in i2b2 with HERON ETL][2017bos], Dan Connolly
i2b2 tranSMART Foundation User Group Meeting June 20, 2017
 - [GPC Breast Cancer Data Quality Reporting][bc_qa] - multi-site
   project with REDCap Data Dictionary
   - "On 23 **Dec 2014**, GPC honest brokers were requested to run a
     breast cancer cohort query ..."
 - [NAACCR_ETL][gpc] - GPC NAACCR ETL work
 - [TumorRegistry][] HERON NAACCR ETL - toward 2011 KU Med Center NCI designation

[TumorRegistry]: https://informatics.kumc.edu/work/wiki/TumorRegistry
[2017bos]: http://www.kumc.edu/Documents/eami/Using%20the%20NAACCR%20Tumor%20Registry%20in%20i2b2%20with%20HERON%20ETL.pdf


## Platform for v18: Spark SQL, PySpark, and luigi

Spark SQL is a portable platform that lets us leverage the
considerable working knowledge of SQL in our community, especially our
experience developing HERON ETL (30KLOC of SQL).

We use PySpark to fill in gaps where SQL is awkward, such as iterating
over columns or tables.

While Spark automatically breaks down work in a resilient manner, jobs
fail completely when they fail. luigi allows us to break down work into
tasks so that completion of a task preserves its results.


## Example: NAACCR Ontology for i2b2

To scrape the NAACCR record layout and such:
```
$ (cd naaccr_ddict; python scrape.py)
INFO:__main__:record_layout.csv: 802 items
INFO:__main__:data_descriptor.csv: 890 items
INFO:__main__:item_description.csv: 890 items
```

Then use `heron_load/naaccr_concepts_load.sql` from HERON ETL to build
an i2b2 ontology. It was ported from Oracle SQL to Spark SQL for use
in PySpark module `tumor_reg_ont.py`, which is used in
the `tumor_reg_tasks.NAACCR_Ontology1` luigi task:

```
$ luigi --module tumor_reg_tasks NAACCR_Ontology1 --naaccr-ddict=naaccr_ddict
DEBUG: Checking if NAACCR_Ontology1(design_id=upper, naaccr_version=18, naaccr_ch10_bytes=3078052) is complete
15:48:09 INFO: ...status   PENDING
15:48:09 INFO: Running Worker with 1 processes
...
15:48:20 ===== Luigi Execution Summary =====
15:48:20 
15:48:20 Scheduled 1 tasks of which:
15:48:20 * 1 ran successfully:
15:48:20     - 1 NAACCR_Ontology1(...)
```


### TODO: Coded concepts, site-specific factors, primary sites, morphologies

We have yet to port / integrate the code from `heron_staging/tumor_reg` that gets

 - primary sites and morphologies from WHO
 - site-specific factors from cancerstaging.org
   - These are obsolete in cases abstracted per v18 but still used in older cases.

Metadata for coded values is also work in progress. HERON ETL was
based on a NAACCR v12 MS Access DB that no longer seems to be
maintained / published. We're exploring LOINC, which has answer lists
for values from v11 and v12, as well as [WerthPADOH][PADOH], which
seems to have well curated code-labels.


### Wish list: missing data sentinels

[WerthPADOH][PADOH] also includes considerable work on distinguishing
sentinel codes for missing data.

"Absence of data is not represented within OMOP." -- [NAACCR treatment
ETL
instructions](https://github.com/OHDSI/OncologyWG/wiki/ETL-Instructions-for-Mapping-NAACCR-Treatment-Data-into-the-OMOP-CDM)


## Use case: GPC Breast Cancer survey

In `test_data`, we have GENERATED_SQL for two i2b2 queries from GPC
Breast cancer work.


## Toward Synthetic NAACCR Test Data

In `test_data`, we are working on `data_char_sim.sql` and
`tr_summary.py` to capture statistics of NAACCR data and synthesize
test data with similar distributions.


## Toward a PCORNet CDM Tumor Table

In `pcornet_cdm` we are exploring FIELDS, VALUESETS a la [PCORNET CDM][pcdm].

Another approach is to take the i2b2 fact table and load it into OBS_GEN.


## Goal: "data lake" ETL, multiple i2b2 fact tables

Unlike earlier HERON ETL work, where we use Oracle sqlldr to copy the
NAACCR flat file into a table before processing it, we has prototyped
more of a "data lake" approach: we use Spark SQL and PySpark to
describe the transformation from the NAACCR flat file to an i2b2 fact
table. When we ask Spark to write the fact table, it schedules the
work of reading and transforming the flat file.

We aim to take advantage of support for [multiple fact
tables](https://community.i2b2.org/wiki/display/MFT) in i2b2 1.7.09.
Some notes:

     - `crc.properties ` set `queryprocessor.multifacttable=true`
     - in ontology table, set `c_facttablecolumn=NAACCR_FACT.concept_cd`


## Related Work

  - Thornton ML, (ed). [Standards for Cancer Registries Volume II: Data
    Standards and Data Dictionary][dno], Record Layout Version 18, 21st
    ed. Springfield, Ill.: North American Association of Central
    Cancer Registries, February 2018, (Revised Mar. 2018, Apr. 2018,
    May 2018, Jun. 2018, Aug. 2018, Sept. 2018, Oct. 2018).
 - [naaccr-xml][ims] - NAACCR XML reader in Java by F. Depry of IMS for SEER
   - v5.4 Jun 13, 2019
   - v5.3 May 21, 2019
   - v1.0 Feb 7, 2016
   - v0.5 (beta) Apr 20, 2015
 - [OMOP CDM WG - Oncology Subgroup][owg]
   - [call for use cases](http://forums.ohdsi.org/t/oncology-data-use-cases/4382/18) May 2018
     - revived after Jun 25 meeting
   - using [SEER API](https://api.seer.cancer.gov/swagger-ui.html) in vocabulary mapping work
   - Next meeting: 7/9/2019
   - Standing weekly meetings: Tuesday, 11 am ET.
 - [naaccr][PADOH] - NAACCR reader in R by N. Werth of PA Detp. of Health
   - nifty approach to sentinels for missing data
   - [​Handle site-specific codes in fields #35](https://github.com/WerthPADOH/naaccr/issues/35)
     WerthPADOH opened this issue on Apr 24 2019; added to HERON March 2016; see [GPC ticket 150](https://informatics.gpcnetwork.org/trac/Project/ticket/150)

HL7 FHIR Implementation Guide: Breast Cancer Data, Release 1 - US Realm (Draft for Comment 2)
http://hl7.org/fhir/us/breastcancer/2018Sep/



 - [mCODE][] - Standard Health Record Collaborative HL7 FHIR
   Implementation Guide: minimal Common Oncology Data Elements
   (mCODE), v0.9.0, Version: 0.9.0 ; FHIR © Version: 1.0.2 ; Generated
   on Wed, Apr 17, 2019 12:01-0400.

[dno]: http://datadictionary.naaccr.org/
[ims]: https://github.com/imsweb/naaccr-xml
[PADOH]: https://github.com/WerthPADOH/naaccr
[gpc]: https://informatics.gpcnetwork.org/trac/Project/wiki/NAACCR_ETL
[bc_qa]: https://github.com/kumc-bmi/bc_qa
[pcdm]: https://github.com/CDMFORUM
[i2b2]: https://transmartfoundation.org/
[owg]: https://www.ohdsi.org/web/wiki/doku.php?id=projects:workgroups:oncology-sg
[mCODE]: http://standardhealthrecord.org/guides/mcode/
