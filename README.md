# NAACCR Tumor Registry v18 Data in i2b2 etc. (WIP)

The [NAACCR_ETL][gpc] process used at KUMC and other GPC sites to load
tumor registry data into [i2b2][] is **outdated by version 18** of the
[NAACCR standard][dno]. We are taking this opportunity to reconsider
our platform and approach:

  - Portable to database engines other than Oracle
  - Explicit tracking of data flow to facilitate parallelism
  - Rich test data to facilitate development without access to private data
  - Separate repository from HERON EMR ETL to avoid
    *information blocking* friction

This is very *much work in progress*.

ref:

  - Thornton ML, (ed). [Standards for Cancer Registries Volume II: Data
    Standards and Data Dictionary][dno], Record Layout Version 18, 21st
    ed. Springfield, Ill.: North American Association of Central
    Cancer Registries, February 2018, (Revised Mar. 2018, Apr. 2018,
    May 2018, Jun. 2018, Aug. 2018, Sept. 2018, Oct. 2018).

---

## Previous work: KUMC HERON i2b2 NAACCR ETL

  - 2011: HERON i2b2 clinical data warehouse helps KUMC win CTSA award
  - 2011: HERON [TumorRegistry][] integration helps KU Med Center win NCI designation
  - 2017: [Using the NAACCR Cancer Registry in i2b2 with HERON
    ETL][2017bos] presented by Dan Connolly at i2b2 tranSMART
    Foundation User Group Meeting

please cite:

  * Waitman LR, Warren JJ, Manos EL, Connolly DW.  [Expressing
    Observations from Electronic Medical Record Flowsheets in an i2b2
    based Clinical Data Repository to Support Research and Quality
    Improvement][RW2011].  AMIA Annu Symp Proc. 2011;2011:1454-63. Epub 2011
    Oct 22.

[RW2011]: http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3243191/

---

## Previous work: GPC Breast Cancer Survey

 - [GPC Breast Cancer Data Quality Reporting][bc_qa] - multi-site
   project with REDCap Data Dictionary
   - "On 23 **Dec 2014**, GPC honest brokers were requested to run a
     breast cancer cohort query ..."
 - [NAACCR_ETL][gpc] - GPC NAACCR ETL wiki page

*TODO: add citations for GPC, BC survey*

[TumorRegistry]: https://informatics.kumc.edu/work/wiki/TumorRegistry
[2017bos]: http://www.kumc.edu/Documents/eami/Using%20the%20NAACCR%20Tumor%20Registry%20in%20i2b2%20with%20HERON%20ETL.pdf

---

## Platform for v18: Spark SQL, PySpark, and luigi

 - **Spark SQL**
   - lets us leverage the **working knowledge of SQL** in our community
     - HERON ETL: 30KLOC of SQL
   - portable: **same JVM platform as i2b2**; **JDBC** connectivity to datamarts
 - **PySpark** to fill in gaps where SQL is awkward, such as
   - iterating over columns or tables
 - **luigi**
   - While Spark automatically breaks down work in a resilient manner, jobs
     fail completely when they fail.
   - luigi tasks **preserve partial results**

---

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


See `client.cfg` for more details on luigi usage.

---

### TODO: Coded concepts

Metadata for coded values is also work in progress.

  - HERON ETL was based on a NAACCR v12 MS Access DB that no longer seems to be
    maintained / published.
  - exploring LOINC answer lists (from v11 and v12)
  - well curated code-labels: [naaccr][PADOH] NAACCR reader in R by
    N. Werth of PA Dept. of Health

---

### Related work: NAACCR XML

 - [naaccr-xml][ims] - NAACCR XML reader in Java by F. Depry of IMS for SEER
   - first release: v0.5 (beta) Apr 20, 2015
   - frequent release:
     - v5.4 Jun 13, 2019
     - v5.3 May 21, 2019
     - v1.0 Feb 7, 2016
 - XML replaces flat file in 2020 (*IOU citation*)
 - Reading XML with Spark is straightforward
 - NAACCR XML WG meets alternate Fridays 11amET (e.g. Aug 2)

---

### TODO: site-specific factors, primary sites, morphologies

We have yet to port / integrate the code from `heron_staging/tumor_reg` that gets

 - primary sites and morphologies from WHO
 - SEER site summary
 - site-specific factors from cancerstaging.org
   -  added to HERON March 2016; see [GPC ticket 150](https://informatics.gpcnetwork.org/trac/Project/ticket/150)
   - These are obsolete in cases abstracted per v18 but still used in older cases.
   - [WerthPADOH][PADOH] issue: [​Handle site-specific codes in fields #35](https://github.com/WerthPADOH/naaccr/issues/35) opened Apr 24 2019

---

### Related work: OMOP / OHDSI Ongology Working Group

 - [OMOP CDM WG - Oncology Subgroup][owg]
   - [call for use cases](http://forums.ohdsi.org/t/oncology-data-use-cases/4382/18) May 2018
     - revived after Jun 25 meeting
   - using [SEER API](https://api.seer.cancer.gov/swagger-ui.html) in vocabulary mapping work
   - Standing weekly meetings: Tuesday, 11 am ET. (e.g. 7/9/2019)

---

### Wish list: missing data sentinels

  - [WerthPADOH][PADOH] includes **sentinel codes for missing data**
    - e.g. Grade code 9 = "Grade/differentiation unknown, not stated, or not applicable"
  - "Absence of data is not represented within OMOP." -- [OMOP Oncology WG NAACCR
    treatment ETL instructions][omop1]

[omop1]: https://github.com/OHDSI/OncologyWG/wiki/ETL-Instructions-for-Mapping-NAACCR-Treatment-Data-into-the-OMOP-CDM


---

## Use case: GPC Breast Cancer survey

  - GENERATED_SQL for two i2b2 queries from GPC Breast cancer work

See `test_data/bcNNN_generated.sql`.

---

### Toward Synthetic NAACCR Test Data

In `test_data`:

  - capture statistics of NAACCR data
  - synthesize test data with similar distributions

See `test_data/data_char_sim.sql`, `test_data/tr_summary.py`

---

### Goal: data characterization, checks, charts

jupyter notebook of checks, charts

  - PCORNet CDM Emperical Data Characterization report
  - GPC Breast Cancer QA reports

---

## Toward PCORNet CDM Integration

 - exploring FIELDS, VALUESETS a la [PCORNET CDM][pcdm] for a TUMOR table 
 - Another approach: i2b2 `OBSERVATION_FACT` -> PCORNet `OBS_GEN`.
   - crosswalk to LOINC (as of NAACCR v12): `loinc-naaccr/loinc_naaccr.csv`
     - *on loinc-csvdb branch*

See `pcornet_cdm/` directory.

---

## Goal: "data lake" ETL, multiple i2b2 fact tables

 - HERON ETL 2011: copy NAACCR flat file into DB with Oracle sqlldr
 - Spark approach:
   1. specify transformation from the NAACCR flat file to an i2b2 fact table
      - use PySpark to un-pivot / melt
   2. `facts.write.jdbc(...oracle db...)`
      - Spark it schedules work of transforming the flat file.
 - aim to use [multiple fact tables](https://community.i2b2.org/wiki/display/MFT)
   in i2b2 1.7.09.
     - `crc.properties ` set `queryprocessor.multifacttable=true`
     - in ontology table, set `c_facttablecolumn=NAACCR_FACT.concept_cd`

---

## Related work: FHIR / HL7

 - [HL7 FHIR Implementation Guide: Breast Cancer Data, Release 1 - US
Realm](http://hl7.org/fhir/us/breastcancer/2018Sep/) (Draft for Comment 2)
 - [mCODE][] - Standard Health Record Collaborative HL7 FHIR
   Implementation Guide: minimal Common Oncology Data Elements
   (mCODE), v0.9.0, Version: 0.9.0 ; FHIR © Version: 1.0.2 ; Generated
   on Wed, Apr 17, 2019 12:01-0400.

---

[dno]: http://datadictionary.naaccr.org/
[ims]: https://github.com/imsweb/naaccr-xml
[PADOH]: https://github.com/WerthPADOH/naaccr
[gpc]: https://informatics.gpcnetwork.org/trac/Project/wiki/NAACCR_ETL
[bc_qa]: https://github.com/kumc-bmi/bc_qa
[pcdm]: https://github.com/CDMFORUM
[i2b2]: https://transmartfoundation.org/
[owg]: https://www.ohdsi.org/web/wiki/doku.php?id=projects:workgroups:oncology-sg
[mCODE]: http://standardhealthrecord.org/guides/mcode/
