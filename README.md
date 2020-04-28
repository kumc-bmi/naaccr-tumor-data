# NAACCR Tumor Registry v18 Data in i2b2 etc.: Beta Testing

_While various issues remain, KUMC put a version of this code
in production in the [Sep 2019
HERON Great Salt Lake
release](https://bmi-work.kumc.edu/work/blog/2019/09/slug)._

---

## NAACCR File ETL: Getting Started

We suppose you have access to your NAACCR v18 file; for testing,
you can use [naaccr-xml-sample-v180-incidence-100.txt](https://raw.githubusercontent.com/kumc-bmi/naaccr-tumor-data/onejar/naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt).

For integration testing, we use the [H2 Database Engine](https://www.h2database.com/),
but when you are ready, you should configure access to Postgres, SQL Server, or Oracle as in section
[3.4.2 Set Database Properties](https://community.i2b2.org/wiki/display/getstarted/3.4.2+Set+Database+Properties)
of the [i2b2 installation guide](https://community.i2b2.org/wiki/display/getstarted/i2b2+Installation+Guide).

```properties
db.username=SA
db.password=
db.driver=org.h2.Driver
db.url=jdbc:h2:file:/tmp/DB1;create=true

naaccr.flat-file=naaccr-xml-sample-v180-incidence-100.txt
naaccr.records-table: NAACCR_RECORDS
naaccr.extract-table: NAACCR_DATA
naaccr.stats-table: NAACCR_STATS
```

Then, to create `NAACCR_OBSERVATIONS`, run:

```shell script
java -jar naaccr-tumor-data.jar facts
```

_Please excuse / ignore `WARNING: An illegal reflective access ...`;
see [issue 30](https://github.com/kumc-bmi/naaccr-tumor-data/issues/30)._

_Note: adding these observations to the i2b2 star schema requires running `naaccr_facts_load.sql`; porting
that script to othere databases / environments is still in-progress._

---

### naaccr-tumor-data.jar Usage Reference

_The `naaccr-tumor-data` command is short for `java -jar naaccr-tumor-data.jar`._

```
Usage:
  naaccr-tumor-data load-records [--db=PF]
  naaccr-tumor-data discrete-data [--no-file] [--db=PF]
  naaccr-tumor-data summary  [--no-file] [--db=F] [--task-id=ID]
  naaccr-tumor-data tumors   [--no-file] [--db=F] [--task-id=ID]
  naaccr-tumor-data facts    [--no-file] [--db=F] [--task-id=ID]
  naaccr-tumor-data ontology [--table-name=N] [--version=V] [--task-hash=H] [--update-date=D] [--who-cache=D]
  naaccr-tumor-data import [--db=F] TABLE DATA META
  naaccr-tumor-data load [--db=F]
  naaccr-tumor-data run SCRIPT [--db=F]
  naaccr-tumor-data query SQL [--db=F]

Options:
  load-records       load NAACCR records into a (CLOB) column of a DB table
  discrete-data      split NAACCR records into a wide table of discrete data
  tumors             build NAACCR_TUMORS table
  facts              build NAACCR_OBSERVATIONS table
  summary            build NAACCR_EXTRACT_STATS table
  --db=PROPS         database properties file [default: db.properties]
  --no-file          use loaded records rather than file as input
  --task-id=ID       version / completion marker [default: task123]
  ontology           build NAACCR_ONTOLOGY table
  --table-name=T     ontology table name [default: NAACCR_ONTOLOGY]
  --version=NNN      ontology version [default: 180]
  --task-hash=H      ontology completion marker
  --update-date=D    ontology update_date in YYYY-MM-DD format
  --who-cache=DIR    where to find WHO oncology metadata
  import             import CSV
  TABLE              target table name
  DATA               CSV file
  META               W3C tabular data metadata (JSON)
  load               load data from stdin using JSON (details TODO)
  run                run SQL script
  query              run SQL query and write results to stdout in JSON

```

---

## NAACCR Record Layout Version 18

The [NAACCR_ETL][gpc] process used at KUMC and other GPC sites to load
tumor registry data into [i2b2][] is **outdated by version 18** of the
[NAACCR standard][dno].

ref:

  - Thornton ML, (ed). [Standards for Cancer Registries Volume II: Data
    Standards and Data Dictionary][dno], Record Layout Version 18, 21st
    ed. Springfield, Ill.: North American Association of Central
    Cancer Registries, February 2018, (Revised Mar. 2018, Apr. 2018,
    May 2018, Jun. 2018, Aug. 2018, Sept. 2018, Oct. 2018).

---

## Previous work: KUMC HERON i2b2 NAACCR ETL

  - 2011: [HERON i2b2 clinical data warehouse helps KUMC win CTSA award][bmi-ctsa]
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
  * Rogers AR, Lai S, Keighley J, Jungk J. [The Incidence of Breast
    Cancer among Disabled Kansans with Medicare][JK15] KJM 2015-08


[RW2011]: http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3243191/
[bmi-ctsa]: https://informatics.kumc.edu/work/blog/2011/06/ctsa-grant-funded
[JK15]: https://doi.org/10.17161/kjm.v8i3.11526

---

## Previous work: GPC Breast Cancer Survey

 - [GPC Breast Cancer Data Quality Reporting][bc_qa] - multi-site
   project with REDCap Data Dictionary
   - "On 23 **Dec 2014**, GPC honest brokers were requested to run a
     breast cancer cohort query ..."
 - [NAACCR_ETL][gpc] - GPC NAACCR ETL wiki page

cite:

  * Chrischilles et. al.  [Upper extremity disability and quality of
   life after breast cancer treatment in the Greater Plains
   Collaborative clinical research network.][EC2019] Breast Cancer Res
   Treat. 2019 Jun;175(3):675-689. doi:
   10.1007/s10549-019-05184-1. Epub 2019 Mar 9.
  * Waitman LR, Aaronson LS, Nadkarni PM, Connolly DW, Campbell
    JR. [The greater plains collaborative: a PCORnet clinical research
    data network.][RW2014] J Am Med Inform
    Assoc. 2014;21:637–641. doi: 10.1136/amiajnl-2014-002756.


[EC2019]: https://www.ncbi.nlm.nih.gov/pubmed/30852760
[RW2014]: https://www.ncbi.nlm.nih.gov/pubmed/24778202

[TumorRegistry]: https://informatics.kumc.edu/work/wiki/TumorRegistry
[2017bos]: http://www.kumc.edu/Documents/eami/Using%20the%20NAACCR%20Tumor%20Registry%20in%20i2b2%20with%20HERON%20ETL.pdf

---

## Building on NAACCR XML from SEER

 - [naaccr-xml][ims] - NAACCR XML reader in Java by F. Depry of IMS for SEER
   - first release: v0.5 (beta) Apr 20, 2015; v1.0 Feb 7, 2016
   - frequent release:
     - v6.6 Feb 6, 2020
     - ...
     - v5.4 Jun 13, 2019
     - v5.3 May 21, 2019
 - XML replaces flat file in 2020 (*IOU citation*)
 - also supports flat files
 - [imsweb/layout](https://github.com/imsweb/layout) has sections, codes, etc.
 - NAACCR XML WG meets alternate Fridays 11amET (e.g. Aug 2)

---

## Coded concepts

Metadata for coded values is also work in progress.

  - HERON ETL was based on a NAACCR v12 MS Access DB that no longer seems to be
    maintained / published.
  - currently using a mix of:
    - LOINC answer lists (from v11 and v12)
    - well curated code-labels: [naaccr][PADOH] NAACCR reader in R by
      N. Werth of PA Dept. of Health
  - hope to incorporate codes from [imsweb/layout](https://github.com/imsweb/layout)

---

### Primary sites, morphologies from WHO

Maintained by the World Health Organization (WHO)

 - primary sites: e.g. `C50` for Breast
   - i2b2 ontology support ported from HERON ETL
 - morphology: `9800/3` for Leukemia
   - morphology i2b2 ontology support TODO

---

## SEER Site Recode

 - combines primary site and histology
 - e.g. `20010` for **Lip**
 - i2b2 ontology support ported from HERON ETL

---

## site-specific factors

Obsolete in 2018, but to capture data from older cases...

 - site-specific factors from cancerstaging.org
   -  added to HERON March 2016; see [GPC ticket 150](https://informatics.gpcnetwork.org/trac/Project/ticket/150)
   - These are obsolete in cases abstracted per v18 but still used in older cases.
   - [WerthPADOH][PADOH] issue: [​Handle site-specific codes in fields #35](https://github.com/WerthPADOH/naaccr/issues/35) opened Apr 24 2019

---

## Platform for v18: JVM, JDBC, H2 DB, tablesaw, groovy, (and luigi)

We are taking this opportunity to reconsider
our platform and approach:

  - Portable to database engines other than Oracle
  - Explicit tracking of data flow to facilitate parallelism
  - Rich test data to facilitate development without access to private data
  - Separate repository from HERON EMR ETL to avoid
    *information blocking* friction

See [CONTRIBUTING](CONTRIBUTING.md) for details.

 - **JDBC**
   - lets us leverage the **working knowledge of SQL** in our community
     - HERON ETL: 30KLOC of SQL
   - portable: **same JVM platform as i2b2**
   - **JDBC** connectivity to datamarts
   - **[H2](https://www.h2database.com/html/main.html)** for in-memory DB
 - **groovy** to fill in gaps where SQL is awkward, such as
   - iterating over columns or tables
   - [tablesaw](https://jtablesaw.github.io/tablesaw/) Dataframe library
     a la python pandas, Spark
   - _difference from Java worthwhile? see [CONTRIBUTING](CONTRIBUTING.md)_
 - **luigi** (optional)
   - luigi tasks **preserve partial results**

---

## NAACCR Ontology for i2b2: Luigi Usage

The `NAACCR_Ontology1` task creates a `NAACCR_ONTOLOGY` table:

```
$ luigi --module tumor_reg_tasks NAACCR_Ontology1
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


If you're interested in luigi usage, see `client.cfg` for details. If not, see:

  - `tumor_reg_ont.py`
  - `heron_load/tumor_item_value.csv`, and
  - `heron_load/naaccr_concepts_load.sql`

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

## `NAACCR_PATIENTS`, `NAACCR_TUMORS`, and `NAACCR_OBSERVATIONS`: Usage

Using `tumor_reg_data.py`, the `NAACCR_Load` task turns a NAACCR v18
flat file into tables for patients, tumors, and observations:

```
$ luigi --module tumor_reg_tasks NAACCR_Load
...
15:02:04 5890 INFO: Informed scheduler that task   NAACCR_Load_2019_08_20_tumor_registry_k_1306872023_225d26f0cd   has status   DONE
```

The tables are:

   - `naaccr_patients`, `naaccr_tumors`, `naaccr_observations`
   - `observation_fact_NNNN`, `observation_fact_deid_NNNN`
      - where NNNN is an upload_id

`naaccr_patients` and `observation_fact_NNNN` depend on an existing
`patient_mapping` table.  `naaccr_tumors` uses a reserved range of
`encounter_num`.

_TODO: publish generated notebook; smooth out the level of detail here vs. there._

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
