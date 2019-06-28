# NAACCR Tumor Data

## Goals

 - FIELDS, VALUESETS a la [PCORNET CDM][pcdm]
 - "data lake" ETL for [i2b2][]
   - [multiple fact tables](https://community.i2b2.org/wiki/display/MFT)
     - i2b2 1.7.09 or higher
     - `crc.properties ` set `queryprocessor.multifacttable=true`
     - in ontology table, set `c_facttablecolumn=NAACCR_FACT.concept_cd`
   - enhancement? sentinels for missing data a la [WerthPADOH][PADOH]
     - "Absence of data is not represented within OMOP." -- [NAACCR treatment ETL instructions](https://github.com/OHDSI/OncologyWG/wiki/ETL-Instructions-for-Mapping-NAACCR-Treatment-Data-into-the-OMOP-CDM)

## Previous work

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
