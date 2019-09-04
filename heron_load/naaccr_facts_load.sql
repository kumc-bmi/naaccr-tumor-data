/** naaccr_facts_load.sql -- load i2b2 facts from NAACCR tumor registry data

Copyright (c) 2013-2019 University of Kansas Medical Center
see LICENSE file for license details.

ISSUE: parameterize nightherondata schema?

 * ack: "Key, Dustin" <key.d@ghc.org>
 * Thu, 18 Aug 2011 16:16:31 -0700
 *
 * see also: naaccr_txform.sql
 */

/* check dependencies */
select patientIdNumber from naaccr_patients where 'dep' = 'tumor_reg_tasks.NAACCR_Patients';
select encounter_num from naaccr_tumors where 'dep' = 'tumor_reg_tasks.NAACCR_Visits';


whenever sqlerror continue;
drop index naaccr_patients_pk;
whenever sqlerror exit;

create unique index naaccr_patients_pk on naaccr_patients (patientIdNumber);

-- ISSUE: where to put this check?
select case when count(*) = 0 then 1 else 0 end complete
from naaccr_patients trpat
where patient_num is null
;
commit;

whenever sqlerror continue;
drop index naaccr_tumors_pk;
whenever sqlerror exit;
create unique index naaccr_tumors_pk on naaccr_tumors (recordId);

delete from NightHeronData.encounter_mapping
where encounter_ide_source = :encounter_ide_source;
commit;

insert into NightHeronData.encounter_mapping
  (encounter_num, encounter_ide,
   encounter_ide_status, encounter_ide_source, project_id,
   patient_ide, patient_ide_source,
   import_date, upload_id, download_date, sourcesystem_cd )
 select encounter_num
      , tv.recordId as encounter_ide
      , 'A' as encounter_ide_status
      -- see also select source_cd from nightherondata.source_master
      , :encounter_ide_source as encounter_ide_source
      , :project_id as project_id
      , tv.patientIdNumber as patient_ide
      , :patient_ide_source as patient_ide_source
      , current_timestamp as import_date
      , :upload_id as upload_id
      , tv.dateCaseReportExported as download_date
      , :encounter_ide_source as sourcesystem_cd
  from naaccr_tumors tv
;

/* check for dups from Spark SQL:

select * from naaccr_observations;
select ENCOUNTER_ide, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, count(*)
from naaccr_observations
group by ENCOUNTER_ide, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM
having count(*) > 1;
*/

create table observation_fact_&&upload_id as
select * from nightherondata.observation_fact where 1 = 0;

insert /*+ append*/ into observation_fact_&&upload_id
  ( encounter_num
  , patient_num
  , concept_cd
  , provider_id
  , start_date
  , modifier_cd
  , instance_num
  , valtype_cd
  , tval_char
  , nval_num
  , valueflag_cd
--  , quantity_num
  , units_cd
  , end_date
  , location_cd
--  , observation_blob
--  , confidence_num
  , update_date
  , import_date
  , upload_id
  , download_date
  , sourcesystem_cd
  )
select
    -- use sub-select to be sure cardinality doesn't change
    (select encounter_num
     from naaccr_tumors t
     where t.recordId = tf.recordId) as encounter_num
  , (select patient_num from naaccr_patients pat
     where pat.patientIdNumber = tf.patientIdNumber) as patient_num,
  tf.concept_cd,
  tf.provider_id,
  tf.start_date,
  tf.modifier_cd,
  tf.instance_num,
  tf.valtype_cd,
  tf.tval_char,
  tf.nval_num, tf.valueflag_cd, tf.units_cd,
  tf.end_date,
  tf.location_cd,
  tf.update_date,
  cast(current_timestamp as date) import_date, :upload_id upload_id, :download_date download_date, :source_cd source_cd
from (select * from naaccr_observations
/* TODO
      union all
      select * from seer_recode_facts
      union all
      select * from cs_site_factor_facts*/) tf
;

commit;


/* For this upload of data, check primary key constraints. */
create unique index observation_fact_pk_&&upload_id on observation_fact_&&upload_id
 (ENCOUNTER_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM);

/* debug dups:

select ENCOUNTER_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM, count(*)
from observation_fact_&&upload_id
group by ENCOUNTER_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM
having count(*) > 1;
*/


/** Summary stats.

Report how many rows are dropped when joining on patient_ide and encounter_id.
Subsumes check for null :part (#789).
*/
select count(distinct patient_num) as pat_qty, count(distinct encounter_num) visit_qty, count(*) as obs_qty
from observation_fact_&&upload_id;
