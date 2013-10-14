/** naaccr_facts_load.sql -- load i2b2 facts from NAACCR tumor registry data

Copyright (c) 2013 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

 * ack: "Key, Dustin" <key.d@ghc.org>
 * Thu, 18 Aug 2011 16:16:31 -0700
 *
 * see also: naacr_init.sql, naacr_txform.sql
 */

/* Check that we're running in the identified repository. */
select * from NightHeronData.observation_fact where 1=0;

/* Check for NAACCR extract table (in KUMC database).
oops... typo in schema name. keep it that way?
*/
select * from naacr.extract@kumc where 1=0;

/* check that transformation views are in place */
select * from tumor_reg_visits@kumc where 1=0;
select * from tumor_reg_facts@kumc where 1=0;


/* Exploration/analysis queries ...

-- How many records did we load from the extract?
select count(*)
from naacr.extract@kumc ne;
-- 65584

-- How many distinct patients? How many tumors per patient?
select count(distinct ne."Patient ID Number") as total_patients,
round(count(*) / count(distinct ne."Patient ID Number"), 3) as tumors_per_patient
from naacr.extract@kumc ne;
-- 60667	1.081


-- Patient mapping: do all of them have Patient IDs?
select count(to_number(ne."Patient ID Number"))
  from NAACR.EXTRACT@KUMC ne;
-- 65584, so yes.

-- How many of them match MRNs from our patient mapping?
select count(*)
from naacr.extract@kumc ne
join NIGHTHERONDATA.patient_mapping pm
  on pm.patient_ide_source =
  (select source_cd from sms_audit_info)
  and pm.patient_ide = ne."Patient ID Number";
-- 0. oops.

-- how long are MRNs in our patient_mapping?
select min(length(pm.patient_ide)),
  max(length(pm.patient_ide))
from NIGHTHERONDATA.patient_mapping pm
where pm.patient_ide_source =
  (select source_cd from sms_audit_info);
-- 6 to 7 chars (bytes? never mind...)

-- How long are Patient ID Numbers?
select min(length(ne."Patient ID Number")),
  max(length(ne."Patient ID Number"))
from naacr.extract@kumc ne;
-- 8. hmm.

-- How many of them match after we drop the 1st digit?
select numerator, denominator, round(numerator/denominator*100, 2) as density
from (
  select count(*) as numerator
  from naacr.extract@kumc ne
  join NIGHTHERONDATA.patient_mapping pm
    on pm.patient_ide_source =
       (select source_cd from sms_audit_info)
   and pm.patient_ide = substr(ne."Patient ID Number", 2)) matches,
  (select count(*) as denominator from naacr.extract@kumc ne) 
;
-- 65183 out of 65584; i.e. 99.39% 


-- How many match if we convert digit-strings to numbers?
select count(*)
from naacr.extract@kumc ne
join NIGHTHERONDATA.patient_mapping pm
  on pm.patient_ide_source =
  (select source_cd from sms_audit_info)
  and to_number(pm.patient_ide) = to_number(ne."Patient ID Number");
-- ORA-01722: invalid number. Bad data somewhere; so we can't tell.
-- FWIW, the NAACCR Patient IDs all convert to_number just fine.
-- The problem is in the Epic/SMS data.

-- What can we use as a primary key?
select count(*) from (
select distinct ne."Accession Number--Hosp", ne."Sequence Number--Hospital"
from naacr.extract@kumc ne);
-- 65581. almost; all but 4.

-- which 4?
select count(*), ne."Accession Number--Hosp", ne."Sequence Number--Hospital"
from naacr.extract@kumc ne
group by ne."Accession Number--Hosp", ne."Sequence Number--Hospital"
having count(*) > 1;

-- are there any nulls?
select count(*)
from naacr.extract@kumc ne
where ne."Accession Number--Hosp" is null;
-- 2
*/


insert into NightHeronData.encounter_mapping
  (encounter_num, encounter_ide,
   encounter_ide_status, encounter_ide_source,
   patient_ide, patient_ide_source,
   import_date, upload_id, download_date, sourcesystem_cd )
(select NightHeronData.SQ_UP_ENCDIM_ENCOUNTERNUM.nextval as encounter_num
      , tv.encounter_ide
      , 'A' as encounter_ide_status
      , aud.source_cd as encounter_ide_source
      , tv.mrn as patient_ide
      , sms_audit_info.source_cd as patient_ide_source
      , sysdate as import_date
      , up.upload_id
      , :download_date
      , up.source_cd
  from tumor_reg_visits@kumc tv
     , (select * from BlueHeronData.source_master@deid
        where source_cd like 'tumor_registry@%') aud
     , (select * from BlueHeronData.source_master@deid
        where source_cd like 'SMS@%') sms_audit_info
     , NightHeronData.upload_status up
  where up.upload_id = :upload_id);



insert into NightHerondata.observation_fact(
  patient_num, encounter_num,
  concept_cd,
  provider_id,
  start_date,
  modifier_cd,
  instance_num,
  valtype_cd,
  tval_char,
  nval_num,
  valueflag_cd,
  units_cd,
  end_date,
  location_cd,
  update_date,
  import_date, upload_id, download_date, sourcesystem_cd)
select patient_num, encounter_num,
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
  sysdate, up.upload_id, :download_date, up.source_cd
from (select * from tumor_reg_facts@kumc
      union all
      select * from seer_recode_facts@kumc) tf
join NIGHTHERONDATA.patient_mapping pm
  on pm.patient_ide_source =
  (select source_cd from BlueHeronData.source_master@deid
   where source_cd like 'SMS@%')
  and ltrim(pm.patient_ide, '0') = ltrim(tf.mrn, '0')
join NIGHTHERONDATA.encounter_mapping em
  on em.encounter_ide_source =
  (select source_cd from BlueHeronData.source_master@deid
   where source_cd like 'tumor_registry@%')
 and em.encounter_ide = tf.encounter_ide
 , NightHeronData.upload_status up
  where up.upload_id = :upload_id
/* don't bother with:    and part = :part */
;

commit;


/* TODO: make some test data */

/* clean up from buggy load.
set timing on;

  CREATE INDEX NIGHTHERONDATA.observation_fact_upload_id
        ON NIGHTHERONDATA.observation_fact (upload_id);

  drop index NIGHTHERONDATA.encounter_mapping_upload_id;
  
  CREATE INDEX NIGHTHERONDATA.encounter_mapping_upload_id
        ON NIGHTHERONDATA.encounter_mapping (upload_id);
  -- Elapsed: 00:00:23.840

select count(*)
from NIGHTHERONDATA.encounter_mapping
where upload_id in (12, 13, 14, 15);


delete
from NIGHTHERONDATA.encounter_mapping
where upload_id in (12, 13, 14, 15);
-- 64,697 rows deleted.
-- Elapsed: 00:00:00.804

delete
from NIGHTHERONDATA.observation_fact
where upload_id=31;

drop INDEX NIGHTHERONDATA.observation_fact_upload_id;

 */

-- summary stats
update NightHeronData.upload_status
  set loaded_record = 0 /* TODO */
    , no_of_record = (select count(*) from NightHerondata.observation_fact)
  where upload_id = :upload_id;
