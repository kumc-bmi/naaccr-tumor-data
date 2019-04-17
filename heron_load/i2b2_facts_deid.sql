/** i2b2_facts_deid.sql -- de-identify i2b2 observation facts

Optimization Reference:
1. http://www.dba-oracle.com/t_insert_tuning.htm
    a. Drop and rebuild indexes after insert.
    b. Make sure to define multiple freelist (or freelist groups) to remove contention for the table header. 
       Multiple freelists add additional segment header blocks, removing the bottleneck. SEGMENT SPACE MANAGEMENT AUTO should work as well at tablespace level.
    c. Use PARALLEL and APPEND hint. For this INSERT optimization, make sure to define multiple freelists and use the SQL "APPEND" option.
    d. Avoid multiple parallel jobs on single table at the same time.
    e. Use large block size.
    f. no logging
2. http://dba-oracle.com/t_nologging_append.htm
    a. If database in archivelog mode, the table must be altered to nologging mode AND the SQL must be using the APPEND 
    b. The only danger with using nologging for index is that you must re-run the create index syntax if you perform a roll-forward database recovery.  
       Using nologging with create index can speed index creation by up to 30%.
    c. However, you must be aware that a roll-forward through this operation is not possible,
       since there are no images in the archived redo logs for this operation. 
3. https://stackoverflow.com/questions/34073532/what-is-the-purpose-of-logging-nologging-option-in-oracle

    Our databases are on noarchive log mode.

    Table Mode    Insert Mode     ArchiveLog mode      result
    -----------   -------------   -----------------    ----------
    LOGGING       APPEND          ARCHIVE LOG          redo generated
    NOLOGGING     APPEND          ARCHIVE LOG          no redo
    LOGGING       no append       ARCHIVE LOG          redo generated
    NOLOGGING     no append       ARCHIVE LOG          redo generated
    LOGGING       APPEND          noarchive log mode   no redo
    NOLOGGING     APPEND          noarchive log mode   no redo
    LOGGING       no append       noarchive log mode   redo generated
    NOLOGGING     no append       noarchive log mode   redo generated
4. https://samadhandba.wordpress.com/2011/02/14/oracle-insert-performance-2/
5.http://www.dba-oracle.com/art_dbazine_ts_mgt.htm
    a. Cons of ASSM
          a. Slow for full-table scans
          a. Slower for high-volume concurrent INSERTS

tested in nheronB2:   
logging,    normal insert, rollback possible before commit , select possible before and after commit
logging,    append insert, rollback possible before commit , select possible after commit
nologging,  normal insert, rollback possible before commit , select possible before and after commit
nologging,  append insert, rollback possible before commit , select possible after commit




Copyright (c) 2012 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

Note the use of valtype_cd in de-identification:
 * _i - only for identified DB
 * _d - only for de-identified DB
 * _ - for both DBs

per :upload_id , per :part.
 
*/

alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI';

/* Check for id repository, uploader service tables, blueheron link */
select * from NightHeronData.observation_fact where 1 = 0;
select * from NightHeronData.upload_status where 1 = 0;
select concept_cd from BlueHeronData.observation_fact@deid where 1 = 0;

-- Starting with 4.
 
alter table NightHeronData.observation_fact nologging;
insert /*+ parallel(4) */ into NightHeronData.observation_fact (
    encounter_num, sub_encounter, patient_num, concept_cd, provider_id, start_date
  , modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, units_cd
  , end_date, location_cd,observation_blob, confidence_num
  , update_date, download_date, import_date, sourcesystem_cd, upload_id
)
select /*+ parallel(4) */ 
    f.encounter_num, f.sub_encounter, f.patient_num
  , f.concept_cd
  , f.provider_id
  , f.start_date
  , f.modifier_cd, f.instance_num
  , substr(f.valtype_cd, 1, 1) valtype_cd
  , f.tval_char
  , f.nval_num, f.valueflag_cd, f.units_cd
  , f.end_date
  , f.location_cd,f.observation_blob, f.confidence_num
  , f.update_date, f.download_date
  , SYSDATE as import_date
  , f.sourcesystem_cd, f.upload_id
from observation_fact_upload f
where
  /* 'd' as the second character in the valtype_cd indicates that the row should 
  only be inserted into the de-identified fact table (BlueHeron) not the identified 
  fact table (NightHeron).
  */
  (f.valtype_cd not like '_d' or f.valtype_cd is null)
  and
  mod(f.encounter_num, &&heron_etl_chunks)+1 = :part
;


/* Make a de-id upload_status record, copying some fields from id to de-id. */
commit;
delete from BlueHeronData.upload_status@deid
where upload_id=:upload_id;
insert into BlueHeronData.upload_status@deid (
  upload_id
, upload_label
, user_id
, source_cd
, load_date
, message
, transform_name)
select upload_id
     , upload_label
     -- I guess this doesn't work across links;
     -- it raises ORA-02070
     -- , (select to_char(username) from user_users) as user_id
     -- so we'll just assume deid user = ETL user
     , user_id
     , source_cd
     , sysdate as load_date
     , message
     , transform_name
from NightHeronData.upload_status up
where up.upload_id=:upload_id;
commit;


-- instance_num has 18 significant digits which can store up to 7 bytes
create or replace view byte_conv as
select num_bytes, rpad('x', num_bytes * 2, 'x') as xfill
  from (select 7 as num_bytes from dual);


whenever sqlerror continue;
truncate table observation_fact_transfer;
drop table observation_fact_transfer;
whenever sqlerror exit;

/* Shift start/end dates, dates in values 
   Note: in order to do the transform and leverage the `to_date_noex` function
   it is necessary to create a temporary table to store the results of the 
   transform before inserting them over the link because otherwise
   oracle raise an error due to `global_names` (ORA-02069)
 */
create table observation_fact_transfer nologging as
select * from observation_fact_upload where 1 = 0;

insert /*+  parallel(4) */into observation_fact_transfer obs(
    patient_num, encounter_num, sub_encounter, concept_cd, provider_id, start_date
  , modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, units_cd
  , end_date, location_cd, observation_blob, confidence_num
  , update_date, download_date, import_date, sourcesystem_cd, upload_id)
select /*+ parallel(4) */ f.patient_num, f.encounter_num, ora_hash(f.sub_encounter) sub_encounter
  , f.concept_cd
  , f.provider_id
  -- date-shift the observation
  , f.start_date + pdim.date_shift as start_date
  , f.modifier_cd
  , case 
    when f.instance_num is null then null
    else to_number(
          rawtohex(
            UTL_RAW.SUBSTR(
              DBMS_CRYPTO.Hash(UTL_RAW.CAST_FROM_NUMBER(f.instance_num),
                              /* typ 3 is HASH_SH1 */
                              3),
              1, (select num_bytes from byte_conv))),
            (select xfill from byte_conv))
    end as deid_instance_num
  , substr(f.valtype_cd, 1, 1) valtype_cd
  , case
      when valtype_cd in ('D', 'Dd')
      then case
        when to_date_noex(f.tval_char, 'yyyy-mm-dd hh24:mi:ss') is not null
        then to_char(to_date(f.tval_char,'yyyy-mm-dd hh24:mi:ss') + pdim.date_shift,
                     'yyyy-mm-dd hh24:mi:ss')
        else null end
      else f.tval_char
    end as tval_char
  , f.nval_num, f.valueflag_cd, f.units_cd
  , f.end_date + pdim.date_shift as end_date
  , f.location_cd, f.observation_blob, f.confidence_num
  , f.update_date + pdim.date_shift as update_date
  , f.download_date
  , SYSDATE as import_date
  , f.sourcesystem_cd, f.upload_id
from observation_fact_upload f
  join NightHeronData.patient_dimension pdim
    on pdim.patient_num = f.patient_num
where
  /* 'i' as the second character in the valtype_cd indicates that the row should 
  only be inserted into the identified fact table (NightHeron) not the de-identified 
  fact table (BlueHeron).
  */
  (f.valtype_cd not like '_i' or f.valtype_cd is null)
  and
  mod(f.encounter_num, &&heron_etl_chunks)+1 = :part
;

insert /*+  parallel(4) */ into BlueHeronData.observation_fact@deid (
    patient_num, encounter_num, sub_encounter, concept_cd, provider_id, start_date
  , modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, units_cd
  , end_date, location_cd, observation_blob, confidence_num
  , update_date, download_date, import_date, sourcesystem_cd, upload_id
)
select /*+ parallel(4) */ patient_num, encounter_num, sub_encounter, concept_cd, provider_id, start_date
  , modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, units_cd
  , end_date, location_cd, observation_blob, confidence_num
  , update_date, download_date, import_date, sourcesystem_cd, upload_id 
from observation_fact_transfer;


/* Record end date.
 */
update BlueHeronData.upload_status@deid
set end_date = sysdate, load_status='OK'
where upload_id=:upload_id;
