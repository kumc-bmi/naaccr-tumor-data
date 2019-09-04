/** i2b2_facts_deid.sql -- de-identify i2b2 observation facts

Copyright (c) 2012 University of Kansas Medical Center

Note the use of valtype_cd in de-identification:
 * _i - only for identified DB
 * _d - only for de-identified DB
 * _ - for both DBs

*/



/* Shift start/end dates, dates in values 
 */
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

