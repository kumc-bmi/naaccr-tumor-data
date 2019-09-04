/** i2b2_facts_deid.sql -- de-identify i2b2 observation facts

Copyright (c) 2012-2019 University of Kansas Medical Center

Note the use of valtype_cd in de-identification:
 * _i - only for identified DB
 * _d - only for de-identified DB
 * _ - for both DBs

*/

select start_date from observation_fact_&&upload_id where 'dep' = 'naaccr_observations';
select date_shift from NightHeronData.patient_dimension where 'dep' = 'epic_dimensions_load';


/* Shift start/end dates, dates in values 
 */
create table observation_fact_deid_&&upload_id compress as
select f.patient_num, f.encounter_num
  , f.concept_cd
  , f.provider_id
  -- date-shift the observation
  , f.start_date + pdim.date_shift as start_date
  , f.modifier_cd
  , 1 instance_num
  , substr(f.valtype_cd, 1, 1) valtype_cd
  , case
      when valtype_cd in ('D', 'Dd')
      then to_char(f.start_date + pdim.date_shift, 'yyyy-mm-dd hh24:mi:ss')
      else f.tval_char
    end as tval_char
  , f.nval_num, f.valueflag_cd, f.units_cd
  , f.end_date + pdim.date_shift as end_date
  , f.location_cd, f.observation_blob, f.confidence_num
  , f.update_date + pdim.date_shift as update_date
  , f.download_date
  , to_date(current_timestamp) as import_date
  , f.sourcesystem_cd, f.upload_id
from observation_fact_&&upload_id f
  join NightHeronData.patient_dimension pdim
    on pdim.patient_num = f.patient_num
where
  /* 'i' as the second character in the valtype_cd indicates that the row should 
  only be inserted into the identified fact table (NightHeron) not the de-identified 
  fact table (BlueHeron).
  */
  (f.valtype_cd not like '_i' or f.valtype_cd is null)
;

