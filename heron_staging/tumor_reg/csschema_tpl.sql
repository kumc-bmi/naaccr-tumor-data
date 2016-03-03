/** csschema.sql -- i2b2 facts for NAACCR site-specific factors using CS Schema

Copyright (c) 2016 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

Largely derived from:

  Collaborative Staging System Schema
  American Joint Committee on Cancer (AJCC)
  https://cancerstaging.org/cstage/schema/Pages/version0205.aspx
  https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip

by way of csterms.py

  http://informatics.kumc.edu/work/browser/tumor_reg/csterms.py
 */

-- test that we're in the KUMC sid with the NAACCR data
-- note mis-spelling of schema name: naacr
select "Accession Number--Hosp" from naacr.extract where 1=0;

set define off;


create or replace view tumor_cs_schema as
  select MRN
       , ne.case_index
       , start_date
       , ne.primary_site, ne.histology
       , case
&&CASES
         end schemaid
from
 (select ne."Patient ID Number" as MRN
       , ne.case_index
       , ne."Primary Site" as primary_site
       , substr(ne."Morph--Type&Behav ICD-O-3", 1, 4) histology
       -- TODO: push start_date into tumor_item_value
       , to_date(case length(ne."Date of Diagnosis")
               when 8 then ne."Date of Diagnosis"
               when 6 then ne."Date of Diagnosis" || '01'
               when 4 then ne."Date of Diagnosis" || '0101'
               end, 'yyyymmdd') as start_date
  from naacr.extract ne
  where ne."Date of Diagnosis" is not null
    and ne."Accession Number--Hosp" is not null) ne
;

create or replace view cs_site_factor_facts as

with ssf_item as (
  select min(itemnbr) lo, max(itemnbr) hi
  from tumor_item_type
  where itemname like 'CS Site-Specific Factor%'
)

select cs.mrn
     , cs.case_index
     , 'CS|' || cs.schemaid || '|' ||
       trim(substr(ssf.itemname, length('CS Site-Specific Factor '))) ||
       ':' || ssf.codenbr concept_cd
     , '@' item_name
     , '@' provider_id
     , cs.start_date
     , '@' modifier_cd  -- hmm... modifier for synthesized info?
     , 1 instance_num
     , '@' valtype_cd   -- TODO: numeric values, e.g. for Breast SSF3 Number of ... Nodes
     , null tval_char
     , to_number(null) nval_num
     , null as valueflag_cd
     , null as units_cd
     , cs.start_date as end_date
     , '@' location_cd
     , to_date(null) as update_date
from tumor_item_value ssf
join tumor_cs_schema cs on cs.case_index = ssf.case_index
join ssf_item on ssf.itemnbr between ssf_item.lo and ssf_item.hi
where cs.schemaid is not null
;


/* Eyeball it:

select *
from tumor_cs_schema
;

select *
from cs_site_factor_facts
;

*/
