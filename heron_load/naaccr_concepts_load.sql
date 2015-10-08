/** naaccr_load.sql -- load i2b2 concepts from NAACCR tumor registry data

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
select * from naacr.extract where 1=0;

/* check that transformation views are in place */
select * from tumor_item_value tiv where 1=0;

/* check that metadata_init.sql was run to create the ontology table. */
select c_name from BlueHeronMetadata.NAACCR_ONTOLOGY@deid where 1=0;

-- check that WHO materials are staged
select * from who.topo where 1=0;



/* Exploration/analysis queries ...

-- How many records did we load from the extract?
select count(*)
from naacr.extract ne;
-- 65584

-- How many distinct patients? How many tumors per patient?
select count(distinct ne."Patient ID Number") as total_patients,
round(count(*) / count(distinct ne."Patient ID Number"), 3) as tumors_per_patient
from naacr.extract ne;
-- 60667	1.081


-- Patient mapping: do all of them have Patient IDs?
select count(to_number(ne."Patient ID Number"))
  from NAACR.EXTRACT ne;
-- 65584, so yes.

-- How many of them match MRNs from our patient mapping?
select count(*)
from naacr.extract ne
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
from naacr.extract ne;
-- 8. hmm.

-- How many of them match after we drop the 1st digit?
select numerator, denominator, round(numerator/denominator*100, 2) as density
from (
  select count(*) as numerator
  from naacr.extract ne
  join NIGHTHERONDATA.patient_mapping pm
    on pm.patient_ide_source =
       (select source_cd from sms_audit_info)
   and pm.patient_ide = substr(ne."Patient ID Number", 2)) matches,
  (select count(*) as denominator from naacr.extract ne) 
;
-- 65183 out of 65584; i.e. 99.39% 


-- How many match if we convert digit-strings to numbers?
select count(*)
from naacr.extract ne
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
from naacr.extract ne);
-- 65581. almost; all but 4.

-- which 4?
select count(*), ne."Accession Number--Hosp", ne."Sequence Number--Hospital"
from naacr.extract ne
group by ne."Accession Number--Hosp", ne."Sequence Number--Hospital"
having count(*) > 1;

-- are there any nulls?
select count(*)
from naacr.extract ne
where ne."Accession Number--Hosp" is null;
-- 2
*/



/********
 * Concepts
 */

/* ICD-O topographic codes for primary site */
/* TODO: check that it's OK to throw away lvl='incl' synonyms */
whenever sqlerror continue;
drop table icd_o_topo;
whenever sqlerror exit;
create table icd_o_topo as
with major as (
  select * from who.topo
  where lvl = '3'
)
, minor as (
  select * from who.topo
  where lvl = '4'
)
select 3 lvl, major.kode concept_cd, 'FA' as c_visualattributes,
       major.kode || '\' path, major.title concept_name
from major
union all
select 4 lvl, replace(minor.kode, '.', '') concept_cd,  'LA' as c_visualattributes,
       major.kode || '\' || minor.kode || '\', minor.title
from major
join minor on minor.kode like (major.kode || '%')
;

/*
-- eyeball it
select * from icd_o_topo order by path;
*/

/* ICD-O-2, -3 morphology codes for histology */
whenever sqlerror continue;
drop table icd_o_morph;
whenever sqlerror exit;
create table icd_o_morph as
with item as (
  select '0419' itemnbr from dual -- ICD-O-2
union all
  select '0521' itemnbr from dual -- ICD-O-3
),
morph as (
  select replace(code, 'M', '') code, label, item.itemnbr
  from who.morph2 m, item
  where item.itemnbr = '0419'
union all
  select m.code, m.notes label, item.itemnbr
  from who.morph3 m, item
  where item.itemnbr = '0521'
  and m.label = 'title'
),
major as (
  select substr(code, 1, 3) lo,
         case
           when code like '%-%'
           then substr(code, 5, 3)
           else code
         end hi,
         morph.*
  -- only ICD-O-2 has hierarchy
  from morph where code not like '%/%'
union all
  -- 975 seems to be new in ICD-O-3
  select '975' lo, '975' hi, '975' code,
    'Neoplasms of histiocytes and accessory lymphoid cells' label,
    itemnbr
  from dual, item
),
minor as (
  select morph.*
  from morph where code like '%/%'
)
select 3 lvl, item.itemnbr, major.code concept_cd, 'FA' as c_visualattributes,
       major.code || '\' path, major.code || ' ' || major.label concept_name
from major, item

union all

select 4 lvl, minor.itemnbr, replace(minor.code, '/', '') concept_cd,  'LA' as c_visualattributes,
       major.code || '\' || minor.code || '\', minor.code || ' ' || minor.label

from major
join minor on substr(minor.code, 1, 3) between major.lo and major.hi
;

select case when count(*) > 0 then 1/0 else 1 end
  all_morph2_codes_joined from (
select *
from who.morph2
left join icd_o_morph
       on regexp_replace(code, '[M/]', '') = concept_cd
where code like '%/%' and concept_cd is null
)
;

select case when count(*) > 0 then 1/0 else 1 end
  all_morph3_codes_joined from (
select *
from who.morph3
left join icd_o_morph
       on regexp_replace(code, '[M/]', '') = concept_cd
where code like '%/%' and concept_cd is null
and label = 'title'
)
;


/*
-- eyeball it
select * from icd_o_morph order by path;
*/

/* For concept labels, we use
 *   1. CODEDCRP from the NAACCR t_code table, or if that is not available,
 *   2. c_name from Dustin Key's work, or if that is not available,
 *   3. the raw code number
 */

whenever sqlerror continue;
drop table tumor_reg_codes;
whenever sqlerror exit;
create table tumor_reg_codes as
select distinct
  tiv.sectionid, tiv.section
, tiv.itemid, tiv.itemnbr, tiv.itemname
, tiv.concept_cd, tiv.codenbr
from tumor_item_value tiv;

-- select * from tumor_reg_codes;
-- select count(*) from tumor_reg_codes;

whenever sqlerror continue;
drop table tumor_reg_concepts;
whenever sqlerror exit;
create table tumor_reg_concepts as
select sectionid, section
, itemid, itemnbr, itemname
, concept_cd, codenbr, min(codedcrp) as c_name
from (
select tiv.*
     , tiv.codenbr || ' ' || nc.codedcrp as codedcrp
from tumor_reg_codes tiv
left join naacr.t_code nc
  on nc.itemid = tiv.ItemID
 and nc.codenbr = tiv.codenbr
)
group by sectionid, section
, itemid, itemnbr, itemname
, concept_cd, codenbr;


-- select count(*) from tumor_reg_concepts;

delete from BlueHeronMetadata.NAACCR_ONTOLOGY@deid;

insert into BlueHeronMetadata.NAACCR_ONTOLOGY@deid (
  c_hlevel, c_fullname, c_name, c_basecode, c_dimcode, c_visualattributes,
  c_synonym_cd, c_facttablecolumn, c_tablename, c_columnname, c_columndatatype,
  c_operator, m_applied_path,
  update_date, import_date, sourcesystem_cd
)
select i2b2_root.c_hlevel + terms.c_hlevel as c_hlevel
     , i2b2_root.c_fullname || naaccr_folder.path || terms.path as c_fullname
     , terms.concept_name
     , terms.concept_cd
     , i2b2_root.c_fullname || naaccr_folder.path || terms.path as c_dimcode
     , c_visualattributes,
  norm.*,
  sysdate as update_date, sysdate as import_date,
  tumor_reg_source.source_cd as sourcesystem_cd
from
(
select 1 as c_hlevel
     , '' as path
     , 'Cancer Cases' as concept_name
     , null as concept_cd
     , 'FA' as c_visualattributes
from dual

union all
/* Section concepts */
select distinct 2 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\' as path
     , trim(to_char(sectionid, '09')) || ' ' || section as concept_name
     , null as concept_cd
     , 'FA' as c_visualattributes
from tumor_reg_concepts

union all
/* Item concepts */
select distinct 3 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\'
       || substr(trim(to_char(itemnbr, '0999')) || ' ' || itemname, 1, 40) || '\' as path
     , trim(to_char(itemnbr, '0999')) || ' ' || itemname as concept_name
     , 'NAACCR|' || itemnbr || ':' as concept_cd
     , case when codenbr is null then 'LA' else 'FA' end as c_visualattributes
from tumor_reg_concepts
where itemnbr not in (
                    -- skip Histology since
                    -- we already have Morph--Type&Behav
                    '0420', '0522')

union all
/* Code concepts */
select distinct 4 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\'
       || substr(trim(to_char(itemnbr, '0999')) || ' ' || itemname, 1, 40) || '\'
       || case when c_name is null then codenbr else substr(c_name, 1, 40) end || '\'
       as concept_path
     , case when c_name is not null then c_name
       else codenbr end as concept_name
     , concept_cd
     , 'LA' as c_visualattributes
from tumor_reg_concepts where codenbr is not null
and itemnbr not in ('0400', '0419', '0521',
                    -- skip Histology since
                    -- we already have Morph--Type&Behav
                    '0420', '0522')

union all

/* Primary site concepts */
select distinct lvl + 1 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\'
       || substr(trim(to_char(itemnbr, '0999')) || ' ' || itemname, 1, 40) || '\'
       || icdo.path
       as concept_path
     , icdo.concept_name concept_name
     , 'NAACCR|400:' || icdo.concept_cd concept_cd
     , icdo.c_visualattributes
from icd_o_topo icdo, tumor_reg_concepts
where itemnbr  = '0400'

/* Morph--Type&Behav concepts */
union all
select distinct lvl + 1 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\'
       || substr(trim(to_char(tr.itemnbr, '0999')) || ' ' || itemname, 1, 40) || '\'
       || icdo.path
       as concept_path
     , icdo.concept_name concept_name
     , substr(tr.concept_cd, 1, length('NAACCR|400:')) || icdo.concept_cd concept_cd
     , icdo.c_visualattributes
from icd_o_morph icdo, tumor_reg_concepts tr
where tr.itemnbr in ('0419', '0521')

union all
/* SEER Site Summary concepts*/
select 2 as c_hlevel
     , 'SEER Site\' as path
     , 'SEER Site Summary' as concept_name
     , null as concept_cd
     , 'FA' as c_visualattributes
from dual

union all

select 3 + hlevel as c_hlevel
     , 'SEER Site\' || path || '\' as path
     , name as concept_name
     , case when basecode is null then null
       else 'SEER_SITE:' || basecode end as concept_cd
     , visualattributes as c_visualattributes
from seer_site_terms@deid
) terms
, (select 'naaccr\' as path
     , 'NAACCR' as concept_name
     from dual) naaccr_folder
, (select 0 c_hlevel, '\i2b2\' c_fullname from dual) i2b2_root
, BlueHeronMetadata.normal_concept@deid norm
, (select * from BlueHeronData.source_master@deid
   where source_cd like 'tumor_registry@%') tumor_reg_source;


/* Regression tests for earlier bugs. */
select case when count(*) = 3 then 1 else 1/0 end naaccr_morph_bugs_fixed
from (
select distinct c_basecode
from BlueHeronMetadata.NAACCR_ONTOLOGY@deid
where c_basecode in ('NAACCR|521:97323', 'NAACCR|521:80413',
                     'NAACCR|521:98353')
);

insert into etl_test_values (test_domain, test_name, test_value, result_id, result_date, detail_num_1, detail_char_1, detail_char_2)
select 'Cancer Cases' test_domain, 'rx_summary_concepts' test_name
     , case when ont.c_basecode is null then 0 else 1 end test_value
     , sq_result_id.nextval result_id
     , sysdate result_date
     , rx.itemnbr, rx.itemname, ont.c_fullname
from
(
select 1640 itemnbr, 'RX Summ--Surgery Type' itemname from dual union all
select 1290,         'RX Summ--Surg Prim Site' from dual union all
select 1292,         'RX Summ--Scope Reg LN Sur' from dual union all
select 1294,         'RX Summ--Surg Oth Reg/Dis' from dual union all          
select 1296,         'RX Summ--Reg LN Examined' from dual union all
select 1330,         'RX Summ--Reconstruct 1st' from dual union all      
select 1340,         'Reason for No Surgery' from dual union all 
select 1360,         'RX Summ--Radiation' from dual union all
select 1370,         'RX Summ--Rad to CNS' from dual union all
select 1380,         'RX Summ--Surg/Rad Seq' from dual

union all  -- not related to RX, but has the same test structure
select 0230,         'Age at Diagnosis' from dual union all
select 0560,         'Sequence Number-Hospital' from dual
) rx
left join (select c_basecode, c_name, c_fullname
           from BlueHeronMetadata.NAACCR_ONTOLOGY@deid) ont
  on ont.c_basecode = 'NAACCR|' || rx.itemnbr || ':'
;

drop table icd_o_topo;
drop table icd_o_morph;
