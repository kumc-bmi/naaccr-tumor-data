/** naaccr_load.sql -- load i2b2 concepts from NAACCR tumor registry data

Copyright (c) 2013 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

 * ack: "Key, Dustin" <key.d@ghc.org>
 * Thu, 18 Aug 2011 16:16:31 -0700
 *
 * see also: naacr_init.sql, naacr_txform.sql
 */

/* check that the LOINC answers, scraped chapters, and R labels loaded */
select answer_code from loinc_naaccr_answers where dep = 'loinc_naaccr_answers.csv';
select 1 from data_descriptor where 1 = 0;
select 1 from record_layout where 1 = 0;
select 1 from section where 1 = 0;
select label from code_labels where dep = 'code-labels';

/* oh for bind parameters... */
select task_id from current_task where 1=0;

/* Check that we're running in the identified repository. */
select * from NightHeronData.observation_fact where 1=0;

/* Check for NAACCR extract table (in KUMC database).
oops... typo in schema name. keep it that way?
*/
select * from naacr.extract where 1=0;

/* check that transformation views are in place */
select valtype_cd from tumor_item_type where dep = 'naaccr_txform.sql';
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


/** tumor_reg_codes - one row for each distinct concept_cd in the data
 *  - NAACCR|III:CCC concept codes: one row per code value per coded item
 *  - NAACCR|NNN: concept codes: one per non-coded (numeric, date, ...) item
 */

whenever sqlerror continue;
drop table tumor_reg_codes;
whenever sqlerror exit;
create table tumor_reg_codes as
-- Note: this includes both coded and other (date, numeric) items
select distinct
  tiv.sectionid, tiv.section
, tiv.itemid, tiv.itemnbr, tiv.itemname
, tiv.concept_cd, tiv.codenbr
from tumor_item_value tiv;

-- select * from tumor_reg_codes;
-- select count(*) from tumor_reg_codes;

/** tumor_reg_concepts -- one row per code from data or data dictionary
 *
 * A left join from the data dictionary to the data would leave out
 * codes that appear only in the data.
 *
 * A left join from the data to the data dictionary would leave out
 * codes that appear only in the data dictionary.
 *
 * So we take the union of these, left join it with the data dictionary,
 * and for c_visualattributes, check whether any such data exist.
 */
whenever sqlerror continue;
drop table tumor_reg_concepts;
whenever sqlerror exit;
create table tumor_reg_concepts as
select coded.sectionid, coded.section, coded.itemnbr, coded.itemname
     , concept_cd
     , case
       -- concepts where we have data are Active
       when exists (
         select 1
         from tumor_reg_codes trc
         where trc.itemnbr = coded.itemnbr
         and trc.codenbr = tc.codenbr) then 'LA'
       else 'LH'
       end c_visualattributes
     , tc.codenbr, coalesce(label.c_name, tc.codenbr) c_name
from
-- For each *coded* item from the data dictionary...
(
  select sectionid, section, itemnbr, itemname
  from tumor_item_type
  where valtype_cd = '@'
) coded
-- ... find all the code values...
join (
  -- ... from the data ...
  select distinct itemnbr, codenbr, concept_cd
  from tumor_reg_codes
  where codenbr is not null

  union

  -- ... as well as those from the data dictionary (t_code) ...
  select distinct to_number(ti."ItemNbr"), codenbr
                , 'NAACCR|' || ti."ItemNbr" || ':' || codenbr concept_cd
  from naacr.t_code tc
  join naacr.t_item ti on tc.itemid = ti."ItemID"
  where tc.codedcrp is not null
  -- exclude description of codes; we just want codes
  and tc.codenbr not like '% %'
  and tc.codenbr not like '%<%'
  and tc.codenbr not in ('..', '*', 'User-defined', 'nn')
) tc on coded.itemnbr = tc.itemnbr
-- now get labels where available from t_code
left join (
  select ty.itemnbr, tc.codenbr, min(tc.codenbr || ' ' || tc.codedcrp) c_name
  from naacr.t_code tc
  join tumor_item_type ty on tc.itemid = ty.itemid
  group by ty.itemnbr, tc.codenbr
) label
  on label.itemnbr = coded.itemnbr
 and label.codenbr = tc.codenbr
;

/*
-- eyeball it:
-- select * from tumor_reg_concepts order by sectionid, itemnbr, codenbr;
-- select count(*) from tumor_reg_concepts;
-- 1849 (in test)
*/

create or replace temporary view naaccr_code_values as
select sectionId, section, naaccrNum, naaccrId
     , concat('NAACCR|', naaccrNum, ':',
              coalesce(rl.code, la.answer_code)) concept_cd
     , answer_code, rl.code
     , concat(coalesce(rl.code, la.answer_code), ' ',
              coalesce(rl.label, la.answer_string)) as name_char
     , answer_string
     , rl.label
     , rl.description c_tooltip
     , loinc_num, ty.AnswerListId, sequence_no
     , rl.scheme, rl.means_missing
from tumor_item_type ty
left join loinc_naaccr_answers la
       on la.code_value = ty.naaccrNum
      and la.answerlistid = ty.AnswerListId
      and answer_code is not null
left join code_labels rl
       on rl.item = ty.naaccrNum
      and (la.answerlistid is null or rl.code = la.answer_code)
where ty.valtype_cd = '@'
;


create or replace temporary view naaccr_ont_aux as

with root as (
select 1 as c_hlevel
     , '' as path
     , 'Cancer Cases (NAACCR Hierarchy)' as concept_name
     , null as concept_cd
     , 'FA' as c_visualattributes
from (values('X'))
),

section_concepts as (
select 2 as c_hlevel
     , concat('S:', nts.sectionid, ' ', section, '\\') as path
     , concat(trim(format_string('%02d', nts.sectionid)), ' ', section) as concept_name
     , null as concept_cd
     , 'FA' as c_visualattributes
     , nts.section
from section nts
),

item_concepts as (
select 3 as c_hlevel
     , concat(sc.path,
       -- ISSUE: migrate from naaccrName to naaccrId for path?
              substr(concat(trim(format_string('%04d', ni.naaccrNum)), ' ', ni.naaccrName), 1, 40), '\\') as path
     , concat(trim(format_string('%04d', ni.naaccrNum)), ' ', ni.naaccrName) as concept_name
     , concat('NAACCR|', ni.naaccrNum, ':') as concept_cd
     , case
       when ni.valtype_cd = '@' then 'FA'
       else 'LA' -- TODO: hide concepts where we have no data
                 -- TODO: hide Histology since '0420', '0522'
                 -- we already have Morph--Type/Behav
       end as c_visualattributes
     , ni.naaccrNum
from tumor_item_type ni
join section_concepts sc on sc.section = ni.section
),

code_concepts as (
select distinct 4 as c_hlevel
     , concat(ic.path,
              substr(v.name_char, 1, 40), '\\')
       as path
     , v.name_char as concept_name
     , v.concept_cd
     , /* TODO: case
       when itemnbr in (
                    -- hide Histology since
                    -- we already have Morph--Type/Behav
                    '0420', '0522')
       then 'LH'
       else c_visualattributes
       end as*/ 'LA' as c_visualattributes
from naaccr_code_values v
join item_concepts ic on ic.naaccrNum = v.naaccrNum
)

-- TODO: where itemnbr not in (400, 419, 521) -- separate code for primary site, Morph.


/* Primary site concepts -- TODO
select distinct lvl + 1 as c_hlevel
     , 'S:' || sectionid || ' ' || section || '\'
       || substr(trim(to_char(itemnbr, '0999')) || ' ' || itemname, 1, 40) || '\'
       || icdo.path
       as concept_path
     , icdo.concept_name concept_name
     , 'NAACCR|400:' || icdo.concept_cd concept_cd
     , icdo.c_visualattributes
from icd_o_topo icdo, tumor_reg_concepts
where itemnbr  = 400
*/

/* Morph--Type/Behav concepts -- TODO
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
where tr.itemnbr in (419, 521)

union all
*/

/* SEER Site Summary concepts --TODO
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
*/
select c_hlevel, path, concept_name, concept_cd, c_visualattributes from root
union all
select c_hlevel, path, concept_name, concept_cd, c_visualattributes from section_concepts
union all
select c_hlevel, path, concept_name, concept_cd, c_visualattributes from item_concepts
union all
select c_hlevel, path, concept_name, concept_cd, c_visualattributes from code_concepts
;


create or replace temporary view naaccr_ontology as
select i2b2_root.c_hlevel + terms.c_hlevel as c_hlevel
     , concat(i2b2_root.c_fullname, naaccr_folder.path, terms.path) as c_fullname
     , substr(terms.concept_name, 1, 200) as c_name
     , terms.concept_cd as c_basecode
     , concat(i2b2_root.c_fullname, naaccr_folder.path, terms.path) as c_dimcode
     , (select task_id from current_task) as c_comment
     , c_visualattributes
     , norm.*
     , current_timestamp as update_date, current_timestamp as import_date -- @@ISSUE: ambient
     , tumor_reg_source.source_cd as sourcesystem_cd
from

naaccr_ont_aux terms
cross join (select 'naaccr\\' as path
     , 'NAACCR' as concept_name
     from (values('X'))) naaccr_folder
cross join (select 0 c_hlevel, '\\i2b2\\' c_fullname from (values('X'))) i2b2_root
cross join (
  select 'N' as c_synonym_cd
       , 'concept_cd' as c_facttablecolumn
       , 'concept_dimension' as c_tablename
       , 'CONCEPT_PATH' as c_columnname
       , 'T' c_columndatatype
       , 'like' c_operator
       , '@' m_applied_path
  from (values('X'))
) norm
cross join (select 'tumor_registry@kumed.com' as source_cd
   from (values('X'))) tumor_reg_source
;


/* Regression tests for earlier bugs. */
select case when count(*) = 4 then 1 else 1/0 end naaccr_morph_bugs_fixed
from (
select distinct c_basecode
from BlueHeronMetadata.NAACCR_ONTOLOGY@deid
where c_basecode in ('NAACCR|521:97323', 'NAACCR|521:80413',
                     'NAACCR|521:98353', 'NAACCR|400:C619')
);


insert into etl_test_values (test_domain, test_name, test_value, result_id, result_date, detail_num_1, detail_char_1)
select 'Cancer Cases' test_domain, 'item_terms_indep_data' test_name
     , case when ont.c_basecode is null then 0 else 1 end test_value
     , sq_result_id.nextval result_id
     , sysdate result_date
     , ti.itemnbr, ti.itemname
from (
select "ItemNbr" itemnbr, null codecrp, "ReqStatus"
     , 'NAACCR|' || "ItemNbr" || ':' c_basecode, "ItemName" itemname
from naacr.t_item) ti
left join (-- avoid link/LOB error ORA-22992
  select c_basecode, c_name
  from BlueHeronMetadata.NAACCR_ONTOLOGY@deid) ont
  on ont.c_basecode = ti.c_basecode
where ont.c_basecode is null
and ti."ReqStatus" != 'Retired'
;

insert into etl_test_values (test_domain, test_name, test_value, result_id, result_date, detail_num_1, detail_char_1, detail_char_2)
select 'Cancer Cases' test_domain, 'code_terms_indep_data' test_name
     , case when ont.c_basecode is null then 0 else 1 end test_value
     , sq_result_id.nextval result_id
     , sysdate result_date
     , ti."ItemNbr", ti.codenbr, substr(ti."ItemName" || ' / ' || ti.codedcrp, 1, 255)
from (
select "ItemNbr", "ItemName", "ReqStatus", "AllowValue", codenbr
     , 'NAACCR|' || "ItemNbr" || ':' || codenbr c_basecode, codedcrp
from naacr.t_code tc
join naacr.t_item ti on ti."ItemID" = tc.itemid
) ti
left join (-- avoid link/LOB error ORA-22992
  select c_basecode, c_name
  from BlueHeronMetadata.NAACCR_ONTOLOGY@deid) ont
  on ont.c_basecode = ti.c_basecode
where ont.c_basecode is null
and ti."ReqStatus" != 'Retired'

-- skip numeric values that are actually codes
and ti."AllowValue" != '10-digit number'
and ti."AllowValue" not like 'Census Tract Codes%'
and ti.codenbr not in ('00000000', '88888888', '99999999')

-- comments on/descriptions of codes
and ti.codenbr not like '<_>%'
and ti.codenbr not like '% %'
;

drop table icd_o_topo;
drop table icd_o_morph;
