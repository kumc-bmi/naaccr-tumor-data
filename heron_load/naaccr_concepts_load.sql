/** naaccr_concepts_load.sql -- build i2b2 concepts for NAACCR tumor registry
 *
 * Copyright (c) 2013-2019 University of Kansas Medical Center
 *
 * ack: "Key, Dustin" <key.d@ghc.org>
 * Thu, 18 Aug 2011 16:16:31 -0700
 *
 * see also: naacr_txform.sql
 */

/* check that static dependencies are available */
select c_hlevel, c_fullname, c_name, update_date, source_cd from naaccr_top where 'dep' = 'arbitrary';
select sectionId from section where 'dep' = 'section.csv';
select valtype_cd from tumor_item_type where 'dep' = 'tumor_item_type.csv';
select label from code_labels where 'dep' = 'code-labels';
select answer_code from loinc_naaccr_answers where 'dep' = 'loinc_naaccr_answers.csv';
select lvl from who_topo where dep='WHO Oncology MetaFiles';
select hlevel from seer_terms where 'dep' = 'seer_recode_terms.csv'

/* oh for bind parameters... */
select task_id from current_task where 1=0;

create or replace temporary view i2b2_path_concept as
select 'N' as c_synonym_cd
     , 'CONCEPT_CD' as c_facttablecolumn
     , 'CONCEPT_DIMENSION' as c_tablename
     , 'CONCEPT_PATH' as c_columnname
     , 'T' c_columndatatype
     , 'LIKE' c_operator
     , '@' m_applied_path
;


create or replace temporary view naaccr_top_concept as
select top.c_hlevel
     , top.c_fullname
     , top.c_name
     -- task_id can be too long; 'NAACCR' is pretty boring, so let's strip that
     , (select substr(task_id, length('NAACCR'), 50) from current_task) as c_basecode
     , 'CA' as c_visualattributes
     , concat('North American Association of Central Cancer Registries version 18.0',
              '\n ', (select task_id from current_task)) as c_tooltip
     , top.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     -- import_date
     , top.sourcesystem_cd
from naaccr_top top
cross join i2b2_path_concept i2b2
;

create or replace temporary view section_concepts as
with ea as (
select nts.sectionId, nts.section
     , top.c_hlevel + 1 c_hlevel
     , concat(top.c_fullname, 'S:', nts.sectionid, ' ', section, '\\') as c_fullname
     , concat(trim(format_string('%02d', nts.sectionid)), ' ', section) as c_name
     , null as c_basecode
     , 'FA' as c_visualattributes
     , cast(null as string) as c_tooltip
from section nts
cross join naaccr_top_concept top
)
select sectionId, section
     , ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     , top.sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2;


create or replace temporary view item_concepts as
with ea as (
select sc.sectionId, ni.naaccrNum
     , sc.c_hlevel + 1 as c_hlevel
     , concat(sc.c_fullname,
       -- ISSUE: migrate from naaccrName to naaccrId for path?
              substr(concat(trim(format_string('%04d', ni.naaccrNum)), ' ', ni.naaccrName), 1, 40), '\\') as c_fullname
     , concat(trim(format_string('%04d', ni.naaccrNum)), ' ', ni.naaccrName) as c_name
     , concat('NAACCR|', ni.naaccrNum, ':') as c_basecode
     , case
       when ni.valtype_cd = '@' then 'FA'
       else 'LA' -- TODO: hide concepts where we have no data
                 -- TODO: hide Histology since '0420', '0522'
                 -- we already have Morph--Type/Behav
       end as c_visualattributes
     , cast(null as string) as c_tooltip -- TODO
from tumor_item_type ni
join section_concepts sc on sc.section = ni.section
)
select sectionId, naaccrNum
     , ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     , top.sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2;


create or replace temporary view code_concepts as
with mix as (
select ty.naaccrNum
     , coalesce(rl.code, la.answer_code) as answer_code
     , coalesce(rl.label, la.answer_string) as answer_string
     , rl.description
     , loinc_num, ty.AnswerListId, sequence_no
     , rl.scheme, rl.means_missing
from (select * from tumor_item_type where valtype_cd = '@') ty
left join loinc_naaccr_answers la
       on la.code_value = ty.naaccrNum
      and la.answerlistid = ty.AnswerListId
      and answer_code is not null
left join code_labels rl
       on rl.item = ty.naaccrNum
      and (la.answerlistid is null or rl.code = la.answer_code)
),
with_name as (
select substr(concat(answer_code, ' ', answer_string), 1, 200) as c_name
     , mix.*
from mix
where answer_code is not null
),
ea as (
select ic.sectionId, ic.naaccrNum, answer_code
     , ic.c_hlevel + 1 c_hlevel
     , concat(ic.c_fullname,
              substr(v.c_name, 1, 40), '\\') as c_fullname
     , v.c_name
     , concat('NAACCR|', ic.naaccrNum, ':', answer_code) as c_basecode
     , 'LA' as c_visualattributes
     , description as c_tooltip
from with_name v
join item_concepts ic on ic.naaccrNum = v.naaccrNum
)
select ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     , top.sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2;
;


-- code_concepts where ic.naaccrNum not in (400, 419, 521) -- separate code for primary site, Morph. TODO: layer


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
/*
-- eyeball it
select * from icd_o_morph order by path;
*/

-- TODO: preserve morph2 tests
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


create or replace temporary view primary_site_concepts as
with ea as (
select lvl + 1 as c_hlevel
     , concat(ic.c_fullname, icdo.path) as c_fullname
     , icdo.concept_name as c_name
     , concat('NAACCR|400:', icdo.concept_cd) as c_basecode
     , icdo.c_visualattributes
     , cast(null as string) as c_tooltip
from icd_o_topo icdo
cross join (
  -- The DRY approach is somehow WAY slower:
  -- select * from item_concepts where naaccrNum = 400
  -- so let's try a hard-coded KLUDGE:
  select '\\i2b2\\naaccr\\S:1 Cancer Identification\\0400 Primary Site\\' as c_fullname
) ic
)
select ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     , top.sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2;


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
*/


create or replace temporary view seer_recode_concepts as
with

folder as (
select top.c_hlevel + 1 c_hlevel
     , concat(top.c_fullname, 'SEER Site\\') as c_fullname
     , 'SEER Site Summary' as c_name
     , null as c_basecode
     , 'FA' as c_visualattributes
     , 'SEER Site Recode ICD-O-3/WHO 2008 Definition' as c_tooltip
from (values('X')) f
cross join naaccr_top_concept top
),

site as (
select f.c_hlevel + s.hlevel as c_hlevel
     , concat(f.c_fullname, s.path, '\\') as c_fullname
     , name as c_name
     , case when basecode is null then null
       else concat('SEER_SITE:', basecode) end as concept_cd
     , visualattributes as c_visualattributes
     , cast(null as string) as c_tooltip
from seer_site_terms s
cross join folder f
),

ea as (
select * from folder
union all
select * from site
)
select ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     , top.sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2;
;


create or replace temporary view naaccr_ontology as

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from naaccr_top_concept

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from section_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from item_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from code_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from seer_recode_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
from primary_site_concepts
;

--TODO limit to 200     , substr(terms.concept_name, 1, 200) as c_name


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
