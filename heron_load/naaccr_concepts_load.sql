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

select c_hlevel, c_fullname, c_name, update_date, sourcesystem_cd from naaccr_top where 'dep' = 'arbitrary';
select sectionId from section where 'dep' = 'section.csv';
select valtype_cd from tumor_item_type where 'dep' = 'tumor_item_type.csv';
select label from code_labels where 'dep' = 'code-labels';
select answer_code from loinc_naaccr_answer where 'dep' = 'loinc_naaccr_answer.csv';
select lvl from who_topo where dep='WHO Oncology MetaFiles';
select hlevel from seer_recode_terms where 'dep' = 'seer_recode_terms.csv';

/* oh for bind parameters in Spark SQL... */
select task_hash from current_task where 1=0;

drop view if exists i2b2_path_concept;
create view i2b2_path_concept as
select 'N' as c_synonym_cd
     , 'CONCEPT_CD' as c_facttablecolumn
     , 'CONCEPT_DIMENSION' as c_tablename
     , 'CONCEPT_PATH' as c_columnname
     , 'T' c_columndatatype
     , 'LIKE' c_operator
     , cast(null as string) as c_comment
     , '@' m_applied_path
     , '@' m_exclusion_cd
     , cast(null as int) as c_totalnum
     , cast(null as string) as valuetype_cd
     , cast(null as string) as c_metadataxml
;


drop view if exists naaccr_top_concept;
create view naaccr_top_concept as
select top.c_hlevel
     , top.c_fullname
     , top.c_name
     , (select task_hash from current_task) as c_basecode
     , 'CA' as c_visualattributes
     , ('North American Association of Central Cancer Registries version 18.0' ||
              '\n ' || (select task_hash from current_task)) as c_tooltip
     , top.c_fullname as c_dimcode
     , i2b2.*
     , top.update_date
     -- import_date
     , top.sourcesystem_cd
from naaccr_top top
cross join i2b2_path_concept i2b2
;
-- select * from naaccr_top_concept;

drop view if exists section_concepts;
create view section_concepts as
with ea as (
select nts.sectionId, nts.section
     , top.c_hlevel + 1 c_hlevel
     , top.c_fullname || 'S:' || nts.sectionid || ' ' || section || '\' as c_fullname
     , trim(printf('%02d', nts.sectionid)) || ' ' || section as c_name
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
-- select * from section_concepts limit 10;


drop view if exists item_concepts;
create view item_concepts as
with ea as (
select sc.sectionId, ni.naaccrNum
     , sc.c_hlevel + 1 as c_hlevel
     , (sc.c_fullname ||
       -- ISSUE: migrate from naaccrName to naaccrId for path?
              substr((trim(printf('%04d', ni.naaccrNum)) || ' ' || ni.naaccrName), 1, 40) || '\') as c_fullname
     , (trim(printf('%04d', ni.naaccrNum)) || ' ' || ni.naaccrName) as c_name
     , ('NAACCR|' || ni.naaccrNum || ':') as c_basecode
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
-- select * from item_concepts;


drop view if exists code_concepts;
create view code_concepts as
with mix as (
select ty.naaccrNum
     , coalesce(rl.code, la.answer_code) as answer_code
     , coalesce(rl.label, la.answer_string) as answer_string
     , rl.description
     , loinc_num, ty.AnswerListId, sequence_no
     , rl.scheme, rl.means_missing
from (select * from tumor_item_type where valtype_cd = '@') ty
left join loinc_naaccr_answer la
       on la.code_value = ty.naaccrNum
      and la.answerlistid = ty.AnswerListId
      and answer_code is not null
left join code_labels rl
       on rl.item = ty.naaccrNum
      and (la.answerlistid is null or rl.code = la.answer_code)
),
with_name as (
select substr((answer_code || ' ' || answer_string), 1, 200) as c_name
     , mix.*
from mix
where answer_code is not null
),
ea as (
select ic.sectionId, ic.naaccrNum, answer_code
     , ic.c_hlevel + 1 c_hlevel
     , (ic.c_fullname ||
              substr(v.c_name, 1, 40) || '\') as c_fullname
     , v.c_name
     , ('NAACCR|' || ic.naaccrNum || ':' || answer_code) as c_basecode
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
-- select * from code_concepts;

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


drop view if exists primary_site_concepts;
create view primary_site_concepts as
with ea as (
select lvl + 1 as c_hlevel
     , (ic.c_fullname || icdo.path) as c_fullname
     , icdo.concept_name as c_name
     , ('NAACCR|400:' || icdo.concept_cd) as c_basecode
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


create view seer_recode_concepts as
with

folder as (
select top.c_hlevel + 1 c_hlevel
     , (top.c_fullname || 'SEER Site\') as c_fullname
     , 'SEER Site Summary' as c_name
     , null as c_basecode
     , 'FA' as c_visualattributes
     , 'SEER Site Recode ICD-O-3/WHO 2008 Definition' as c_tooltip
from (values('X')) f
cross join naaccr_top_concept top
),

site as (
select f.c_hlevel + s.hlevel + 1 as c_hlevel
     , (f.c_fullname || s.path || '\') as c_fullname
     , name as c_name
     , case when basecode is null then null
       else ('SEER_SITE:' || basecode) end as concept_cd
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

create view site_schema_concepts as
with
ea as (
select c_hlevel, c_fullname, c_name, c_basecode, c_visualattributes, c_tooltip from cs_terms
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


create view naaccr_ontology as

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from naaccr_top_concept

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from section_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from item_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from code_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from seer_recode_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from primary_site_concepts

union all

select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_totalnum, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, c_comment, c_tooltip, m_applied_path, update_date, /*import_date,*/ sourcesystem_cd
     , valuetype_cd, c_metadataxml, m_exclusion_cd
from site_schema_concepts
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

drop view if exists pcornet_tumor_fields;
create view pcornet_tumor_fields as
select 'TUMOR' as TABLE_NAME
     , ty.naaccrNum as item
     , upper_snake_case(naaccrId) as FIELD_NAME
     , 'RDBMS ' || (case valtype_cd
        when 'N' then 'Number'
        when '@' then 'Text'
        when 'T' then 'Text'
        when 'D' then 'Date' end) ||
        (case when valtype_cd == 'D' then '' else '(' || length || ')' end)
        as RDBMS_DATA_TYPE
     , 'SAS ' || (case valtype_cd
        when 'N' then 'Numeric'
        when '@' then 'Char'
        when 'T' then 'Char'
        when 'D' then 'Date' end) ||
        (case when valtype_cd == 'D' then ' (Numeric)' else '(' || length || ')' end)
        as SAS_DATA_TYPE
     , 'LOINC scale ' || scale_typ as DATA_FORMAT
     , 'NO' as REPLICATED_FIELD
     , case when valtype_cd = 'D' then 'DATE' else null end as UNIT_OF_MEASURE
     , VALUESET
     , VALUESET_DESCRIPTOR
     , ch10.description as FIELD_DEFINITION
     , row_number() over (order by ty.naaccrNum) FIELD_ORDER
from tumor_item_type ty
left join ch10 on ch10.naaccrNum = ty.naaccrNum
left join (
  select naaccrNum
       , group_concat('' || code, ';') as VALUESET
       , group_concat('' || code || '=' || desc, ';') as VALUESET_DESCRIPTOR
  from item_codes
  where code is not null
  group by naaccrNum
) vs on vs.naaccrNum = ty.naaccrNum
where valtype_cd in ('N', '@', 'T', 'D')
order by ty.naaccrNum
;
