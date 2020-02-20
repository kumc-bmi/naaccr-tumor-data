select * from naaccr_export_stats where 'dep' = 'NAACCR_Summary';
select c_basecode from naaccr_ontology where 'dep' = 'NAACCR_Ontology1';

whenever sqlerror continue;
drop table naaccr_ontology_unpub;
whenever sqlerror exit;

create table naaccr_ontology_unpub compress as select * from naaccr_ontology where 1 = 0;

/** un-published code values

i.e. code values that occur in tumor registry data but not in published ontologies
*/
insert /*+ append */ into naaccr_ontology_unpub (
       c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, m_applied_path, update_date, sourcesystem_cd, m_exclusion_cd
)
with item_concepts as (
select *
from naaccr_ontology
where c_basecode like 'NAACCR|%:'
),
naaccr_top as (
select c_hlevel, c_fullname, c_name, update_date, sourcesystem_cd from naaccr_ontology where c_fullname = '\i2b2\naaccr\'
),
i2b2_path_concept as (
select c_synonym_cd
     , c_facttablecolumn
     , c_tablename
     , c_columnname
     , c_columndatatype
     , c_operator
     , m_applied_path
     from naaccr_ontology where c_fullname = '\i2b2\naaccr\'
),
published_values as (
select *
from naaccr_ontology
where c_basecode like 'NAACCR|%:_%'
)
, export_values as (
select distinct naaccrNum, value
     , concat(concat('NAACCR|', naaccrNum), ':') as item_basecode
     , concat(concat(concat('NAACCR|', naaccrNum), ':'), value) as c_basecode
from naaccr_export_stats
where valtype_cd = '@'
)
, unpub as (
select v.*
from export_values v
left join published_values pv on pv.c_basecode = v.c_basecode
where pv.c_basecode is null
),
ea as (
select ic.c_hlevel + 1 as c_hlevel
     , concat(concat(ic.c_fullname, value), '\') as c_fullname
     , value as c_name
     , v.c_basecode
     , 'LA' as c_visualattributes
from unpub v
join item_concepts ic on ic.c_basecode = v.item_basecode
),
all_cols as (
select ea.*
     , ea.c_fullname as c_dimcode
     , i2b2.*
     , :update_date update_date
     , :source_cd sourcesystem_cd
from ea
cross join naaccr_top top
cross join i2b2_path_concept i2b2
)
select c_hlevel, c_fullname, c_name, c_synonym_cd, c_visualattributes, c_basecode
     , c_facttablecolumn, c_tablename, c_columnname, c_columndatatype, c_operator
     , c_dimcode, m_applied_path
     , cast(update_date as date) update_date
     , sourcesystem_cd, '@' m_exclusion_cd
from all_cols
;
commit;

-- eyeball it:
-- select * from naaccr_ontology_unpub;

select 1 as complete
from naaccr_ontology_unpub
where sourcesystem_cd = :source_cd
and rownum <= 1
;
