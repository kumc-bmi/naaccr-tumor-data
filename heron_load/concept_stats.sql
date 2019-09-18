select c_hlevel from ontology where 'dep' = 'naaccr_ontology';
/* IDEA: parameterize patientIdNumber column for use with other sorts of observations. */
select patientIdNumber from observations where 'dep' = 'naaccr_observations';


create or replace temporary view concept_stats as
with

w_hash as (
select hash(c_fullname) term_id, ont.c_fullname, ont.c_basecode, ont.c_name, ont.c_hlevel
from ontology ont
),

/* ISSUE: check for hash collisions? or at least compute probability? */

has_code as (
select *
from w_hash
where c_basecode is not null
),

closure as (
select distinct ancestor.term_id hi_id, descendant.c_basecode
from w_hash ancestor
join has_code descendant on descendant.c_hlevel > ancestor.c_hlevel
and substring(descendant.c_fullname, 1, length(ancestor.c_fullname)) = ancestor.c_fullname
),

agg as (
select hi_id, count(*) facts, count(distinct obs.patientIdNumber) patients, count(distinct obs.concept_cd) concepts
from closure
join observations obs on obs.concept_cd = closure.c_basecode
group by hi_id
)

select w_hash.*, agg.patients, agg.facts, agg.concepts
from w_hash
join agg on w_hash.term_id = agg.hi_id
;

