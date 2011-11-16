
create index patient_id on naacr.extract (
  "Patient ID Number");
  
create index accession on naacr.extract (
  "Accession Number--Hosp", "Sequence Number--Hospital");

/* would be unique but for a handful of dups:
select * from
(
select count(*), "Accession Number--Hosp", "Sequence Number--Hospital"
from naacr.extract
group by "Accession Number--Hosp", "Sequence Number--Hospital"
having count(*) > 1
) dups
join naacr.extract ne
  on ne."Accession Number--Hosp" = dups."Accession Number--Hosp"
 and ne."Sequence Number--Hospital" = dups."Sequence Number--Hospital";
*/

/* Item names are unique, right? */
select case when count(*) = 0 then 1 else 0 end as test_item_name_uniqueness
from (
select count(*), "ItemName"
from naacr.t_item ni
group by "ItemName"
having count(*) > 1
);



/* Grade: how many of each kind? */
select count(*), ne."Grade", codedcrp
from naacr.extract@kumc ne
left join (
 select nc.codenbr, nc.codedcrp
 from naacr.t_code nc
 join naacr.t_item ni
   on nc.itemid = ni."ItemID" 
   where ni."ItemName" = 'Grade') nc
on ne."Grade" = nc.codenbr
group by ne."Grade", codedcrp
order by 1 desc
;
 

/**
 * Grade, Site, Histology
 * concept_cd follows ontology from Jack London
 *  <jack.london@KimmelCancerCenter.org> 8/18/2011 11:13 AM
 */
create or replace view tumor_grade_site_histology as
select * from (
select ne."Accession Number--Hosp"
     , ne."Sequence Number--Hospital"
     , 'TR|Grade:' || ne."Grade" as concept_cd
from naacr.extract ne

union all

select ne."Accession Number--Hosp"
     , ne."Sequence Number--Hospital"
     , 'TR|Site:' || ne."Primary Site" as concept_cd
from naacr.extract ne


union all
select ne."Accession Number--Hosp"
     , ne."Sequence Number--Hospital"
     , 'TR|S:' || ne."Primary Site" || '|H:' || ne."Morph--Type&Behav ICD-O-3" as concept_cd
from naacr.extract ne

/*
separate facts for , 'Histologic Type ICD-O-3'
, 'Behavior Code ICD-O-3'
?
There don't seem to be codes on t_code.
*/

)
where "Accession Number--Hosp" is not null
order by "Accession Number--Hosp", "Sequence Number--Hospital", concept_cd
;


/**
 * Treatments: in progress. @@TODO

select "Accession Number--Hosp"
     , "Sequence Number--Hospital"
     , ItemName
     , value as codenbr
     , '@' as tval_char
     , nc.codedcrp
from "NAACR"."EXTRACT_EAV" ne
join naacr.t_item ni on ne.ItemName = ni."ItemName"
left join naacr.t_code nc
  on nc.itemid = ni."ItemID"
 and nc.codenbr = ne.value
where ItemName in (
  'RX Hosp--Surg App 2010'
, 'RX Hosp--Surg Prim Site'
, 'RX Hosp--Scope Reg LN Sur'
, 'RX Hosp--Surg Oth Reg/Dis'
, 'RX Hosp--Reg LN Removed'
, 'RX Hosp--Radiation'
, 'RX Hosp--Chemo'
, 'RX Hosp--Hormone'
, 'RX Hosp--BRM'
, 'RX Hosp--Other'
, 'RX Hosp--DX/Stg Proc'
, 'RX Hosp--Palliative Proc'
--, 'RX Hosp--Surg Site 98-02'
--, 'RX Hosp--Scope Reg 98-02' -- @@ '0' code has no codedcrp
--, 'RX Hosp--Surg Oth 98-02' -- @@ '0' code has no codedcrp
)
and value is not null
order by 1, 2;

-- Codes for treatments.
select *
from naacr.t_item ni
join naacr.t_code nc
  on nc.itemid = ni."ItemID"
where ni."ItemName" like '%RX Hosp%';
*/


/**
 * Demographic, Administrative data
 */

/* Race analysis: How many of each?
 odd: no codecrp for 16, 17 */
select count(*), ne."Race 1", nc.codedcrp
from naacr.extract@kumc ne
join naacr.t_code nc
  on ne."Race 1" = nc.codenbr
join naacr.t_item ni
  on ni."ItemID" = nc.itemid
where ni."ItemName" = 'Race 1'
group by ne."Race 1", nc.codedcrp
order by 1 desc
;


create or replace view tumor_demo_admin as
select d."Accession Number--Hosp"
     , d."Sequence Number--Hospital"
     , d.value
     , d.ItemNbr
     , d.ItemName
     , nc.codedcrp
     , d.valtype_cd
     , d.tval_char
     , 'NAACCR|' || d.ItemNbr || ':' || d.codenbr as concept_cd
     , d.codenbr
from (
select "Accession Number--Hosp"
     , "Sequence Number--Hospital"
     , ItemNbr
     , ItemName
     , value
     , value as codenbr
     , '@' as valtype_cd
     , '@' as tval_char
from "NAACR"."EXTRACT_EAV"
where ItemName in (
  'Race 1', 'Sex'
, 'Laterality'
, 'Diagnostic Confirmation'
, 'Inpatient Status'
, 'Class of Case' -- @@ no concept name (codedcrp) for codenbr 10
, 'Primary Payer at DX'
)

union all

select "Accession Number--Hosp"
     , "Sequence Number--Hospital"
     , ItemNbr
     , ItemName
     , value
     , null as codenbr
     , 'D' as valtype_cd
     , case length(value)
       when 8 then substr(value, 1, 4) || '-' || substr(value, 5, 2) || '-' || substr(value, 7, 2)
       when 6 then substr(value, 1, 4) || '-' || substr(value, 5, 2)
       when 4 then value end as tval_char
from "NAACR"."EXTRACT_EAV" ne
where ItemName in (
  'Date of Birth'
, 'Date of 1st Contact'
, 'Date of Inpatient Adm'
, 'Date of Inpatient Disch'
  )

) d
join naacr.t_item ni on ni."ItemName" = d.itemname
left join naacr.t_code nc on nc.itemid = ni."ItemID" and nc.codenbr = d.codenbr
where d."Accession Number--Hosp" is not null
and value is not null
;

-- eyeball it:
-- select * from tumor_demo_admin order by "Accession Number--Hosp";

/* TODO: what's up with these uncoded items? esp. Class of Case
select distinct itemname, value
from tumor_demo_admin
where codedcrp is null
and valtype_cd = '@'
order by 1, 2;


select *
from naacr.t_item ni
where ni."ItemName" = 'Class of Case';

select ni."ItemName", nc.*
from naacr.t_item ni
join naacr.t_code nc on ni."ItemID" = nc.itemid
where ni."ItemName" = 'Class of Case';
*/


/**
 * i2b2 style visit info
 */
create or replace view tumor_reg_visits as
select ne."Accession Number--Hosp" || '-' || ne."Sequence Number--Hospital"
       as encounter_ide
     , ne."Patient ID Number" as MRN
from naacr.extract ne;

/**
 * i2b2 style facts
 */
create or replace view tumor_reg_facts as
select
  ne."Patient ID Number" as MRN,
  ne."Accession Number--Hosp" || '-' || ne."Sequence Number--Hospital" as encounter_ide,
  av.concept_cd,
  '@' provider_id,
  to_date(ne."Date of Diagnosis"
               || case length(ne."Date of Diagnosis")
                  when 8 then ''
                  when 6 then '01'
                  when 4 then '0101'
                  end, 'yyyymmdd') as start_date,
  '@' modifier_cd,
  av.valtype_cd,
  av.tval_char,
  to_number(null) as nval_num,
  null as valueflag_cd,
  null as units_cd,
  to_date(ne."Date of Diagnosis"
               || case length(ne."Date of Diagnosis")
                  when 8 then ''
                  when 6 then '28'
                  when 4 then '1231'
                  end, 'yyyymmdd') as end_date,
  '@' location_cd,
  to_date(null) as update_date
  /* to_date(ne."Date of Last Contact", 'yyyymmdd') as update_date
     gives: ORA-01843: not a valid month */
from naacr.extract ne
join (
select tgsh."Accession Number--Hosp"
     , tgsh."Sequence Number--Hospital"
     , tgsh.concept_cd
     , '@' as valtype_cd
     , '@' as tval_char
from tumor_grade_site_histology tgsh
union all
select tda."Accession Number--Hosp"
     , tda."Sequence Number--Hospital"
     , tda.concept_cd
     , tda.valtype_cd
     , tda.tval_char
from tumor_demo_admin tda

) av
 on ne."Accession Number--Hosp" = av."Accession Number--Hosp"
and ne."Sequence Number--Hospital" = av."Sequence Number--Hospital";

-- eyeball it:
-- select * from tumor_reg_facts order by mrn, encounter_ide;