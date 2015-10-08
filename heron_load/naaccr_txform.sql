/** naaccr_txform -- transform NAACCR data to fit i2b2 star schema

Copyright (c) 2012-2015 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

*/

-- test that we're in the KUMC sid with the NAACCR data
-- note mis-spelling of schema name: naacr
select "Accession Number--Hosp" from naacr.extract where 1=0;
select "Accession Number--Hosp" from naacr.extract_eav where 1=0;
-- check for curated data
select name from seer_site_terms@deid where 1=0;

-- check for metadata tables
select * from naacr.t_item where 1=0;

alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI';

whenever sqlerror continue;
--Inside "continue" clause in case index is not there
drop index naacr.patient_id_idx;
drop index naacr.accession_idx;
whenever sqlerror exit;

/* There are some known duplicates - 3 rows.  The unique constraint on the index
below should cause sqlldr to dump the duplicate rows into the .bad file 
(assuming the index was still there durin the staging).  So, at this point,
we shouldn't have any duplicates.  But if we do, we have the re-creation of the
index here that will fail if there are duplicates.

As per #1155, we had 30 copies of a subset of the data.  So, also, make sure we 
have at least as many rows as we do today.
*/
create index naacr.patient_id_idx on naacr.extract (
  "Patient ID Number");

--Will fail if duplicates found
create unique index naacr.accession_idx on naacr.extract (
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


/* Race analysis/sanity check: How many of each?
 odd: no codecrp for 16, 17 */
select count(*), ne."Race 1", nc.codedcrp
from naacr.extract ne
join naacr.t_code nc
  on ne."Race 1" = nc.codenbr
join naacr.t_item ni
  on ni."ItemID" = nc.itemid
where ni."ItemName" = 'Race 1'
group by ne."Race 1", nc.codedcrp
order by 1 desc
;


/* Grade: how many of each kind? */
select count(*), ne."Grade", codedcrp
from naacr.extract ne
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


/********
 * "the big flat approach"

TODO: ISSUE: There are lots of codes for lack of information, e.g.
  - Grade/differentiation unknown, not stated, or not applicable
  - No further race documented
  - Unknown whether Spanish or not
  - Insurance status unknown
Do we want to record these as facts?

-- tricky: Cause of Death. ICD7-10 codes.

 */


/* Review sections based on ItemName, CodedCRP
select ns.sectionid, ns.section
     , ni."ItemNbr" as ItemNbr, ni."ItemName"
     , nc.codenbr, nc.codedcrp
     , ni."AllowValue", ni."Format"
from naacr.t_item ni
join NAACR.t_section ns on ns.sectionid = ni."SectionID"
left join naacr.t_code nc on nc.itemid = ni."ItemID"
where ni."SectionID" in (
   -- This list was built a la:
   -- select ', ' || sectionid || ' -- ' || section
   -- from naacr.t_section;
  1 -- Cancer Identification
, 2 -- Demographic
-- , 3 -- Edit Overrides/Conversion History/System Admin
, 4 -- Follow-up/Recurrence/Death
-- , 5 -- Hospital-Confidential
, 6 -- Hospital-Specific
-- , 7 -- Other-Confidential
-- , 8 -- Patient-Confidential
-- , 9 -- Record ID
-- , 10 -- Special Use
, 11 -- Stage/Prognostic Factors
-- , 12 -- Text-Diagnosis
-- , 13 -- Text-Miscellaneous
-- , 14 -- Text-Treatment
-- , 15 -- Treatment-1st Course
, 16 -- Treatment-Subsequent & Other
, 17 -- Pathology
)
and length(nc.codenbr) < 8  -- exclude case where description of code is given rather than a code
and nc.codenbr <> 'Blank'
order by ns.sectionid, to_number(ni."ItemNbr"), nc.codenbr
;
*/


/**
 * Review distinct attributes, values; eliminate PHI.
 * TODO: store these in the ID repository and de-id later
 * -- TODO: numeric values for section 11 -- Stage/Prognostic Factors
 * -- Tumor Size: what the heck do the values mean???
 * -- select * from naacr.t_item ni where ni."ItemNbr" = '780';
 * -- Regional Nodes Positive
 *    seems to be a mixture of numeric and coded (90-99) data. ugh.

select distinct ns.SectionID, ns.section, to_number(ne.ItemNbr), ne.ItemName
     , ne.value, nc.codedcrp
from NAACR.extract_eav ne
join naacr.t_item ni on ne.ItemNbr = ni."ItemNbr"
join NAACR.t_section ns on ns.sectionid = to_number(ni."SectionID")
left join naacr.t_code nc
  on nc.itemid = ni."ItemID"
 and nc.codenbr = ne.value
where ne.value is not null
-- and ni."Format" != 'YYYYMMDD'
and ns.SectionID in (
  1 -- Cancer Identification
 , 2 -- Demographic
-- , 3 -- Edit Overrides/Conversion History/System Admin
 , 4 -- Follow-up/Recurrence/Death
-- , 5 -- Hospital-Confidential
 , 6 -- Hospital-Specific
-- , 7 -- Other-Confidential
-- , 8 -- Patient-Confidential
-- , 9 -- Record ID
-- , 10 -- Special Use
  11 -- Stage/Prognostic Factors -- TODO: numeric stuff
-- , 12 -- Text-Diagnosis
-- , 13 -- Text-Miscellaneous
-- , 14 -- Text-Treatment
-- , 15 -- Treatment-1st Course
, 16 -- Treatment-Subsequent & Other
, 17 -- Pathology
)
-- TODO: store these in the ID repository and de-id later
and ni."AllowValue" not like 'City name or UNKNOWN'
and ni."AllowValue" not like 'Reference to EDITS table BPLACE.DBF in Appendix B'
and ni."AllowValue" not like '5-digit or 9-digit U.S. ZIP codes%'
and ni."AllowValue" not like 'Census Tract Codes%'
and ni."AllowValue" not like 'See Appendix A for standard FIPS county codes%'
and ni."AllowValue" not like 'See Appendix A for county codes for each state.%'
and ni."ItemName" not like 'Age at Diagnosis'
and ni."ItemName" not like 'Text--%'
and ni."ItemName" not like 'Place of Death'
order by 1, 2, 3, 4, 5
;
 */



/*****
 * Date parsing. Ugh.
 
 p. 97:
 "Below are the common formats to handle the situation where only
  certain components of date are known.
  YYYYMMDD - when complete date is known and valid
  YYYYMM - when year and month are known and valid, and day is unknown
  YYYY - when year is known and valid, and month and day are unknown
  Blank - when no known date applies"

But we also see wierdness such as '    2009' and '19719999'; see
test cases below.

In Date of Last Contact, we've also seen 19919999
*/
select itemname,  value, to_date(
  case
  when value in ('00000000', '99999999', '99990') then null
  when length(trim(value)) = 4 then value || '0101'
  when length(value) = 6 then value || '01'
  when substr(value, 5, 4) = '9999' then substr(value, 1, 4) || '1231'
  else value
  end,
  'yyyymmdd') as start_date
from (
select 'normal' as itemname, '19700101' as value from dual
union all
select 'no day' as itemname, '197001' as value from dual
union all
select 'no month' as itemname, '1970' as value from dual
union all
select 'leading space' as itemname, '    1970' as value from dual
union all
select 'no month, variation' as itemname, '19709999' as value from dual
union all
select 'all 9s' as itemname, '99999999' as value from dual
union all
select 'all 0s' as itemname, '00000000' as value from dual
union all
select 'almost all 9s' as itemname, '99990' as value from dual
)
;

/* Hunt down "not a valid month"
select min(to_date(ne."Date of Last Contact", 'yyyymmdd'))
from (
  select * from (
  select rownum i, "Date of Last Contact", substr("Date of Last Contact", 5, 2) mm
  from naacr.extract
  ) where i > 960 and i < 970) ne;
*/

create or replace view tumor_item_type as
select ns.sectionid
     , ni."ItemNbr" itemnbr
     , ni."Format"
     , '@' valtype_cd
     , ni."ItemName"
     , ni."ItemID"
from naacr.t_item ni
join NAACR.t_section ns on ns.sectionid = to_number(ni."SectionID")

where ni."SectionID" in (
  1 -- Cancer Identification
 , 2 -- Demographic
-- , 3 -- Edit Overrides/Conversion History/System Admin
 , 4 -- Follow-up/Recurrence/Death
-- , 5 -- Hospital-Confidential
 , 6 -- Hospital-Specific
-- , 7 -- Other-Confidential
-- , 8 -- Patient-Confidential
-- , 9 -- Record ID
-- , 10 -- Special Use
 , 11 -- Stage/Prognostic Factors
     -- TODO: for long lists of numeric codes, find metadata
     -- and chunking strategy
     -- TODO: consider normalizing complication 1, complication 2, ...
-- , 12 -- Text-Diagnosis
-- , 13 -- Text-Miscellaneous
-- , 14 -- Text-Treatment
-- , 15 -- Treatment-1st Course
, 16 -- Treatment-Subsequent & Other
, 17 -- Pathology
)
-- TODO: store these in the ID star schema and de-id later.
and ni."AllowValue" not like 'City name or UNKNOWN'
and ni."AllowValue" not like 'Reference to EDITS table BPLACE.DBF in Appendix B'
and ni."AllowValue" not like '5-digit or 9-digit U.S. ZIP codes%'
and ni."AllowValue" not like 'Census Tract Codes%'
and ni."AllowValue" not like 'See Appendix A for standard FIPS county codes%'
and ni."AllowValue" not like 'See Appendix A for county codes for each state.%'
and ni."AllowValue" not like '10-digit number'
and ni."ItemName" not like 'Age at Diagnosis'
and ni."ItemName" not like 'Text--%'
and ni."ItemName" not like 'Place of Death'
and ni."ItemName" not like 'Abstracted By'
and ni."ItemName" not like 'NPI--Archive FIN'
and ni."ItemName" not like 'NPI--Reporting Facility'
;

insert into etl_test_values (test_domain, test_name, test_value, result_id, result_date, detail_num_1, detail_char_1)
select 'Cancer Cases' test_domain, 'rx_summary_item_types' test_name
     , case when ty.itemnbr is null then 0 else 1 end test_value
     , sq_result_id.nextval result_id
     , sysdate result_date
     , rx.itemnbr, rx.itemname
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
left join tumor_item_type ty
  on ty.itemnbr = rx.itemnbr
;

/* This is the main big flat view. */
create or replace view tumor_item_value as
select "Accession Number--Hosp"
     , "Sequence Number--Hospital"
     , ni.sectionid
     , ni.valtype_cd
     , ne.ItemNbr
     , ne.value
     , case when ni."Format" = 'YYYYMMDD'
       then to_date(case
         when value in ('00000000', '99999999', '99990') then null
         when length(trim(value)) = 4 then value || '0101'
         when length(value) = 6 then value || '01'
         when substr(value, 5, 4) = '9999' then substr(value, 1, 4) || '1231'
         else value
         end, 'yyyymmdd')
       else null end
       as start_date
     , 'NAACCR|' || ne.ItemNbr || ':' || (
         case when ni."Format" = 'YYYYMMDD' then null
         else value end) as concept_cd
     , case when ni."Format" = 'YYYYMMDD' then null
       else value end as codenbr
     , ni.sectionid section
     , ni."ItemName" as ItemName
     , ni."ItemID" as ItemID
from NAACR.extract_eav ne
join tumor_item_type ni on ne.ItemNbr = ni.ItemNbr
where ne.value is not null
;
/* eyeball it:

select tiv.*
     , nc.codedcrp
 from tumor_item_value tiv
left join naacr.t_code nc
  on nc.itemid = tiv.ItemID
 and nc.codenbr = tiv.value
-- order by to_number(SectionID), 1, 2, 3

where "Accession Number--Hosp"='200000045'
 and "Sequence Number--Hospital" = '00'
order by to_number(SectionID), 1, 2, 3;
*/


/**
 * i2b2 style visit info
 */
create or replace view tumor_reg_visits as
select ne."Accession Number--Hosp" || '-' || ne."Sequence Number--Hospital"
       as encounter_ide
     , ne."Patient ID Number" as MRN
from naacr.extract ne
where ne."Accession Number--Hosp" is not null;

/* below are the known duplicates - we now have the unique index constraing as 
per #1155 so we shouldn't need this anymore but here for reference

...
and ne."Accession Number--Hosp" not in (
  '200801856'
, '199601553'
, '200200890'
);
*/


-- select * from tumor_reg_visits;
-- select count(*) from tumor_reg_visits;
-- 65576

/**
 * i2b2 style facts
 */
create or replace view tumor_reg_facts as
select MRN, encounter_ide
     , concept_cd, ItemName
     , '@' provider_id
     , start_date
     , '@' modifier_cd
     , 1 instance_num
     , valtype_cd
     , '@' as tval_char
     , to_number(null) as nval_num
     , null as valueflag_cd
     , null as units_cd
     , start_date as end_date
     , '@' location_cd
     , to_date(null) as update_date
from (
select
  ne."Patient ID Number" as MRN,
  ne."Accession Number--Hosp" || '-' || ne."Sequence Number--Hospital" as encounter_ide,
  av.concept_cd,
  av.ItemName,
  av.valtype_cd,
-- codedcrp is not unique; causes duplicate key errors in observation_fact
--  av.codedcrp,
  case
  when av.start_date is not null then av.start_date
  -- Use Date of Last Contact for Follow-up/Recurrence/Death
  when av.sectionid = 4
  then to_date(case length(ne."Date of Last Contact")
               when 8 then case
               -- handle 19919999
                 when substr(ne."Date of Last Contact", 5, 2) = '99'
                   then substr(ne."Date of Last Contact", 1, 4) || '0101'
                 else ne."Date of Last Contact"
               end
               when 6 then ne."Date of Last Contact" || '01'
               when 4 then ne."Date of Last Contact" || '0101'
               end, 'yyyymmdd')
  -- Use Date of Diagnosis for everything else
  else to_date(case length(ne."Date of Diagnosis")
               when 8 then ne."Date of Diagnosis"
               when 6 then ne."Date of Diagnosis" || '01'
               when 4 then ne."Date of Diagnosis" || '0101'
               end, 'yyyymmdd') end as start_date
from naacr.extract ne
join (
select tiv."Accession Number--Hosp"
     , tiv."Sequence Number--Hospital"
     , tiv.start_date
     , tiv.concept_cd
     , tiv.ItemName
     , tiv.SectionId
     , tiv.valtype_cd
--     , tiv.codedcrp
from tumor_item_value tiv

) av
 on ne."Accession Number--Hosp" = av."Accession Number--Hosp"
and ne."Sequence Number--Hospital" = av."Sequence Number--Hospital"
where (case 
       when av.start_date is not null then 1
       when av.sectionid = 4 and ne."Date of Last Contact" is not null then 1
       when av.sectionid != 4 and ne."Date of Diagnosis" is not null then 1
       else 0
       end) = 1
/* TODO: figure out what's up with the 42 records with no Date of Diagnosis
and the ones with no date of last contact */
and ne."Accession Number--Hosp" is not null);

/* Known duplicates handled by the unique index now as per #1155
and ne."Accession Number--Hosp" not in (
  '200801856'
, '199601553'
, '200200890'
))
;
*/

-- eyeball it:
-- select * from tumor_reg_facts order by encounter_ide desc, start_date desc;


/*ugh: multiple codedcrp s:
select tiv."Accession Number--Hosp"
     , tiv."Sequence Number--Hospital"
     , tiv.start_date
     , tiv.concept_cd
     , tiv.ItemName
     , tiv.codedcrp
from tumor_item_value tiv
where tiv."Accession Number--Hosp"='196900417'
and tiv."Sequence Number--Hospital"='00'
and tiv.concept_cd='NAACCR|190:6';

Spanish, NOS
Hispanic, NOS
Latino, NOS
*/

/* count facts by scheme

select count(*), scheme from(
select substr(f.concept_cd, 1, instr(f.concept_cd, ':')) scheme
from tumor_reg_facts f)
group by scheme
order by 1 desc;
*/
