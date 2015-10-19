/** data_char_sim.sql -- characterize, simulate data

Copyright (c) 2015 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON
 
*/

-- Check that NAACCR data dictionary and data is available.
select sectionid from naacr.t_section where 1=0;
select "ItemNbr" from naacr.t_item where 1=0;
select case_index, itemnbr from naacr.extract_eav where 1=0;  -- Note case_index introduced by HERON ETL.

-- Check that tumor_item_type from naaccr_txform.sql is available (esp. for valtype_cd)
select valtype_cd from tumor_item_type where 1=0;


drop materialized view data_agg_naaccr;
create materialized view data_agg_naaccr as
with
-- count data points by item (variable)
by_item as (
  select agg.itemnbr, ty.itemname, ty.valtype_cd, tot
  from
  (
    select eav.itemnbr, count(*) tot
    from naacr.extract_eav eav
    group by eav.itemnbr
  ) agg
  join tumor_item_type ty on ty.itemnbr = agg.itemnbr
)
,
-- break down nominals by value
by_val as (
  select by_item.itemnbr, eav.value, count(*) freq
  from naacr.extract_eav eav
  join by_item on eav.itemnbr = by_item.itemnbr
  where by_item.valtype_cd = '@'
  group by by_item.itemnbr, eav.value
)
,
-- parse dates
event as (
  select case_index, eav.itemnbr
       , coalesce( to_date_noex(value, 'YYYYMMDD')
                         , to_date_noex(value, 'MMDDYYYY')
                         , to_date_noex(value, 'YYYYMM')
, to_date_noex(value, 'YYYY')) t
  from naacr.extract_eav eav
  join tumor_item_type by_item on eav.itemnbr = by_item.itemnbr
  where by_item.valtype_cd = 'D'
)
,
-- pick out reference date variable: date of diagnosis
dxty as (
  select *
  from by_item
  where itemname = 'Date of Diagnosis')
,
-- pick out the reference event for each case
e0 as (
  select *
  from event
  where event.itemnbr = (select itemnbr from dxty)
)
,
-- normalize other events to the reference event
e2 as (
  select e0.itemnbr, sysdate - e0.t mag
  from e0
  union all
  select e.itemnbr, e.t - e0.t mag
  from event e
  join e0
    on e0.case_index = e.case_index
   and e0.itemnbr <> e.itemnbr
)
,
-- assuming normal distribution, characterize continuous data
stats as (
  select itemnbr, count(mag) qty, avg(mag) mean, stddev(mag) sd
  from e2
  group by itemnbr
)

-- For nominal data, save the probability of each value
select by_val.itemnbr, '@' valtype_cd
     , to_number(null) mean, to_number(null) sd
     , by_val.value, by_val.freq / by_item.tot * 100 pct, by_item.tot
from by_val join by_item on by_item.itemnbr = by_val.itemnbr

union all

-- For continuous data, save mean, stddev
select stats.itemnbr, by_item.valtype_cd
     , mean, sd
     , null value, null pct, stats.qty
from stats
join by_item on stats.itemnbr = by_item.itemnbr
;

select * from data_agg_naaccr;

create or replace view data_char_naaccr as
select ty.sectionid, ty.section, ty.itemnbr, ty.ItemName itemname, ty.valtype_cd
     , tot qty, round(mean / 365.25, 2) mean_yr, round(sd / 365.25, 2) sd_yr
              , mean                           , sd
     , value, round(pct, 3) pct
from tumor_item_type ty
join data_agg_naaccr d on d.itemnbr = ty.ItemNbr
order by ty.sectionid, ty.ItemNbr, value
;
-- 
select * from data_char_naaccr
where valtype_cd='D'
or itemnbr in (1760, 1780, 1861);


drop table simulated_naaccr;

create table simulated_naaccr as
with
by_item as (
  select distinct itemnbr, itemname, valtype_cd, mean, sd, qty tot
  from data_char_naaccr d
)
,
-- Compute cumulative distribution function for each value of each nominal item.
by_val as (
  select choice.itemnbr, choice.value, choice.pct, coalesce(sum(lo), 0) cdf from (
    select choice.itemnbr, choice.value, choice.pct
         , other.pct lo
    from data_agg_naaccr choice
    left join data_agg_naaccr other
           on other.itemnbr = choice.itemnbr
          and other.value < choice.value
    where choice.value is not null
    ) choice
    group by choice.itemnbr, choice.value, choice.pct
)
,
entity as (
  select rownum case_index
  from dual
  connect by rownum <= 500
)
,
-- for each item of each case, pick a uniformly random percentage and a normally distributed magnitude
ea as (
  select e.case_index, by_item.itemnbr, by_item.valtype_cd
       , by_item.tot
       , DBMS_RANDOM.VALUE * 100 x
       , DBMS_RANDOM.NORMAL * sd + mean mag
  from entity e
  cross join by_item
)
,
-- compute reference event date
-- TODO: consider distributing these uniformly rather than normally
e0 as (
  select case_index, itemnbr, tot, sysdate - mag t
  from ea
  where itemnbr = 390 -- Date of Diagnosis
)
,
-- compute dates of other events, with the same percentage of nulls
e2 as (
  select ea.case_index, ea.itemnbr, ea.tot
       , case when ea.x > ea.tot / e0.tot * 100
         then e0.t + mag
         else null
         end t
  from ea
  join e0 on e0.case_index = ea.case_index
  where e0.itemnbr <> ea.itemnbr
  and ea.valtype_cd = 'D'
)
,
event as (
  select * from e0
  union all
  select * from e2
)

-- Correlate the random percentage with the relevant value
select ea.case_index, ea.itemnbr, by_val.value
from ea
join by_val
  on by_val.itemnbr = ea.itemnbr
 and ea.x >= by_val.cdf
 and ea.x < by_val.cdf + by_val.pct

union all

-- Convert dates to strings
select event.case_index, event.itemnbr, to_char(event.t, 'YYYYMMDD') value
from event
;

/* Eyeball simulated_naaccr:

select d.case_index, ty.sectionid, ty.section, ty.itemnbr, ty.itemname, ty.valtype_cd, d.value
from simulated_naaccr d
join tumor_item_type ty on ty.itemnbr = d.itemnbr
order by d.case_index, ty.sectionid, ty.itemnbr;
*/

