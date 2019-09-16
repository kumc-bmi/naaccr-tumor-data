/** data_char_sim.sql -- characterize, simulate data

Copyright (c) 2015 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON
 
*/

-- Check that NAACCR data dictionary and data is available.
select sectionid from section where 1=0;
select recordId, dateOfDiagnosis from naaccr_extract where 1=0;
select recordId, naaccrNum from tumors_eav where 1=0;
select `naaccr-item-num`, section from record_layout;

-- Check that tumor_item_type from naaccr_txform.sql is available (esp. for valtype_cd)
select valtype_cd from tumor_item_type where 1=0;


create or replace temporary view data_agg_naaccr as
with
cases as (
  select count(*) as tumor_qty
  from naaccr_extract
)
-- count data points by item (variable)
, by_item as (
  select agg.naaccrId, ty.naaccrNum, ty.valtype_cd, present
  from
  (
    select eav.naaccrId, count(*) present
    from tumors_eav eav
    group by eav.naaccrId
  ) agg
  join tumor_item_type ty on ty.naaccrId = agg.naaccrId
)
,
-- break down nominals by value
by_val as (
  select by_item.naaccrId, by_item.naaccrNum, eav.value, count(*) freq
  from tumors_eav eav
  join by_item on eav.naaccrId = by_item.naaccrId
  where by_item.valtype_cd = '@'
  group by by_item.naaccrId, by_item.naaccrNum, eav.value
)
/*
,
-- TODO: parse dates
event as (
  select tumorid as case_index, eav.naaccrNum
       , date(substr(value, 1, 4) || '-' || substr(value, 5, 2) || '-' || substr(value, 7, 2)) as t
  from tumors_eav eav
  join tumor_item_type by_item on eav.naaccrNum = by_item.naaccrNum
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
  where event.naaccrNum = (select naaccrNum from dxty)
)
,
-- normalize other events to the reference event
e2 as (
  select e0.naaccrNum, julianday(current_date) - julianday(e0.t) mag
  from e0
  union all
  select e.naaccrNum, julianday(e.t) - julianday(e0.t) mag
  from event e
  join e0
    on e0.case_index = e.case_index
   and e0.naaccrNum <> e.naaccrNum
)
,
-- assuming normal distribution, characterize continuous data
stats as (
  select naaccrNum, count(mag) present, avg(mag) mean, stddev(mag) sd
  from e2
  group by naaccrNum
)
*/

-- For nominal data, save the probability of each value
select by_val.naaccrId, by_val.naaccrNum, '@' valtype_cd
     , cast(null as float) mean, cast(null as float) sd
     , by_val.value, by_val.freq, by_val.freq * 1.0 / cases.tumor_qty * 100 as pct
     , by_item.present
     , cases.tumor_qty
from by_val join by_item on by_item.naaccrId = by_val.naaccrId
cross join cases

/* TODO
union all

-- For continuous data, save mean, stddev
select stats.naaccrNum, by_item.valtype_cd
     , mean, sd
     , null value, null freq, null pct, stats.present, cases.tumor_qty
from stats
join by_item on stats.naaccrNum = by_item.naaccrNum
cross join cases
*/

;

create or replace temporary view data_char_naaccr_nom as
select s.sectionId, rl.section, nom.*
from data_agg_naaccr nom
join record_layout rl
  on rl.`naaccr-item-num` = nom.naaccrNum
join section s
  on s.section = rl.section
;

create or replace temporary view data_char_naaccr as
select ty.sectionid, ty.section, ty.naaccrNum, ty.naaccrId, ty.valtype_cd
     , freq, present, tumor_qty
     , round(mean / 365.25, 2) mean_yr, round(sd / 365.25, 2) sd_yr
     , mean                           , sd
     , value, round(pct, 3) pct
from tumor_item_type ty
join data_agg_naaccr d on d.NaaccrNum = ty.NaaccrNum
order by ty.sectionid, ty.NaaccrNum, value
;


-- Compute cumulative distribution function for each value of each nominal item.
;

create or replace temporary view nominal_cdf as
select naaccrNum, naaccrId, value, pct
     , sum(pct)
       over(partition by naaccrNum, xmlid
            order by value
            rows between unbounded preceding and current row)
            as cdf
from data_agg_naaccr
order by naaccrNum, xmlid, value;
;


create or replace temporary view simulated_naaccr_nom as
with
by_item as (
  select distinct naaccrNum, naaccrId, valtype_cd, mean, sd, present, tumor_qty
  from data_char_naaccr d
)
,
-- for each item of each case, pick a uniformly random percentage
ea as (
  select e.case_index, by_item.naaccrNum, by_item.naaccrId, by_item.valtype_cd
       , by_item.present, by_item.tumor_qty
       , rand() * 100 as x
       -- , (e.case_index * 1017 + by_item.naaccrNum) % 100 as x  -- kludge; random() and CTEs don't work in Oracle
  from simulated_entity e
  cross join by_item
)
select ea.case_index, ea.naaccrNum, ea.naaccrId, by_val.value
     , ea.x, by_val.cdf, by_val.pct
from ea
join nominal_cdf by_val
  on by_val.naaccrNum = ea.naaccrNum
where ea.x >= by_val.cdf - by_val.pct
  and ea.x < by_val.cdf
;


create or replace temporary view simulated_naaccr as
with
by_item as (
  select distinct naaccrNum, naaccrId, valtype_cd, mean, sd, present, tumor_qty
  from data_char_naaccr d
)
,
-- for each item of each case, pick a uniformly random percentage and a normally distributed magnitude
ea as (
  select e.case_index, by_item.naaccrNum, by_item.valtype_cd
       , by_item.present, by_item.tumor_qty
       , (e.case_index * 1017 + by_item.naaccrNum) % 100 as x  -- kludge; random() and CTEs don't work
       , ((e.case_index * 1017 + by_item.naaccrNum) % 100) / 100.0 * sd + mean as mag
  from simulated_entity e
  cross join by_item
)
,
-- compute reference event date
-- TODO: consider distributing these uniformly rather than normally
e0 as (
  select case_index, naaccrNum, tumor_qty, date(current_date, '-' || mag || ' days') t
       , ea.x, ea.mag
  from ea
  where naaccrNum = 390 -- Date of Diagnosis
)
,
-- compute dates of other events, with the same percentage of nulls
e2 as (
  select ea.case_index, ea.naaccrNum, ea.tumor_qty
       , case when ea.x > ea.present / e0.tumor_qty * 100
         then date(e0.t, ea.mag || ' days')
         else null
         end t
       , ea.x, ea.mag
  from ea
  join e0 on e0.case_index = ea.case_index
  where e0.naaccrNum <> ea.naaccrNum
  and ea.valtype_cd = 'D'
)
,
event as (
  select * from e0
  union all
  select * from e2
)

-- Correlate the random percentage with the relevant value
select ea.case_index, ea.naaccrNum, by_val.value
     , ea.x, by_val.cdf, by_val.pct
from ea
join nominal_cdf by_val
  on by_val.naaccrNum = ea.naaccrNum
where ea.x >= by_val.cdf
  and ea.x < by_val.cdf + by_val.pct

union all

-- Convert dates to strings
select event.case_index, event.naaccrNum
     , substr(event.t, 1, 4) ||
       substr(event.t, 5, 2) ||
       substr(event.t, 7, 2) value
     , x, null, mag
from event
;

/* Eyeball simulated_naaccr:

select d.case_index, ty.sectionid, ty.section, ty.naaccrNum, ty.itemname, ty.valtype_cd, d.value
from simulated_naaccr d
join tumor_item_type ty on ty.naaccrNum = d.naaccrNum
order by d.case_index, ty.sectionid, ty.naaccrNum;
*/

