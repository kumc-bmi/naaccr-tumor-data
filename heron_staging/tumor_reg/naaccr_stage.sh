#!/bin/bash
set -e

# SC2164 Use cd ... || exit in case cd fails.
# -- http://www.shellcheck.net/
cd "/d1/naaccr/$yyyy_mm_naaccr" || exit

# Take note of the number of records in the new incremental update file.
record_count=$(wc -l "${naaccr_data}" |cut -d' ' -f1)
echo records in the incremental update: "$record_count"

# Count rows in the extract table, then create the incremental update table if needed.
sqlplus NAACR/${staging_account_password}@${staging_sid} <<EOF

set echo on;
whenever sqlerror exit failure

select 'The extract table contains ' || count(*) || ' rows before update.' msg from NAACR.EXTRACT;

whenever sqlerror continue;

create table naacr.EXTRACT_INCR as select * from naacr.EXTRACT where 1=0;

EOF


sqlldr NAACR/${staging_account_password}@${staging_sid} \
  control="${WORKSPACE}/heron_staging/tumor_reg/naaccr_extract.ctl" direct=true rows=10000 \
  log="${naaccr_data}.log" \
  bad="${naaccr_data}.bad" \
  data="${naaccr_data}"

cp "${naaccr_data}.log" "${WORKSPACE}"

# Hopefully, there isn't a bad file, but if so copy it to the workspace and count lines
! ( [ -f "${naaccr_data}.bad" ] && cp "${naaccr_data}.bad" "${WORKSPACE}" && wc -l "${naaccr_data}.bad" )


# Merge new data
sqlplus NAACR/${staging_account_password}@${staging_sid} <<EOF

whenever sqlerror exit failure

set echo on;

-- Count what we should have just loaded
select 'The incremental update table (naacr.extract_incr) now contains ' || count(*) || ' rows.' msg from NAACR.EXTRACT_INCR;

/* Fail if we have cases where Sequence Number--Hospital is null in either 
the existing cumulative data or the update.
*/
select case when count(*) = 0 then 1 else 1/0 end seq_hsp_null from (
  select * from naacr.extract_incr where "Sequence Number--Hospital" is null
  union all
  select * from naacr.extract where "Sequence Number--Hospital" is null
  );

-- Which rows are updates to existing data?
create or replace view updates as
select ex."Accession Number--Hosp", ex."Sequence Number--Hospital"
from naacr.extract_incr inc
join naacr.extract ex on ex."Accession Number--Hosp" = inc."Accession Number--Hosp" 
and ex."Sequence Number--Hospital" = inc."Sequence Number--Hospital";

select 'The incremental update contains ' || count(*) || ' corrections to existing data.' msg from updates;

/* Make sure we don't try to update rows in the existing data that are duplicates
with respect to "Accession Number--Hosp", "Sequence Number--Hospital"
*/
with existing_dups as (
  select count(*) qty, "Accession Number--Hosp", "Sequence Number--Hospital"
  from naacr.extract
  group by "Accession Number--Hosp", "Sequence Number--Hospital"
  having count(*) > 1
  )
select case when count(*) = 0 then 1 else 1/0 end update_existing_dups from (
  select * from existing_dups dups
  join updates upd on upd."Accession Number--Hosp" = dups."Accession Number--Hosp"
  )
;

-- Delete rows from existing data that we plan to update
delete from naacr.extract where 
("Accession Number--Hosp", "Sequence Number--Hospital")
in (
  select * from updates
  );

select 'The extract table contains ' || count(*) || ' rows after deleting rows to be updated.' msg from NAACR.EXTRACT;

-- Insert all rows from the incremental update
insert into naacr.extract 
select * from naacr.extract_incr;

select 'The extract table contains ' || count(*) || ' rows after incremental update.' msg from NAACR.EXTRACT;

update NAACR.EXTRACT
set case_index = naacr.sq_case_index.nextval;

EOF

