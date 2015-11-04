#!/bin/bash

rm -f ./*.log ./*.bad

# SC2164 Use cd ... || exit in case cd fails.
# -- http://www.shellcheck.net/
cd "/d1/naaccr/$yyyy_mm_naaccr" || exit

# Take note of the number of records in the new file in the monthly staging ticket and make sure it's greater than the data from last month.
echo previous record count: "$record_count_prev"
record_count=$(wc -l "${naaccr_data}.DAT" |cut -d' ' -f1)
echo new record count: "$record_count"
if test "$record_count" -lt "$record_count_prev"
then
  exit 1
fi

sqlldr NAACR/${staging_account_password}@${staging_sid} \
  control="${WORKSPACE}/heron_staging/tumor_reg/naaccr_extract.ctl" direct=true rows=10000 \
  log="${naaccr_data}.log" \
  bad="${naaccr_data}.bad" \
  data="${naaccr_data}.DAT"

cp "${naaccr_data}.log" "${WORKSPACE}"

[ -f "${naaccr_data}.bad" ] && cp "${naaccr_data}.bad" "${WORKSPACE}" && wc -l "${naaccr_data}.bad"


sqlplus NAACR/${staging_account_password}@${staging_sid} <<EOF

whenever sqlerror exit failure

update NAACR.EXTRACT
set case_index = naacr.sq_case_index.nextval;

commit;

EOF
