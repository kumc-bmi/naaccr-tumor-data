.mode csv
.header on

select lr.loinc_number, rc.code_system, rc.code_value, l.component
from LOINC_RELATED_CODE_LK lr
join related_code rc
  on LR.RELATEDCODEID = RC.ID
join loinc l
  on L.LOINC_NUM = LR.LOINC_NUMBER
where LR.SOURCE = 'NAACCR'
order by 0 + rc.code_value;
