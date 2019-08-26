.mode csv
.header on

select rc.code_system, rc.code_value
     , l.loinc_num, l.component, l.DisplayName
     , l.property, l.time_aspct, l.system, l.scale_typ, l.method_typ
     , l.AnswerListId, l.answer_list_type
     , l.example_units, l.unitsandrange
     , l.long_common_name, l.DefinitionDescription
from loinc_related_code_lk lr
join related_code rc
  on lr.relatedcodeid = rc.id
join loinc l
  on l.loinc_num = lr.loinc_number
where lr.source = 'NAACCR'
order by 0 + rc.code_value;
