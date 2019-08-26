.mode csv
.header on

select lr.loinc_number, l.component
     , rc.code_system, rc.code_value
     , la.AnswerListId, la.AnswerListName
     , a.answer_code, a.sequence_no
     , astr.answer_string
from LOINC_RELATED_CODE_LK lr
join loinc l on L.LOINC_NUM = LR.LOINC_NUMBER
join related_code rc on LR.RELATEDCODEID = RC.ID
join LoincAnswerListLinks la on la.LoincNumber = lr.loinc_number
join answer a on a.answerlistid = la.answerlistid
join answer_string astr on a.answerstringid = astr.id
where LR.SOURCE = 'NAACCR'
order by 0 + rc.code_value, 0 + a.sequence_no;
