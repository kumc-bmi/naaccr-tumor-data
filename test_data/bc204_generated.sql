insert into QUERY_GLOBAL_TEMP (encounter_num, patient_num, panel_count)
with t as ( 
 select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\SEER Site\\Breast\\%')   
group by  f.encounter_num , f.patient_num 
 ) 
select t.encounter_num, t.patient_num, 0 as panel_count  from t 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0440 Grade\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\SEER Site\\Breast\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0490 Diagnostic Confirmation\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0521 Morph--Type&Behav ICD-O-3\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1750 Date of Last Contact\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1760 Vital Status\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1860 Recurrence Date--1st\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0190 Spanish/Hispanic Origin\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3430 Derived AJCC-7 Stage Grp\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0160 Race 1\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0161 Race 2\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0162 Race 3\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0163 Race 4\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0164 Race 5\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0170 Race Coding Sys--Current\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0180 Race Coding Sys--Original\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0380 Sequence Number--Central\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0410 Laterality\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\0820 Regional Nodes Positive\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\0830 Regional Nodes Examined\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2869 CS Site-Specific Factor15\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2876 CS Site-Specific Factor22\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2877 CS Site-Specific Factor23\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2880 CS Site-Specific Factor 1\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2890 CS Site-Specific Factor 2\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2940 Derived AJCC-6 T\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3020 Derived SS2000\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3400 Derived AJCC-7 T\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1770 Cancer Status\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:4 Follow-up/Recurrence/Death\\1861 Recurrence Date--1st Flag\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0390 Date of Diagnosis\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0220 Sex\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\3000 Derived AJCC-6 Stage Grp\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2850 CS Mets at DX\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:11 Stage/Prognostic Factors\\2860 CS Mets Eval\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0670 RX Hosp--Surg Prim Site\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:1 Cancer Identification\\0400 Primary Site\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =1 where QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:2 Demographic\\0240 Date of Birth\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =2 where QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path LIKE '\\i2b2\\Demographics\\Language\\%')   
group by  f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update QUERY_GLOBAL_TEMP set panel_count =2 where QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path LIKE '\\i2b2\\Demographics\\Vital Status\\%')   
group by  f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\30 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\31 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\32 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\33 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\34 Type of case not required by CoC to b\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\36 Type of case not required by CoC to b\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\38 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\40 Diagnosis AND all first course treatm\\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 update QUERY_GLOBAL_TEMP set panel_count = -1  where QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select   f.encounter_num, f.patient_num  
from i2b2demodata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  i2b2demodata.concept_dimension   where concept_path like '\\i2b2\\naaccr\\S:6 Hospital-Specific\\0610 Class of Case\\99 \\%')   
group by  f.encounter_num , f.patient_num ) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   )  
<*>
 insert into DX (  patient_num  , encounter_num  ) select * from ( select distinct  patient_num  , encounter_num from QUERY_GLOBAL_TEMP where panel_count = 2 ) q
 