"insert into BlueHerondata.QUERY_GLOBAL_TEMP (encounter_num, patient_num, panel_count)
with t as ( 
 select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')  
  AND (   f.start_date >= to_date('01-May-2012 00:00:00','DD-MON-YYYY HH24:MI:SS')
 )  
group by  f.encounter_num , f.patient_num 
 ) 
select t.encounter_num, t.patient_num, 0 as panel_count  from t 
<*>
insert into BlueHerondata.QUERY_GLOBAL_TEMP (encounter_num, patient_num, panel_count)
with t as ( 
 select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343316\%')  
  AND (   f.start_date >= to_date('01-May-2012 00:00:00','DD-MON-YYYY HH24:MI:SS')
 )  
group by  f.encounter_num , f.patient_num 
 ) 
select t.encounter_num, t.patient_num, 0 as panel_count  from t 
<*>
insert into BlueHerondata.QUERY_GLOBAL_TEMP (encounter_num, patient_num, panel_count)
with t as ( 
 select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343324\%')  
  AND (   f.start_date >= to_date('01-May-2012 00:00:00','DD-MON-YYYY HH24:MI:SS')
 )  
group by  f.encounter_num , f.patient_num 
 ) 
select t.encounter_num, t.patient_num, 0 as panel_count  from t 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\ICD9\A18090800\A8359006\A8352677\A8360422\A8343316\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\ICD9\A18090800\A8359006\A8352677\A8360422\A8343324\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0150 Marital Status at DX\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0668 RX Hosp--Surg App 2010\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0672 RX Hosp--Scope Reg LN Sur\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0674 RX Hosp--Surg Oth Reg/Dis\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0676 RX Hosp--Reg LN Removed\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0690 RX Hosp--Radiation\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0700 RX Hosp--Chemo\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0710 RX Hosp--Hormone\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0720 RX Hosp--BRM\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0730 RX Hosp--Other\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0740 RX Hosp--DX/Stg Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0746 RX Hosp--Surg Site 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0747 RX Hosp--Scope Reg 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0748 RX Hosp--Surg Oth 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2800 CS Tumor Size\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2960 Derived AJCC-6 N\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2980 Derived AJCC-6 M\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3410 Derived AJCC-7 N\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3420 Derived AJCC-7 M\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =1 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  0 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0560 Sequence Number--Hospital\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343316\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343324\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0160 Race 1\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0161 Race 2\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0162 Race 3\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0163 Race 4\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0164 Race 5\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0190 Spanish/Hispanic Origin\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0220 Sex\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0240 Date of Birth\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0380 Sequence Number--Central\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0390 Date of Diagnosis\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0400 Primary Site\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0410 Laterality\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0440 Grade\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0490 Diagnostic Confirmation\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:1 Cancer Identification\0521 Morph--Type&Behav ICD-O-3\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0610 Class of Case\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0670 RX Hosp--Surg Prim Site\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\0820 Regional Nodes Positive\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\0830 Regional Nodes Examined\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:4 Follow-up/Recurrence/Death\1750 Date of Last Contact\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:4 Follow-up/Recurrence/Death\1760 Vital Status\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:4 Follow-up/Recurrence/Death\1770 Cancer Status\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:4 Follow-up/Recurrence/Death\1860 Recurrence Date--1st\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:4 Follow-up/Recurrence/Death\1861 Recurrence Date--1st Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2850 CS Mets at DX\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2860 CS Mets Eval\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2869 CS Site-Specific Factor15\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2876 CS Site-Specific Factor22\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2877 CS Site-Specific Factor23\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2880 CS Site-Specific Factor 1\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2890 CS Site-Specific Factor 2\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2940 Derived AJCC-6 T\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3000 Derived AJCC-6 Stage Grp\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3020 Derived SS2000\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3400 Derived AJCC-7 T\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =2 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  1 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\3430 Derived AJCC-7 Stage Grp\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\ICD9\A18090800\A8359006\A8352677\A8360422\A8343316\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\ICD9\A18090800\A8359006\A8352677\A8360422\A8343324\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:2 Demographic\0230 Age at Diagnosis\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\0748 RX Hosp--Surg Oth 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1200 RX Date--Surgery\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1201 RX Date--Surgery Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1210 RX Date--Radiation\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1211 RX Date--Radiation Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1220 RX Date--Chemo\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1221 RX Date--Chemo Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1230 RX Date--Hormone\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1240 RX Date--BRM\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1241 RX Date--BRM Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1250 RX Date--Other\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1251 RX Date--Other Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1280 RX Date--DX/Stg Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1281 RX Date--Dx/Stg Proc Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1285 RX Summ--Treatment Status\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1290 RX Summ--Surg Prim Site\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1292 RX Summ--Scope Reg LN Sur\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1294 RX Summ--Surg Oth Reg/Dis\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1296 RX Summ--Reg LN Examined\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1310 RX Summ--Surgical Approch\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1320 RX Summ--Surgical Margins\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1350 RX Summ--DX/Stg Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1340 Reason for No Surgery\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1350 RX Summ--DX/Stg Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1360 RX Summ--Radiation\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1370 RX Summ--Rad to CNS\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1380 RX Summ--Surg/Rad Seq\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1390 RX Summ--Chemo\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1400 RX Summ--Hormone\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1410 RX Summ--BRM\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1420 RX Summ--Other\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1430 Reason for No Radiation\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1639 RX Summ--Systemic/Sur Seq\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1640 RX Summ--Surgery Type\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1646 RX Summ--Surg Site 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1647 RX Summ--Scope Reg 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1648 RX Summ--Surg Oth 98-02\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3170 RX Date--Most Defin Surg\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3171 RX Date Mst Defn Srg Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3180 RX Date--Surgical Disch\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3181 RX Date Surg Disch Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3220 RX Date--Radiation Ended\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3221 RX Date Rad Ended Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3230 RX Date--Systemic\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3231 RX Date Systemic Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3250 RX Summ--Transplnt/Endocr\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:6 Hospital-Specific\3280 RX Hosp--Palliative Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1231 RX Date--Hormone Flag\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\1330 RX Summ--Reconstruct 1st\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2830 CS Lymph Nodes\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:11 Stage/Prognostic Factors\2840 CS Lymph Nodes Eval\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:15 Treatment-1st Course\3270 RX Summ--Palliative Proc\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2085 Date Case Initiated\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2090 Date Case Completed\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2092 Date Case Completed--CoC\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2100 Date Case Last Changed\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2110 Date Case Report Exported\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =3 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  2 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */  f.encounter_num, f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\S:3 Edit Overrides/Conversion History/System Admin\2111 Date Case Report Received\%')   
group by  f.encounter_num , f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num  and BlueHerondata.QUERY_GLOBAL_TEMP.encounter_num = t.encounter_num   ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343316\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343324\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Procedures\PRC\ICD9 (Inpatient)\(85-86) Operations on integument\(p85) Operations on the breast\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Medications\RXAUI:3257528\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Medications\RXAUI:3257701\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Procedures\PRC\Metathesaurus CPT Hierarchical Terms\Radiology Procedures\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =4 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  3 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Procedures\PRC\Metathesaurus CPT Hierarchical Terms\Surgical Procedures\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\naaccr\SEER Site\Breast\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343316\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Diagnoses\A18090800\A8359006\A8352677\A8360422\A8343324\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Demographics\Vital Status\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Demographics\Gender\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Demographics\Race\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Demographics\Language\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path LIKE '\i2b2\Demographics\Marital Status\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Visit Details\Vitals\BMI\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Visit Details\Vitals\HEIGHT\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
update BlueHerondata.QUERY_GLOBAL_TEMP set panel_count =5 where BlueHerondata.QUERY_GLOBAL_TEMP.panel_count =  4 and exists ( select 1 from ( select  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */ f.patient_num  
from BlueHerondata.observation_fact f 
where  
f.concept_cd IN (select concept_cd from  BlueHerondata.concept_dimension   where concept_path like '\i2b2\Visit Details\Vitals\WEIGHT\%')   
group by  f.patient_num ) t where BlueHerondata.QUERY_GLOBAL_TEMP.patient_num = t.patient_num    ) 
<*>
 insert into BlueHerondata.DX (  patient_num   ) select * from ( select distinct  patient_num  from BlueHerondata.QUERY_GLOBAL_TEMP where panel_count = 5 ) q"