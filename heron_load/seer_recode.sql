/** seer_recode -- recode primary site, histology into SEER site summary

Copyright (c) 2012-2019 University of Kansas Medical Center

Largely derived from:
  SEER Site Recode ICD-O-3 (1/27/2003) Definition 
  http://seer.cancer.gov/siterecode/icdo3_d01272003/

by way of seer_recode.py

  http://informatics.kumc.edu/work/browser/tumor_reg/seer_recode.py
 */

create or replace temporary view seer_recode_aux as
with per_tumor as (
select primarySite as site, histologicTypeIcdO3 as histology
  -- ISSUE: histologyIcdO2, histologyIcdO1
     , ne.*
from naaccr_extract_id ne
)

select
case
/* Lip */ when (site between 'C000' and 'C009')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20010'

/* Tongue */ when (site between 'C019' and 'C029')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20020'

/* Salivary Gland */ when (site between 'C079' and 'C089')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20030'

/* Floor of Mouth */ when (site between 'C040' and 'C049')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20040'

/* Gum and Other Mouth */ when (site between 'C030' and 'C039'
   or site between 'C050' and 'C059'
   or site between 'C060' and 'C069')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20050'

/* Nasopharynx */ when (site between 'C110' and 'C119')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20060'

/* Tonsil */ when (site between 'C090' and 'C099')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20070'

/* Oropharynx */ when (site between 'C100' and 'C109')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20080'

/* Hypopharynx */ when (site = 'C129'
   or site between 'C130' and 'C139')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20090'

/* Other Oral Cavity and Pharynx */ when (site = 'C140'
   or site between 'C142' and 'C148')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '20100'

/* Esophagus */ when (site between 'C150' and 'C159')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21010'

/* Stomach */ when (site between 'C160' and 'C169')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21020'

/* Small Intestine */ when (site between 'C170' and 'C179')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21030'

/* Cecum */ when (site = 'C180')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21041'

/* Appendix */ when (site = 'C181')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21042'

/* Ascending Colon */ when (site = 'C182')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21043'

/* Hepatic Flexure */ when (site = 'C183')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21044'

/* Transverse Colon */ when (site = 'C184')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21045'

/* Splenic Flexure */ when (site = 'C185')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21046'

/* Descending Colon */ when (site = 'C186')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21047'

/* Sigmoid Colon */ when (site = 'C187')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21048'

/* Large Intestine, NOS */ when (site between 'C188' and 'C189'
   or site = 'C260')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21049'

/* Rectosigmoid Junction */ when (site = 'C199')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21051'

/* Rectum */ when (site = 'C209')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21052'

/* Anus, Anal Canal and Anorectum */ when (site between 'C210' and 'C212'
   or site = 'C218')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21060'

/* Liver */ when (site = 'C220')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21071'

/* Intrahepatic Bile Duct */ when (site = 'C221')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21072'

/* Gallbladder */ when (site = 'C239')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21080'

/* Other Biliary */ when (site between 'C240' and 'C249')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21090'

/* Pancreas */ when (site between 'C250' and 'C259')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21100'

/* Retroperitoneum */ when (site = 'C480')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21110'

/* Peritoneum, Omentum and Mesentery */ when (site between 'C481' and 'C482')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21120'

/* Other Digestive Organs */ when (site between 'C268' and 'C269'
   or site = 'C488')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '21130'

/* Nose, Nasal Cavity and Middle Ear */ when (site between 'C300' and 'C301'
   or site between 'C310' and 'C319')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '22010'

/* Larynx */ when (site between 'C320' and 'C329')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '22020'

/* Lung and Bronchus */ when (site between 'C340' and 'C349')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '22030'

/* Pleura */ when (site = 'C384')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '22050'

/* Trachea, Mediastinum and Other Respiratory Organs */ when (site = 'C339'
   or site between 'C381' and 'C383'
   or site = 'C388'
   or site = 'C390'
   or site = 'C398'
   or site = 'C399')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '22060'

/* Bones and Joints */ when (site between 'C400' and 'C419')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '23000'

/* Soft Tissue including Heart */ when (site = 'C380'
   or site between 'C470' and 'C479'
   or site between 'C490' and 'C499')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '24000'

/* Melanoma of the Skin */ when (site between 'C440' and 'C449')
  and (histology between '8720' and '8790') then '25010'

/* Other Non-Epithelial Skin */ when (site between 'C440' and 'C449')
  and  not (histology between '8000' and '8005'
   or histology between '8010' and '8046'
   or histology between '8050' and '8084'
   or histology between '8090' and '8110'
   or histology between '8720' and '8790'
   or histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '25020'

/* Breast */ when (site between 'C500' and 'C509')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '26000'

/* Cervix Uteri */ when (site between 'C530' and 'C539')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27010'

/* Corpus Uteri */ when (site between 'C540' and 'C549')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27020'

/* Uterus, NOS */ when (site = 'C559')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27030'

/* Ovary */ when (site = 'C569')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27040'

/* Vagina */ when (site = 'C529')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27050'

/* Vulva */ when (site between 'C510' and 'C519')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27060'

/* Other Female Genital Organs */ when (site between 'C570' and 'C589')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '27070'

/* Prostate */ when (site = 'C619')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '28010'

/* Testis */ when (site between 'C620' and 'C629')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '28020'

/* Penis */ when (site between 'C600' and 'C609')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '28030'

/* Other Male Genital Organs */ when (site between 'C630' and 'C639')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '28040'

/* Urinary Bladder */ when (site between 'C670' and 'C679')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '29010'

/* Kidney and Renal Pelvis */ when (site = 'C649'
   or site = 'C659')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '29020'

/* Ureter */ when (site = 'C669')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '29030'

/* Other Urinary Organs */ when (site between 'C680' and 'C689')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '29040'

/* Eye and Orbit */ when (site between 'C690' and 'C699')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '30000'

/* Brain */ when (site between 'C710' and 'C719')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9530' and '9539'
   or histology between '9590' and '9992') then '31010'

/* Cranial Nerves Other Nervous System */ when (site between 'C710' and 'C719')
  and (histology between '9530' and '9539') then '31040'

/* None */ when (site between 'C700' and 'C709'
   or site between 'C720' and 'C729')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '31040'

/* Thyroid */ when (site = 'C739')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '32010'

/* Other Endocrine including Thymus */ when (site = 'C379'
   or site between 'C740' and 'C749'
   or site between 'C750' and 'C759')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '32020'

/* Hodgkin - Nodal */ when (site = 'C024'
   or site between 'C098' and 'C099'
   or site = 'C111'
   or site = 'C142'
   or site = 'C379'
   or site = 'C422'
   or site between 'C770' and 'C779')
  and (histology between '9650' and '9667') then '33011'

/* Hodgkin - Extranodal */ when (site = 'All other sites')
  and (histology between '9650' and '9667') then '33012'

/* NHL - Nodal */ when (site = 'C024'
   or site = 'C098'
   or site = 'C099'
   or site = 'C111'
   or site = 'C142'
   or site = 'C379'
   or site = 'C422'
   or site between 'C770' and 'C779')
  and (histology between '9590' and '9597'
   or histology between '9670' and '9671'
   or histology = '9673'
   or histology = '9675'
   or histology between '9678' and '9680'
   or histology = '9684'
   or histology between '9687' and '9691'
   or histology = '9695'
   or histology between '9698' and '9702'
   or histology = '9705'
   or histology between '9708' and '9709'
   or histology = '9712'
   or histology between '9714' and '9719'
   or histology between '9724' and '9729'
   or histology = '9735'
   or histology between '9737' and '9738'
   or histology between '9811' and '9818'
   or histology = '9823'
   or histology = '9827'
   or histology = '9837') then '33041'

/* NHL - Extranodal */ when  not (site = 'C024'
   or site between 'C098' and 'C099'
   or site = 'C111'
   or site = 'C142'
   or site = 'C379'
   or site = 'C422'
   or site between 'C770' and 'C779')
  and (histology between '9590' and '9597'
   or histology between '9670' and '9671'
   or histology = '9673'
   or histology = '9675'
   or histology between '9678' and '9680'
   or histology = '9684'
   or histology between '9687' and '9691'
   or histology = '9695'
   or histology between '9698' and '9702'
   or histology = '9705'
   or histology between '9708' and '9709'
   or histology = '9712'
   or histology between '9714' and '9719'
   or histology between '9724' and '9729'
   or histology = '9735'
   or histology between '9737' and '9738') then '33042'

/* None */ when  not (site = 'C024'
   or site between 'C098' and 'C099'
   or site = 'C111'
   or site = 'C142'
   or site = 'C379'
   or site between 'C420' and 'C422'
   or site = 'C424'
   or site between 'C770' and 'C779')
  and (histology between '9811' and '9818'
   or histology = '9823'
   or histology = '9827'
   or histology = '9837') then '33042'

/* Myeloma */ when (histology between '9731' and '9732'
   or histology = '9734') then '34000'

/* Acute Lymphocytic Leukemia */ when (histology = '9826'
   or histology between '9835' and '9836') then '35011'

/* None */ when (site = 'C420'
   or site = 'C421'
   or site = 'C424')
  and (histology between '9811' and '9818'
   or histology = '9837') then '35011'

/* Chronic Lymphocytic Leukemia */ when (site = 'C420'
   or site = 'C421'
   or site = 'C424')
  and (histology = '9823') then '35012'

/* Other Lymphocytic Leukemia */ when (histology = '9820'
   or histology between '9832' and '9834'
   or histology = '9940') then '35013'

/* Acute Myeloid Leukemia */ when (histology = '9840'
   or histology = '9861'
   or histology between '9865' and '9867'
   or histology = '9869'
   or histology between '9871' and '9874'
   or histology between '9895' and '9897'
   or histology = '9898'
   or histology between '9910' and '9911'
   or histology = '9920') then '35021'

/* Acute Monocytic Leukemia */ when (histology = '9891') then '35031'

/* Chronic Myeloid Leukemia */ when (histology = '9863'
   or histology between '9875' and '9876'
   or histology between '9945' and '9946') then '35022'

/* Other Myeloid/Monocytic Leukemia */ when (histology = '9860'
   or histology = '9930') then '35023'

/* Other Acute Leukemia */ when (histology = '9801'
   or histology between '9805' and '9809'
   or histology = '9931') then '35041'

/* Aleukemic, subleukemic and NOS */ when (histology = '9733'
   or histology = '9742'
   or histology = '9800'
   or histology = '9831'
   or histology = '9870'
   or histology = '9948'
   or histology between '9963' and '9964') then '35043'

/* None */ when (site = 'C420'
   or site = 'C421'
   or site = 'C424')
  and (histology = '9827') then '35043'

/* Mesothelioma */ when (histology between '9050' and '9055') then '36010'

/* Kaposi Sarcoma */ when (histology = '9140') then '36020'

/* Miscellaneous */ when (histology between '9740' and '9741'
   or histology between '9750' and '9769'
   or histology = '9950'
   or histology between '9960' and '9962'
   or histology between '9965' and '9967'
   or histology between '9970' and '9971'
   or histology = '9975'
   or histology = '9980'
   or histology between '9982' and '9987'
   or histology = '9989'
   or histology between '9991' and '9992') then '37000'

/* None */ when (site between 'C760' and 'C768'
   or site = 'C809')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '37000'

/* None */ when (site between 'C420' and 'C424')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '37000'

/* None */ when (site between 'C770' and 'C779')
  and  not (histology between '9050' and '9055'
   or histology = '9140'
   or histology between '9590' and '9992') then '37000'

end

as recode
, per_tumor.*
from per_tumor
;


create or replace temporary view seer_recode_facts as
with per_tumor as (
select recordId
     , patientIdNumber
     , recode
     , dateOfDiagnosis start_date
     , coalesce(dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis) update_date
from seer_recode_aux
)

select recordId
     , patientIdNumber
     , '@' naaccrId
     , concat('SEER_SITE:', recode) concept_cd
     , '@' provider_id
     , start_date
     , '@' modifier_cd
     , 1 instance_num
     , '@' as valtype_cd
     , '@' as tval_char
     , cast(null as float) as nval_num
     , null as valueflag_cd
     , null as units_cd
     , start_date as end_date
     , '@' location_cd
     , update_date
from per_tumor ne
where start_date is not null
;


create or replace temporary view cs_site_factor_facts as
with per_obs as (
select recordId
     , patientIdNumber
     , recode
     , naaccrId
     , raw_value
     , dateOfDiagnosis start_date
     , coalesce(dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis) update_date
from cs_obs_raw
)
select recordId
     , patientIdNumber
     , '@' naaccrId
     , concat('CS', sra.recode, '|',
              substr(sra.naaccrId, length('csSiteSpecificFactor1')),
              ':', sra.raw_value) concept_cd
     , '@' provider_id
     , sra.start_date
     , '@' modifier_cd  -- hmm... modifier for synthesized info?
     , 1 instance_num
     , '@' valtype_cd
     , cast(null as string) as tval_char
     , cast(null as float) as nval_num
     , cast(null as string) as valueflag_cd
     , cast(null as string) as units_cd
     , sra.start_date as end_date
     , '@' location_cd
     , update_date
from per_obs sra
where start_date is not null
;


/*
select count(*), concept_cd
from seer_recode_facts
group by concept_cd
order by 1 desc;
*/

/**
 * Verify the above algorithm vs. results of John K.'s SAS code.
select count(*) from naacr.extract;
select count(*) from seer_recode_facts;
select count(*) from seer_jk;

select count(distinct mrn)
from seer_recode_facts
where concept_cd='SEER_SITE:22030';
-- 6443 here; 6407 in i2b2. hmm.


select jk.*, sf.recode,
  sf.site,
  sf.histology
from
seer_recode_aux sf
left join seer_jk jk
on jk.accno = sf."Accession Number--Hosp"
  and jk.SeqNoHos = sf."Sequence Number--Hospital"
  and jk.sitenew = sf.site
  and jk.histo3 = sf.histology
where sf.recode != jk.site_recode
order by jk.site_recode
;
 */


/* TODO: Handle null histology.
select case
 when (site between 'C019' and 'C029')
  and  not (histology between '9590' and '9989'
   or histology between '9050' and '9055'
   or histology = '9140') then '20020'
   end recode
   from
   (select 'C019' site, null histology from dual);

For now, verify that it's in the noise:
 */

select case when missing_histology / tot > .001 then 1/0
            else 1 end as few_missing_histologies
from (
select (
select count(*)
from naacr.extract ne
 where ne."Morph--Type&Behav ICD-O-3" is null) missing_histology,
(select count(*)  from naacr.extract ne) tot
from dual
) 
;
