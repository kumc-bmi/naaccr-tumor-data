/** csschema.sql -- i2b2 facts for NAACCR site-specific factors using CS Schema

Copyright (c) 2016 University of Kansas Medical Center
part of the HERON* open source codebase; see NOTICE file for license details.
* http://informatics.kumc.edu/work/wiki/HERON

Largely derived from:

  Collaborative Staging System Schema
  American Joint Committee on Cancer (AJCC)
  https://cancerstaging.org/cstage/schema/Pages/version0205.aspx
  https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip

by way of csterms.py

  http://informatics.kumc.edu/work/browser/tumor_reg/csterms.py
 */

-- test that we're in the KUMC sid with the NAACCR data
-- note mis-spelling of schema name: naacr
select "Accession Number--Hosp" from naacr.extract where 1=0;

set define off;


create or replace view tumor_cs_schema as
  select MRN
       , ne.case_index
       , start_date
       , ne.primary_site, ne.histology
       , case
/* Anus, Anal Canal, and Other Parts of Rectum
C21.0-C21.2, C21.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1
C21.0 Anus, NOS (excluding skin of anus and perianal skin C44.5)
C21.1 Anal canal
C21.2 Cloacogenic zone
C21.8 Overlapping lesion of rectum, anus and anal canal
Note:  Skin of anus is coded separately (C44.5). */
 when (primary_site in ('C210', 'C211', 'C212', 'C218') and (not primary_site in ('C445')))
  then 'Anus'

/* Cystic Duct
C24.0
C24.0 Extrahepatic Bile Duct
Note: In the 7th edition of the AJCC manual, Cystic Duct was removed from the Extrahepatic Bile Duct staging chapter and added to the Gallbladder staging chapter.  For Collaborative Stage version 2, a new schema was created for Cystic Duct due to differences between schemas for bile ducts and gallbladder.  */
 when primary_site in ('C240')
  then 'CysticDuct'

/* Malignant Melanoma of Hard Palate
C05.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C05.0 Hard palate */
 when (primary_site in ('C050') and histology between '8720' and '8790')
  then 'MelanomaPalateHard'

/* Malignant Melanoma of Maxillary Sinus
C31.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C31.0 Maxillary sinus
Note: Laterality must be coded for this site. */
 when (primary_site in ('C310') and histology between '8720' and '8790')
  then 'MelanomaSinusMaxillary'

/* Broad and Round Ligaments, Parametrium, Uterine Adnexa
C57.1-C57.4
C57.1  Broad ligament
C57.2  Round ligament
C57.3  Parametrium
C57.4  Uterine adnexa
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C571', 'C572', 'C573', 'C574')
  then 'AdnexaUterineOther'

/* Gallbladder
C23.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF15, SSF16
C23.9  Gallbladder */
 when primary_site in ('C239')
  then 'Gallbladder'

/* Malignant Melanoma of Soft Palate and Uvula
C05.1-C05.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C05.1 Soft palate, NOS
C05.2 Uvula
Note: Soft palate excludes nasopharyngeal (superior) surface of soft palate (C11.3) */
 when (primary_site in ('C051', 'C052') and histology between '8720' and '8790')
  then 'MelanomaPalateSoft'

/* Carcinomas of the Appendix (excluding Carcinoid Tumor and Neuroendocrine Carcinoma)
C18.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF7, SSF10, SSF12
M-8000-8152,8154-8231,8243-8245,8247,8248,8250-8934,8940-9136,9141-9582,9700-9701
C18.1 Appendix
Note: Carcinoid tumor and neuroendocrine carcinoma (histology codes 8153, 8240-8242, 8246, 8249) of the appendix are included in the "CarcinoidAppendix" schema. */
 when (primary_site in ('C181') and (histology between '8000' and '8152'
  or histology between '8154' and '8231'
  or histology between '8243' and '8245'
  or histology = '8247'
  or histology = '8248'
  or histology between '8250' and '8934'
  or histology between '8940' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'Appendix'

/* Vagina
C52.9
C52.9  Vagina, NOS
Note:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C529')
  then 'Vagina'

/* Merkel Cell Carcinoma of the Vulva
C51.0-C51.2, C51.8-C51.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF19, SSF20, SSF21
M-8247
C51.0  Labium majus
C51.1  Labium minus
C51.2  Clitoris
C51.8  Overlapping lesion of vulva
C51.9  Vulva, NOS
Note 1:  This schema is used for Merkel cell carcinoma only.
Note 2:  The AJCC 7th edition stages cancer of the perineum with vulva.  Cancer of the perineum can be coded to several different primary site codes each of which includes sites other than perineum.  Involvement of the vulva and perineum should be assigned to vulva as the primary site in the absence of a statement that the tumor extended from the perineum to the vulva.  Collaborative Stage only includes C51.0-C51.9 (vulva) and does not include primaries of the perineum in this schema. In addition, basal and squamous cell carcinomas of the skin of the perineum would be coded to C44.5 and would not be reportable. */
 when (primary_site in ('C510', 'C511', 'C512', 'C518', 'C519') and histology = '8247')
  then 'MerkelCellVulva'

/* Lymphoma of the Ocular Adnexa and Skin of Eyelid
C44.1, C69.0, C69.5-C69.6
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF5, SSF7, SSF8, SSF9, SSF10, SSF11, SSF12, SSF13
M-9590-9699,9702-9738, 9811-9818,9820-9837
C44.1  Skin of Eyelid
C69.0  Conjunctiva
C69.5  Lacrimal Gland
C69.6  Orbit
Note 1: Laterality must be coded for this site.
Note 2: Ocular lymphomas are assigned a stage grouping but no TNM values in AJCC 6th Edition staging.  Ocular lymphomas are assigned TNM values but no stage grouping in AJCC 7th Edition staging. */
 when (primary_site in ('C441', 'C690', 'C695', 'C696') and (histology between '9590' and '9699'
  or histology between '9702' and '9738'
  or histology between '9811' and '9818'
  or histology between '9820' and '9837'))
  then 'LymphomaOcularAdnexa'

/* Gastrointestinal Stromal Tumor of Small Intestine
C17.0-C17.3, C17.8-C17.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF9, SSF10
M-8935-8936
C17.0  Duodenum
C17.1  Jejunum
C17.2  Ileum (excludes ileocecal valve C18.0)
C17.3  Meckel diverticulum (site of neoplasm)
C17.8  Overlapping lesion of small intestine
C17.9  Small intestine, NOS
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when ((primary_site in ('C170', 'C171', 'C172', 'C173', 'C178', 'C179') and (not primary_site in ('C180'))) and histology between '8935' and '8936')
  then 'GISTSmallIntestine'

/* Lung
C34.0-C34.3, C34.8-C34.9
C34.0  Main bronchus
C34.1  Upper lobe, lung
C34.2  Middle lobe, lung
C34.3  Lower lobe, lung
C34.8  Overlapping lesion of lung
C34.9  Lung, NOS
Note:  Laterality must be coded for this site (except carina). */
 when primary_site in ('C340', 'C341', 'C342', 'C343', 'C348', 'C349')
  then 'Lung'

/* Malignant Melanoma of Subglottic Larynx
C32.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C32.2  Subglottis */
 when (primary_site in ('C322') and histology between '8720' and '8790')
  then 'MelanomaLarynxSubglottic'

/* Malignant Melanoma of Choroid
C69.3
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF14
M-8720-8790
C69.3  Choroid
Note:  Laterality must be coded for this site. */
 when (primary_site in ('C693') and histology between '8720' and '8790')
  then 'MelanomaChoroid'

/* Malignant Melanoma of Anterior Surface of Epiglottis
C10.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C10.1 Anterior surface of epiglottis */
 when (primary_site in ('C101') and histology between '8720' and '8790')
  then 'MelanomaEpiglottisAnterior'

/* Fallopian Tube
C57.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3
C57.0  Fallopian tube
Note 1:  Laterality must be coded for this site.
Note 2:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C570')
  then 'FallopianTube'

/* Neuroendocrine Tumors of Rectum and Rectosigmoid Junction
C19.9, C20.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF11
M-8153, 8240-8242, 8246, 8249
C19.9 Rectosigmoid junction
C20.9 Rectum, NOS
Note 1: For this schema, AJCC only stages well-differentiated neuroendocrine tumors. Note that the "concept" of well-differentiated is reflected in the histology code. The grade code is not needed in order to select the correct schema, but does need to be coded.
Note 2: This schema is also used for carcinoid tumors and malignant gastrinomas. */
 when (primary_site in ('C199', 'C209') and (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249'))
  then 'NETRectum'

/* Renal Pelvis and Ureter
C65.9, C66.9
C65.9  Renal pelvis
C66.9  Ureter
Note:  Laterality must be coded for this site. */
 when primary_site in ('C659', 'C669')
  then 'KidneyRenalPelvis'

/* Malignant Melanoma of Lower Lip
C00.1, C00.4, C00.6
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C00.1  External lower lip
C00.4  Mucosa of lower lip
C00.6  Commissure of lip */
 when (primary_site in ('C001', 'C004', 'C006') and histology between '8720' and '8790')
  then 'MelanomaLipLower'

/* Other and Ill-Defined Sites, Unknown Primary Site
C42.0-C42.4, C76.0-C76.5, C76.7-C76.8, C77.0-C77.5, C77.8-C77.9, C80.9
M-8000-9136,9141-9582,9700-9701
C42.0  Blood
C42.1  Bone marrow
C42.2  Spleen
C42.3  Reticuloendothelial system, NOS
C42.4  Hematopoietic system, NOS
C76.0  Head, face or neck, NOS
C76.1  Thorax, NOS
C76.2  Abdomen, NOS
C76.3  Pelvis, NOS
C76.4  Upper limb, NOS
C76.5  Lower limb, NOS
C76.7  Other ill-defined sites
C76.8  Overlapping lesion of ill-defined sites
C77.0  Lymph nodes of head, face and neck
C77.1  Lymph nodes of intrathoracic
C77.2  Lymph nodes of intra-abdominal
C77.3  Lymph nodes of axilla or arm
C77.4  Lymph nodes of inguinal region or leg
C77.5  Lymph nodes of pelvis
C77.8  Lymph nodes of multiple regions
C77.9  Lymph nodes, NOS
C80.9  Unknown primary site
Note 1:  AJCC does not define TNM staging for these sites.
Note 2: Excludes hematopoietic, reticuloendothelial, immunoproliferative and myeloproliferative neoplasms, Hodgkin and non-Hodgkin lymphomas, and Kaposi sarcoma. */
 when (primary_site in ('C420', 'C421', 'C422', 'C423', 'C424', 'C760', 'C761', 'C762', 'C763', 'C764', 'C765', 'C767', 'C768', 'C770', 'C771', 'C772', 'C773', 'C774', 'C775', 'C778', 'C779', 'C809') and (histology between '8000' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'IllDefinedOther'

/* Merkel Cell Carcinoma of the Penis
C60.0-C60.2, C60.8-C60.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF19, SSF20, SSF21
M-8247
C60.0  Prepuce
C60.1  Glans penis
C60.2  Body of penis
C60.8  Overlapping lesion of penis
C60.9  Penis, NOS
Note 1: This schema is used for Merkel cell carcinoma only. */
 when (primary_site in ('C600', 'C601', 'C602', 'C608', 'C609') and histology = '8247')
  then 'MerkelCellPenis'

/* Malignant Melanoma of Upper Lip
C00.0, C00.3
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C00.0  External upper lip
C00.3  Mucosa of upper lip */
 when (primary_site in ('C000', 'C003') and histology between '8720' and '8790')
  then 'MelanomaLipUpper'

/* Adenosarcoma of the Corpus Uteri; Uterus, NOS
C54.0-C54.3, C54.8-C54.9, C55.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
M-8933
C54.0  Isthmus uteri
C54.1  Endometrium
C54.2  Myometrium
C54.3  Fundus uteri
C54.8  Overlapping lesion of corpus uteri
C54.9  Corpus uteri
C55.9  Uterus, NOS
Note 1: AJCC 7th Edition TNM staging reflects the new staging adopted by the Federation Internationale de Gynecologie et d'Obstetrique (FIGO) and utilizes three new staging schemas for cancer of the Corpus Uteri based on histology. This is a change from the AJCC 6th Edition TNM staging.  The three new schemas are: 
1.   Carcinoma and carcinosarcoma 
2.   Leiomyosarcoma and endometrial stromal sarcoma (ESS) 
3.   Adenosarcoma 
Note 2: This schema is for adenosarcoma ONLY.  When coding pay attention to the FIGO Editions and descriptions about cell and tumor types, disease extension, lymph node status and metastasis.
Note 3:  AJCC TNM values correspond to the stages accepted by FIGO.  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when (primary_site in ('C540', 'C541', 'C542', 'C543', 'C548', 'C549', 'C559') and histology = '8933')
  then 'CorpusAdenosarcoma'

/*  Other and Unspecified Male Genital Organs (excluding Scrotum, Kaposi Sarcoma and Lymphoma)
C63.0-C63.1, C63.7-C63.9
C63.0  Epididymis
C63.1  Spermatic cord
C63.7  Other specified parts of male genital organs
C63.8  Overlapping lesion of male genital organs
C63.9  Male genital organs, NOS
Note 1:  AJCC does not define TNM staging for this site.
Note 2:  Laterality must be coded for C63.0-C63.1.
Note 3:  Melanoma, mycosis fungoides, or Sezary disease of all sites listed is coded using this schema. Kaposi sarcoma of all sites is included in the Kaposi sarcoma schema, and lymphomas of all sites are included in the lymphoma schema. */
 when primary_site in ('C630', 'C631', 'C637', 'C638', 'C639')
  then 'GenitalMaleOther'

/* Brain and Cerebral Meninges
C70.0, C71.0-C71.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF7, SSF8
C70.0  Cerebral meninges
C71.0  Cerebrum
C71.1  Frontal lobe
C71.2  Temporal lobe
C71.3  Parietal lobe
C71.4  Occipital lobe
C71.5  Ventricle, NOS
C71.6  Cerebellum, NOS
C71.7  Brain stem
C71.8  Overlapping lesion of brain
C71.9  Brain, NOS
Note 1: This schema is compatible with the AJCC 4th edition TNM scheme for brain, updated to include metastatic and site-specific information from the AJCC 7th edition. The AJCC opted not to recommend a TNM scheme in the 6th or 7th editions.
Note 2:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C700', 'C710', 'C711', 'C712', 'C713', 'C714', 'C715', 'C716', 'C717', 'C718', 'C719')
  then 'Brain'

/* Malignant Melanoma of Other Larynx
C32.3, C32.8-C32.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C32.3  Laryngeal cartilage
C32.8  Overlapping lesion of larynx
C32.9  Larynx, NOS */
 when (primary_site in ('C323', 'C328', 'C329') and histology between '8720' and '8790')
  then 'MelanomaLarynxOther'

/* Hard Palate (excluding Malignant Melanoma)
C05.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C05.0  Hard palate */
 when primary_site in ('C050')
  then 'PalateHard'

/* Placenta
C58.9
C58.9  Placenta
Note 1:  This schema correlates to the AJCC's Gestational Trophoblastic Tumors scheme.  In most cases, gestational trophoblastic tumors (ICD-O-3 morphology codes 9100-9105) are coded to placenta, C58.9.
Note 2:  If a trophoblastic tumor is not associated with a pregnancy and arises in another site, such as ovary, use the primary site code and Collaborative Staging schema for that site.
Note 3:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C589')
  then 'Placenta'

/* Other Parts of Central Nervous System
C70.1, C70.9, C72.0-C72.5, C72.8-C72.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF7, SSF8
C70.1  Spinal meninges
C70.9  Meninges, NOS
C72.0  Spinal cord
C72.1  Cauda equina
C72.2  Olfactory nerve
C72.3  Optic nerve
C72.4  Acoustic nerve
C72.5  Cranial nerve, NOS
C72.8  Overlapping lesion of brain and central nervous system
C72.9  Nervous system, NOS
Note 1: This schema is compatible with the AJCC fourth edition TNM for spinal cord. 
Note 2: AJCC does not define TNM staging for this site in the sixth or seventh editions. */
 when primary_site in ('C701', 'C709', 'C720', 'C721', 'C722', 'C723', 'C724', 'C725', 'C728', 'C729')
  then 'CNSOther'

/* Body and Tail of Pancreas
C25.1-C25.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF3
C25.1  Body of pancreas
C25.2  Tail of pancreas
Note:  For tumors of the islet cells, determine which subsite of the pancreas is involved and use that primary site code and corresponding Collaborative Stage schema.  If the subsite cannot be determined, use the general code for Islets of Langerhans, C25.4, and use the Collaborative Stage schema for Pancreas, Other and Unspecified. */
 when primary_site in ('C251', 'C252')
  then 'PancreasBodyTail'

/* Maxillary Sinus (excluding Malignant Melanoma)
C31.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C31.0  Maxillary sinus
Note:  Laterality must be coded for this site. */
 when primary_site in ('C310')
  then 'SinusMaxillary'

/* Mycosis Fungoides and Sezary Disease of Skin, Vulva, Penis, Scrotum
C44.0-C44.9, C51.0-C51.2, C51.8-C51.9, C60.0-C60.2, C60.8-C60.9, C63.2
M-9700-9701
C44.0  Skin of lip, NOS
C44.1  Eyelid
C44.2  External ear
C44.3  Skin of other and unspecified parts of face
C44.4  Skin of scalp and neck
C44.5  Skin of trunk
C44.6  Skin of upper limb and shoulder
C44.7  Skin of lower limb and hip
C44.8  Overlapping lesion of skin
C44.9  Skin, NOS
C51.0  Labium majus
C51.1  Labium minus
C51.2  Clitoris
C51.8  Overlapping lesion of vulva
C51.9  Vulva, NOS
C60.0  Prepuce
C60.1  Glans penis
C60.2  Body of penis
C60.8  Overlapping lesion of penis
C60.9  Penis
C63.2  Scrotum, NOS
Note:  Laterality must be coded for C44.1-C44.3 and C44.5-C44.7.  For codes C44.3 and C44.5, if the tumor is midline (e.g., chin), code 5 (midline) in the laterality field. */
 when (primary_site in ('C440', 'C441', 'C442', 'C443', 'C444', 'C445', 'C446', 'C447', 'C448', 'C449', 'C510', 'C511', 'C512', 'C518', 'C519', 'C600', 'C601', 'C602', 'C608', 'C609', 'C632') and histology between '9700' and '9701')
  then 'MycosisFungoides'

/* Other Mouth (excluding Malignant Melanoma)
C05.8-C05.9, C06.8-C06.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C05.8  Overlapping lesion of palate
C05.9  Palate, NOS
C06.8  Overlapping lesion of other and unspecified parts of mouth
C06.9  Mouth, NOS */
 when primary_site in ('C058', 'C059', 'C068', 'C069')
  then 'MouthOther'

/* Cervix Uteri
C53.0-C53.1, C53.8-C53.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4, SSF5, SSF6, SSF7, SSF8, SSF9
C53.0  Endocervix
C53.1  Exocervix
C53.8  Overlapping lesion of cervix
C53.9  Cervix uteri
Note:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C530', 'C531', 'C538', 'C539')
  then 'Cervix'

/* Lacrimal Sac (excluding Lymphoma)
C69.5
C69.5  Lacrimal sac, lacrimal duct  
Note 1:  CS Site-Specific Factor 25 is used to discriminate between lacrimal gland, staged by AJCC, and lacrimal sac, not staged by AJCC. Both sites are coded to ICD-O-3 code C69.5.
Note 2:  Laterality must be coded for this site. */
 when primary_site in ('C695')
  then 'LacrimalSac'

/* Urethra
C68.0
C68.0  Urethra
Note: Transitional cell carcinoma of the prostatic ducts and prostatic urethra are to be coded to urethra (C68.0) according to this schema. */
 when primary_site in ('C680')
  then 'Urethra'

/* Trachea
C33.9
C33.9  Trachea
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C339')
  then 'Trachea'

/* Malignant Melanoma of Upper Gum
C03.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C03.0 Upper gum */
 when (primary_site in ('C030') and histology between '8720' and '8790')
  then 'MelanomaGumUpper'

/* Malignant Melanoma of Other Eye (excluding Conjunctiva, Choroid, Ciliary Body, and Iris)
C69.1, C69.2, C69.5, C69.8-C69.9
M-8720-8790
C69.1  Cornea
C69.2  Retina
C69.5  Lacrimal gland
C69.8  Overlapping lesion of eye and adnexa
C69.9  Eye, NOS
Note 1:  Laterality must be coded for these sites
Note 2:  AJCC does not define TNM staging for these sites.
Note 3:  AJCC includes primary site C69.8 (overlapping lesions of eye and adnexa) in chapter 46, Sarcoma of the Orbit.  This schema includes only melanomas of the sites listed above. */
 when (primary_site in ('C691', 'C692', 'C695', 'C698', 'C699') and histology between '8720' and '8790')
  then 'MelanomaEyeOther'

/* Neuroendocrine Tumors of Ampulla of Vater
C24.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4
M-8153, 8240-8242, 8246, 8249
C24.1  Ampulla of Vater
Note 1:  For this schema, AJCC only stages well-differentiated neuroendocrine tumors.  Note that the "concept" of well-differentiated is reflected in the histology code.  The grade code is not needed in order to select the correct schema, but does need to be coded.
Note 2:  This schema is also used for carcinoid tumors and malignant gastrinomas. */
 when (primary_site in ('C241') and (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249'))
  then 'NETAmpulla'

/* Malignant Melanoma of Accessory (Paranasal) Sinuses
C31.2-C31.3, C31.8-C31.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C31.2 Frontal sinus
C31.3 Sphenoid sinus
C31.8 Overlapping lesion of accessory sinuses
C31.9 Accessory sinus, NOS
Note 1:  Laterality must  be coded for frontal sinus (C31.2).
Note 2:  AJCC does not define TNM staging for these sites and histologies. */
 when (primary_site in ('C312', 'C313', 'C318', 'C319') and histology between '8720' and '8790')
  then 'MelanomaSinusOther'

/* Carcinoma and Carcinosarcoma of Corpus Uteri; Uterus, NOS (excluding Placenta and Adenosarcoma, Leiomyosarcoma, and Endometrial Stromal Sarcoma (ESS))
C54.0-C54.3, C54.8-C54.9, C55.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
M-8000-8790, 8950, 8951, 8980-8981,9700-9701
C54.0  Isthmus uteri
C54.1  Endometrium
C54.2  Myometrium
C54.3  Fundus uteri
C54.8  Overlapping lesion of corpus uteri
C54.9  Corpus uteri
C55.9  Uterus, NOS
Note 1: AJCC 7th Edition TNM staging reflects the new staging adopted by the International Federation of Gynecology and Obstetrics (FIGO) and utilizes three new staging schemas for cancer of the Corpus Uteri based on histology.  This is a change from the AJCC 6th Edition TNM staging.  The three new schemas are: 
1.     Carcinoma and carcinosarcoma 
2.     Leiomyosarcoma and endometrial stromal sarcoma (ESS) 
3.     Adenosarcoma
Note 2: Carcinosarcoma should be staged as carcinoma.
Note 3: This schema is for carcinoma and carcinosarcoma.  When coding pay attention to the FIGO Editions and descriptions about cell and tumor types, disease extension, lymph node status and metastasis.
Note 4:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when (primary_site in ('C540', 'C541', 'C542', 'C543', 'C548', 'C549', 'C559') and (histology between '8000' and '8790'
  or histology = '8950'
  or histology = '8951'
  or histology between '8980' and '8981'
  or histology between '9700' and '9701'))
  then 'CorpusCarcinoma'

/* Neuroendocrine Tumors of Small Intestine
C17.0-C17.3, C17.8-C17.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF6
M-8153, 8240-8242, 8246, 8249
C17.0  Duodenum
C17.1  Jejunum
C17.2  Ileum (excludes ileocecal valve C18.0)
C17.3  Meckel diverticulum (site of neoplasm)
C17.8  Overlapping lesion of small intestine
C17.9  Small intestine, NOS
Note 1:  For this schema, AJCC only stages well-differentiated neuroendocrine tumors.  Note that the "concept" of well-differentiated is reflected in the histology code.  The grade code is not needed in order to select the correct schema, but does need to be coded.
Note 2: This schema is also used for carcinoid tumors and malignant gastrinomas. */
 when ((primary_site in ('C170', 'C171', 'C172', 'C173', 'C178', 'C179') and (not primary_site in ('C180'))) and (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249'))
  then 'NETSmallIntestine'

/* Other Lip (excluding Malignant Melanoma)
C00.2, C00.5, C00.8-C00.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C00.2  External lip, NOS (vermilion)
C00.5  Mucosa of lip, NOS
C00.8  Overlapping lesion of lip
C00.9  Lip, NOS (excludes skin of lip C44.0)
Note: AJCC includes labial mucosa (C00.5) with buccal mucosa (C06.0) */
 when (primary_site in ('C002', 'C005', 'C008', 'C009') and (not primary_site in ('C440')))
  then 'LipOther'

/* Perihilar Bile Ducts
C24.0 Extrahepatic bile duct
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF10, SSF12, SSF13, SSF14
C24.0 Extrahepatic bile duct
Note:  Extrahepatic Bile Duct was a single chapter in the AJCC 6th Edition. It has been divided into two chapters in the AJCC 7th Edition:  Perihilar Bile Ducts and Distal Bile Duct. */
 when primary_site in ('C240')
  then 'BileDuctsPerihilar'

/* Plasma Cell Disorders including Myeloma
None
9731 Plasmacytoma, NOS (except C441, C690, C695-C696)
9732 Multiple myeloma (except C441, C690, C695-C696)
9734 Plasmacytoma, extramedullary (except C441, C690, C695-C696)
Note 1: This schema was added in V0203. Originally these histologies were part of the HemeRetic schema.
Note 2: AJCC does not define TNM staging for this site. */
 when ((histology = '9731' and (not (primary_site = 'C441'
  or primary_site = 'C690'
  or primary_site between 'C695' and 'C696')))
  or (histology = '9732' and (not (primary_site = 'C441'
  or primary_site = 'C690'
  or primary_site between 'C695' and 'C696')))
  or (histology = '9734' and (not (primary_site = 'C441'
  or primary_site = 'C690'
  or primary_site between 'C695' and 'C696'))))
  then 'MyelomaPlasmaCellDisorder'

/* Skin of Eyelid
C44.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF4, SSF5, SSF7, SSF9, SSF11, SSF12, SSF13, SSF14, SSF15, SSF16
C44.1  Eyelid
Note:  Laterality must be coded for this site. */
 when primary_site in ('C441')
  then 'SkinEyelid'

/* Rectosigmoid, Rectum (excluding Gastrointestinal Stromal Tumor and Neuroendocrine Tumor)
C19.9, C20.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF5, SSF7, SSF10
M-8000-8152,8154-8231,8243-8245,8247,8248,8250-8934,8940-9136,9141-9582,9700-9701 
C19.9 Rectosigmoid junction
C20.9 Rectum, NOS */
 when (primary_site in ('C199', 'C209') and (histology between '8000' and '8152'
  or histology between '8154' and '8231'
  or histology between '8243' and '8245'
  or histology = '8247'
  or histology = '8248'
  or histology between '8250' and '8934'
  or histology between '8940' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'Rectum'

/* Middle Ear
C30.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C30.1  Middle ear
Note 1:  Laterality must be coded for this site.
Note 2:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C301')
  then 'MiddleEar'

/* Ovary
C56.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF5
C56.9  Ovary
Note 1:  Laterality must be coded for this site.
Note 2:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C569')
  then 'Ovary'

/* Cheek (Buccal) Mucosa, Vestibule (excluding Malignant Melanoma)
C06.0-C06.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C06.0  Cheek mucosa
C06.1  Vestibule of mouth */
 when primary_site in ('C060', 'C061')
  then 'BuccalMucosa'

/* Prostate
C61.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF14, SSF15
C61.9  Prostate gland
Note 1: Transitional cell (urothelial) carcinoma of the prostatic urethra is to be coded to primary site C68.0, Urethra, and assigned Collaborative Stage codes according to the Urethra schema.
Note 2: The 7th Edition AJCC stage group is derived not only from the T, N, and M categories but also from CS Site-Specific Factor 1 (PSA Lab Value) and CS Site-Specific Factor 8 or CS Site-Specific Factor 10 (Gleason's Score). The specific Gleason's Score used is dependent upon the values of CS Extension - Clinical Extension, CS Site-Specific Factor 3 (CS Extension - Pathologic Extension) and CS Tumor Size/Ext Eval as shown in the Special Calculation Table for TNM 7 Invasive/Unknown Pathologic Extension Eval and Special Calculation Table for TNM 7 Non-Invasive Pathologic Extension. */
 when primary_site in ('C619')
  then 'Prostate'

/* Other and Ill-Defined Respiratory Sites and Intrathoracic Organs
C39.0, C39.8-C39.9
C39.0  Upper respiratory tract, NOS
C39.8  Overlapping lesion of respiratory system and intrathoracic organs
C39.9  Ill-defined sites within respiratory system
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C390', 'C398', 'C399')
  then 'RespiratoryOther'

/* Malignant Melanoma of Conjunctiva
C69.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF3
M-8720-8790
C69.0  Conjunctiva
Note:  Laterality must be coded for this site. */
 when (primary_site in ('C690') and histology between '8720' and '8790')
  then 'MelanomaConjunctiva'

/* Merkel Cell Carcinoma of the Skin (excluding Merkel Cell Carcinoma of Penis, Scrotum, and Vulva)
C44.0, C44.2-C44.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF19, SSF20, SSF21
M-8247
C44.0  Skin of lip, NOS
C44.2  External ear
C44.3  Skin of ear and unspecified parts of face
C44.4  Skin of scalp and neck
C44.5  Skin of trunk
C44.6  Skin of upper limb and shoulder
C44.7  Skin of lower limb and hip
C44.8  Overlapping lesion of skin
C44.9  Skin, NOS
Note 1: This schema is NOT used for Merkel cell carcinoma of the vulva, penis, or scrotum. Each of these has a separate schema.
Note 2: Laterality must be coded for C44.2-C44.3 and C44.5-C44.7. For codes C44.3 and C44.5, if the tumor is midline (e.g., chin), code 5 (midline) in the laterality field.
Note 3: Merkel cell carcinoma presenting in nodal or visceral site with primary site unknown is coded to C44.9, Skin, NOS. */
 when (primary_site in ('C440', 'C442', 'C443', 'C444', 'C445', 'C446', 'C447', 'C448', 'C449') and histology = '8247')
  then 'MerkelCellSkin'

/* Soft Palate and Uvula (excluding Malignant Melanoma)
C05.1-C05.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C05.1  Soft palate, NOS
C05.2  Uvula
Note 1:  AJCC includes inferior surface of the soft palate (C05.1) and uvula (C05.2) with oropharynx (C09._, C10._).
Note 2:  Soft palate excludes nasopharyngeal (superior) surface of soft palate (C11.3). */
 when primary_site in ('C051', 'C052')
  then 'PalateSoft'

/* Kidney (Renal Parenchyma)
C64.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF5, SSF7
C64.9  Kidney, NOS (Renal parenchyma)
Note:  Laterality must be coded for this site. */
 when primary_site in ('C649')
  then 'KidneyParenchyma'

/* Bone
C40.0-C40.3, C40.8-C40.9, C41.0-C41.4, C41.8-C41.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF4
C40.0  Long bones of upper limb, scapula and associated joints
C40.1  Short bones of upper limb and associated joints
C40.2  Long bones of lower limb and associated joints
C40.3  Short bones of lower limb and associated joints
C40.8  Overlapping lesion of bones, joints and articular cartilage of limbs
C40.9  Bone of limb, NOS
C41.0  Bones of skull and face and associated joints (excludes mandible C41.1)
C41.1  Mandible
C41.2  Vertebral column (excludes sacrum and coccyx C41.4)
C41.3  Rib, sternum, clavicle and associated joints
C41.4  Pelvic bones, sacrum, coccyx and associated joints
C41.8  Overlapping lesion of bones, joints and articular cartilage
C41.9  Bone, NOS
Note:  Laterality must be coded for C40.0-C40.3, and C41.3-C41.4.  For sternum, sacrum, coccyx, and symphysis pubis, laterality is coded 0.
Note:  The determination of AJCC stage group from T, N, M, and grade for bone also depends on histologic type. The Histologies Stage Table shows the selection of the AJCC Stage table based on histology. */
 when (primary_site in ('C400', 'C401', 'C402', 'C403', 'C408', 'C409', 'C410', 'C411', 'C412', 'C413', 'C414', 'C418', 'C419') and (not primary_site in ('C411', 'C414')))
  then 'Bone'

/* Bladder
C67.0-C67.9
C67.0  Trigone of bladder
C67.1  Dome of bladder
C67.2  Lateral wall of bladder
C67.3  Anterior wall of bladder
C67.4  Posterior wall of bladder
C67.5  Bladder neck
C67.6  Ureteric orifice
C67.7  Urachus
C67.8  Overlapping lesion of bladder
C67.9  Bladder, NOS */
 when primary_site in ('C670', 'C671', 'C672', 'C673', 'C674', 'C675', 'C676', 'C677', 'C678', 'C679')
  then 'Bladder'

/* Adrenal Gland
C74.0-C74.1, C74.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3
C74.0  Cortex of adrenal gland
C74.1  Medulla of adrenal gland
C74.9  Adrenal gland, NOS
Note 1:  Laterality must be coded for this site.
Note 2:  Except for histologies that have strictly histology-based CS schemas (for example lymphoma), all cases with primary site adrenal gland (C74._) are coded with this schema. However, only adrenal cortical carcinomas will have AJCC stage derived (7th Edition only). Adrenal cortical carcinoma is identified as C74.0 (adrenal cortex) with histology 8010, 8140, or 8370 OR C74.9 (adrenal gland, NOS) with histology 8370. */
 when primary_site in ('C740', 'C741', 'C749')
  then 'AdrenalGland'

/* Tonsil and Oropharynx (excluding Malignant Melanoma)
C09.0-C09.1, C09.8-C09.9, C10.0, C10.2-C10.4, C10.8-C10.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C09.0  Tonsillar fossa
C09.1  Tonsillar pillar
C09.8  Overlapping lesion of tonsil
C09.9  Tonsil, NOS (excluding lingual tonsil C02.4)
C10.0  Vallecula
C10.2  Lateral wall of oropharynx
C10.3  Posterior wall of oropharynx
C10.4  Branchial cleft (site of neoplasm)
C10.8  Overlapping lesion of oropharynx
C10.9  Oropharynx, NOS
Note 1:  Laterality must be coded for C09.0, C09.1, C09.8, and C09.9.
Note 2:  AJCC includes base of tongue (C01.9) with oropharynx (C09._, C10._). */
 when (primary_site in ('C090', 'C091', 'C098', 'C099', 'C100', 'C102', 'C103', 'C104', 'C108', 'C109') and (not primary_site in ('C024')))
  then 'Oropharynx'

/* Head of Pancreas
C25.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF3
C25.0  Head of pancreas
Note:  For tumors of the islet cells, determine which subsite of the pancreas is involved and use that primary site code and the corresponding Collaborative Stage schema.  If the subsite cannot be determined, use the general code for Islets of Langerhans, C25.4, and use the Collaborative Stage schema for Pancreas, Other and Unspecified. */
 when primary_site in ('C250')
  then 'PancreasHead'

/* Malignant Melanoma of Ethmoid Sinus
C31.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C31.1 Ethmoid sinus */
 when (primary_site in ('C311') and histology between '8720' and '8790')
  then 'MelanomaSinusEthmoid'

/* Thyroid Gland
C73.9
C73.9  Thyroid gland
Note: The determination of AJCC stage group from T, N, and M for thyroid depends on histologic type, grade, and age. The Histologies, Grade, Stage table shows the selection of the AJCC Stage table based on histology and grade. For papillary and follicular carcinomas, age is also needed for the selection; if age at diagnosis is unknown, AJCC stage will be derived as unknown for these histologies. */
 when primary_site in ('C739')
  then 'Thyroid'

/* Peritoneum (excluding Gastrointestinal Stromal Tumors and Peritoneum Female Genital M-8000-8576, 8590-8671, 8930-8934, 8940-9110 for females)
C48.1-C48.2, C48.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4
M-8000-8934,8940-9136,9141-9582,9700-9701 for males
M-8580-8589,8680-8921,9120-9136,9141-9582,9700-9701 for females
C48.1 Specified parts of peritoneum (including omentum and mesentery)
C48.2 Peritoneum, NOS
C48.8 Overlapping lesion of retroperitoneum and peritoneum
Note:  AJCC includes these sites with soft tissue sarcomas (C47.0-C48.9) */
 when (primary_site in ('C481', 'C482', 'C488') and ((histology between '8000' and '8934'
  or histology between '8940' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701')
  or (histology between '8580' and '8589'
  or histology between '8680' and '8921'
  or histology between '9120' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701')))
  then 'Peritoneum'

/* Nasal Cavity (excluding Malignant Melanoma)
C30.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C30.0  Nasal cavity (excluding nose, NOS C76.0)
Note:  Laterality must be coded for this site, except subsites nasal cartilage and nasal septum, for which laterality is coded 0. */
 when (primary_site in ('C300') and (not primary_site in ('C760')))
  then 'NasalCavity'

/* Ampulla of Vater (excluding Neuroendocrine Tumor)
C24.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF3
C24.1  Ampulla of Vater */
 when primary_site in ('C241')
  then 'AmpullaVater'

/* Malignant Melanoma of Base of Tongue and Lingual Tonsil
C01.9, C02.4
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C01.9 Base of tongue, NOS
C02.4 Lingual tonsil */
 when (primary_site in ('C019', 'C024') and histology between '8720' and '8790')
  then 'MelanomaTongueBase'

/* Submandibular Gland
C08.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C08.0  Submandibular gland
Note:  Laterality must be coded for C08.0. */
 when primary_site in ('C080')
  then 'SubmandibularGland'

/* Gastrointestinal Stromal Tumors of Peritoneum and Retroperitoneum
C48.0-C48.2, C48.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF9
M-8935-8936
C48.0 Retroperitioneum
C48.1 Specified parts of peritoneum (including omentum and mesentery)
C48.2 Peritoneum, NOS
C48.8 Overlapping lesion of retroperitoneum and peritoneum */
 when (primary_site in ('C480', 'C481', 'C482', 'C488') and histology between '8935' and '8936')
  then 'GISTPeritoneum'

/* Malignant Melanoma of Nasal Cavity
C30.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C30.0 Nasal cavity (excludes nose, NOS C76.0)
Note:  Laterality must be coded for this site, except subsites nasal cartilage and nasal septum, for which laterality is coded 0. */
 when ((primary_site in ('C300') and (not primary_site in ('C760'))) and histology between '8720' and '8790')
  then 'MelanomaNasalCavity'

/* Esophagus
C15.0-C15.5, C15.8-C15.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4, SSF5
C15.0  Cervical esophagus
C15.1  Thoracic esophagus
C15.2  Abdominal esophagus
C15.3  Upper third of esophagus
C15.4  Middle third of esophagus
C15.5  Lower third of esophagus
C15.8  Overlapping lesion of esophagus
C15.9  Esophagus, NOS
Note 1:  The cardia/gastroesophageal junction (EGJ), and the proximal 5 centimeters (cm) of the fundus and body of the stomach (C16.0-C16.2) have been removed from the Stomach chapter and added to the Esophagus chapter effective with AJCC 7th Edition. A new schema EsophagusGEJunction was created in CSv2 to accommodate this change. Tumors arising at the EGJ, or arising in the stomach within 5 cm of the EGJ and crossing the EGJ are staged using the EsophagusGEJunction schema. All other cancers with a midpoint in the stomach lying more than 5 cm distal to the EGJ, or those within 5 cm of the EGJ but not extending into the EGJ or esophagus, are staged using the stomach schema.
Note 2:  There are two widely used but incompatible systems of dividing the esophagus into subsites, one using anatomic landmarks and the other using using thirds of the total length.  Each of these two systems has been assigned topography codes in ICD-O-3; codes C15.0-C15.2 for the former, and C15.3-C15.5 for the latter.  As explained on page 23 of ICD-O-3, "The terms cervical, thoracic, and abdominal are radiographic and intraoperative descriptors; upper, middle, and lower third are endoscopic and clinical descriptors."  In actual practice by physicians, and in publications of UICC and AJCC, the terms and codes for the upper, middle, and lower thirds are often applied to sub-sections of the thoracic esophagus, and the abdominal portion can be considered part of the lower thoracic esophagus. 
Note 3:  Anatomic Limits of Esophagus: 
            Cervical Esophagus (C15.0):  From the lower border of the cricoid cartilage to the thoracic inlet (suprasternal notch), about 18 cm from the incisors.
            Thoracic  Esophagus (C15.1) and Abdominal Esophagus (C15.2): 
                        Upper thoracic portion (C15.3): From the thoracic inlet to the level of the tracheal bifurcation (18-24 cm). 
                        Mid-thoracic portion (C15.4): From the tracheal bifurcation midway to the GEJ (24-32 cm).
                        Lower thoracic portion (C15.5): From midway between the tracheal bifurcation and the EGJ to the EGJ including the abdominal esophagus (32-40 cm). 
Note 4:  Effective with AJCC TNM 7th Edition, there are separate stage groupings for squamous cell carcinoma and adenocarcinoma. Since squamous cell carcinoma typically has a poorer prognosis than adenocarcinoma, a tumor of mixed histopathologic type or a type that is not otherwise specified should be classified as squamous cell carcinoma. 
Note 5:  Effective with AJCC TNM 7th Edition, histologic grade is required for stage grouping.  */
 when primary_site in ('C150', 'C151', 'C152', 'C153', 'C154', 'C155', 'C158', 'C159')
  then 'Esophagus'

/* Scrotum (excluding Malignant Melanoma, Merkel Cell Carcinoma, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease, and Other Lymphomas)
C63.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF10, SSF11
C63.2  Scrotum, NOS
Note: Melanoma (M-8720-8790) of scrotum is included in the Melanoma schema. Merkel cell carcinoma (M-8247) of the scrotum is included in the MerkelCellScrotum schema. Mycosis Fungoides (M-9700) or Sezary disease (M-9701) of scrotum is included in the MycosisFungoides schema. Kaposi sarcoma (M-9140) of the scrotum is included in the KaposiSarcoma schema. Lymphoma of the scrotum is included in the Lymphoma schema. */
 when primary_site in ('C632')
  then 'Scrotum'

/* Other and Ill-Defined Digestive Organs
C26.0, C26.8-C26.9
C26.0  Intestinal tract, NOS
C26.8  Overlapping lesion of digestive system
C26.9  Gastrointestinal tract, NOS
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C260', 'C268', 'C269')
  then 'DigestiveOther'

/* Gum, Upper (excluding Malignant Melanoma)
C03.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C03.0  Upper gum */
 when primary_site in ('C030')
  then 'GumUpper'

/* Kaposi Sarcoma of All Sites
None
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4
M-9140
Note:  This scheme cannot be compared to either the Historic Stage or the 1977 Summary Stage schemes. */
 when histology = '9140'
  then 'KaposiSarcoma'

/* Conjunctiva (excluding Retinoblastoma, Malignant Melanoma, Kaposi Sarcoma, and Lymphoma)
C69.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2
C69.0  Conjunctiva
Note: Laterality must be coded for this site. */
 when primary_site in ('C690')
  then 'Conjunctiva'

/* Glottic Larynx (excluding Malignant Melanoma)
C32.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C32.0  Glottis */
 when primary_site in ('C320')
  then 'LarynxGlottic'

/* Hematopoietic, Reticuloendothelial, Immunoproliferative, and Myeloproliferative Neoplasms
None
M-See list of specific histologies below. All primary sites (C00.0-C80.9) are included unless otherwise specified. 
Schema includes only preferred terms from ICD-O-3.
Plasmacytomas (9731 and 9734) and Multiple Myeloma (9732), except for cases with primary site C441, C690 and C695-C696, have been moved to the MyelomaPlasmaCellDisorder schema in V0203
9733     Plasma cell leukemia [except C441, C690, C695-C696]
9740     Mast cell sarcoma
9741     Malignant mastocytosis
9742     Mast cell leukemia
9750     Malignant histiocytosis
9752     Langerhans cell histiocytosis, unifocal* (see new reportable code 9751/3)
9753     Langerhans cell histiocytosis, multifocal* (see new reportable code 9751/3)
9754     Langerhans cell histiocytosis disseminated
9755     Histiocytic sarcoma
9756     Langerhans cell sarcoma
9757     Interdigitating dendritic cell sarcoma
9758     Follicular dendritic cell sarcoma
9760     Immunoproliferative disease, NOS
9761     Waldenstrom macroglobulinemia
9762     Heavy chain disease, NOS
9764     Immunoproliferative small intestinal disease
9765     Monoclonal gammopathy of undetermined significance*
9766     Angiocentric immunoproliferative lesion*
9767     Angioimmunoblastic lymphadenopathy*
9768     T-gamma lymphoproliferative disease*
9769     Immunoglobulin deposition disease*
9800     Leukemia, NOS
9801     Acute leukemia, NOS
9805     Acute biphenotypic leukemia
9820     Lymphoid leukemia, NOS [except C441, C690, C695-C696]
9823     B-cell chronic lymphocytic leukemia/small lymphocytic lymphoma [C420, C421, or C424 ONLY]
9826     Burkitt cell leukemia leukemia [except C441, C690, C695-C696]
9827     Adult T-cell leukemia/lymphoma (HTLV-1 positive)[C420, C421, or C424 ONLY]
9832     Prolymphocytic leukemia, NOS [except C441, C690, C695-C696]
9833     Prolymphocytic leukemia, B-cell type [except C441, C690, C695-C696]
9834     Prolymphocytic leukemia, T-cell type [except C441, C690, C695-C696]
9835     Precursor cell lymphoblastic leukemia, NOS [except C441, C690, C695-C696]
9836     Precursor B-cell lymphoblastic leukemia [except C441, C690, C695-C696]
9837     Precursor T-cell lymphoblastic leukemia [see 9837 below, new definition]
9840     Acute myeloid leukemia, M6 type
9860     Myeloid leukemia, NOS
9861     Acute myeloid leukemia, NOS
9863     Chronic myeloid leukemia
9866     Acute promyelocytic leukemia
9867     Acute myelomonocytic leukemia
9870     Acute basophilic leukemia
9871     Acute myeloid leukemia with abnormal marrow, eosinophils
9872     Acute myeloid leukemia, minimal differentiation
9873     Acute myeloid leukemia without maturation
9874     Acute myeloid leukemia with maturation
9875     Chronic myelogenous leukemia, BCR/ABL positive
9876     Atypical chronic myeloid leukemia BCR/ABL negative
9891     Acute monocytic leukemia
9895     Acute myeloid leukemia with multilineage dysplasia
9896     Acute myeloid leukemia, t(8;21)(q22;q22)
9897     Acute myeloid leukemia, 11q23 abnormalities
9910     Acute megakaryoblastic leukemia
9920     Therapy-related acute myeloid leukemia, NOS
9930     Myeloid sarcoma
9931     Acute panmyelosis with myelofibrosis
9940     Hairy cell leukemia
9945     Chronic myelomonocytic leukemia, NOS
9946     Juvenile myelomonocytic leukemia
9948     Aggressive NK-cell leukemia
9950     Polycythemia (rubra) vera
9960     Chronic myeloproliferative disease, NOS
9961     Myelosclerosis with myeloid metaplasia
9962     Essential thrombocythemia
9963     Chronic neutrophilic leukemia
9964     Hypereosinophilic syndrome
9970     Lymphoproliferative disorder, NOS*
9975     Myeloproliferative disease, NOS*
9980     Refractory anemia, NOS
9982     Refractory anemia with sideroblasts
9983     Refractory anemia with excess blasts
9984     Refractory anemia with excess blasts in transformation
9985     Refractory cytopenia with multilineage dysplasia
9986     Myelodysplastic syndrome with 5q deletion (5q-) syndrome
9987     Therapy-related myelodysplastic syndrome, NOS
9989     Myelodysplastic syndrome, NOS
The following ICD-O codes were added to the reportable list for Hematopoietic diseases. These are from the "WHO Classification of Tumours of Haematopoietic and Lymphoid Tissues, 3rd edition" publication, which was released in 2008. These new codes have been incorporated into the new Hematopoietic and Lymphoid Neoplasm MP/H rules. Use these only for cases diagnosed on January 1, 2010 and forward.
9751        Langerhans cell histiocytosis, NOS
9806     Mixed phenotype acute leukemia with t(9;22(q34;q11.2); BCR-ABL1
9807     Mixed phenotype acute leukemia with t(v;11q23); MLL, rearranged
9808     Mixed phenotype acute leukemia, B/myeloid, NOS
9809     Mixed phenotype acute leukemia, T/myeloid, NOS
9811     B lymphoblastic leukemia/lymphoma, NOS [C420, C421, or C424 ONLY]
9812     B lymphoblastic leukemia/lymphoma with t(9;22))q34;q11.2); BCR-ABL1 [C420, C421, or C424 ONLY]
9813     B lymphoblastic leukemia/lymphoma with t(v;11q23); MLL rearranged [C420, C421, or C424 ONLY]
9814     B lymphoblastic leukemia/lymphoma with t(12;21)(p13;q22); TEL-AML1 (ETV6-RUNX1)  [C420, C421, or C424 ONLY]
9815     B lymphoblastic/lymphoma with hyperdiploidy[C420, C421, or C424 ONLY]
9816     B lymphoblastic/lymphoma with hypodiploidy (hypodiploid ALL)  [C420, C421, or C424 ONLY]
9817     B lymphoblastic/lymphoma with t(5;14)(q31;q32); IL3-IGH [C420, C421, or C424 ONLY]
9818     B lymphoblastic/lymphoma with t(1;19)(q23;p13.3); E2A PBX1 (TCF3 PBX1) [C420, C421, or C424 ONLY]
9831     T-cell large granular lymphocytic leukemia [except C441, C690, C695-C696]
9837     T lymphoblastic leukemia/lymphoma [C420, C421, or C424 ONLY]
9865     Acute myeloid leukemia with t(6;9)([23;q34) DEK-NUP214
9869     Acute myeloid leukemia with inv(3)(q21q26.2) or t(3;3)(q21;q26;2); RPN1EVI1
9898     Myeloid leukemia associated with Down Syndrome
9911     Acute myeloid leukemia (megakaryoblastic) with t(1;22)(p13;q13); RBM15-MKL1
9965     Myeloid and lymphoid neoplasms with PDGFRA rearrangement
9966     Myeloid neoplasm with PDGFRB rearrangement
9967     Myeloid and lymphoid neoplasm with FGFR1 abnormalities
9971     Polymorphic PTLD
9975     Myeloproliferative neoplasm, unclassifiable
9991     Refractory neutropenia
9992     Refractory thrombocytopenia
*Usually considered of uncertain/borderline behavior
Note:  AJCC does not define TNM staging for this site. */
 /* SKIPPED HemeRetic */

/* Anterior Surface of Epiglottis (excluding Malignant Melanoma)
C10.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C10.1  Anterior surface of epiglottis
Note:  AJCC includes lingual (anterior) surface of epiglottis (C10.1) with larynx.  SEER Extent of Disease included it with oropharynx. */
 when primary_site in ('C101')
  then 'EpiglottisAnterior'

/* Stomach (excluding Gastrointestinal Stromal Tumor and Neuroendocrine Tumor)
C16.1-C16.6, C16.8-C16.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF13, SSF14, SSF15
C16.1  Fundus of stomach
C16.2  Body of stomach
C16.3  Gastric antrum
C16.4  Pylorus
C16.5  Lesser curvature of stomach, NOS
C16.6  Greater curvature of stomach, NOS
C16.8  Overlapping lesion of stomach
C16.9  Stomach, NOS */
 when primary_site in ('C161', 'C162', 'C163', 'C164', 'C165', 'C166', 'C168', 'C169')
  then 'Stomach'

/* Other and Unspecified Pancreas
C25.3-C25.4, C25.7-C25.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF3
C25.3  Pancreatic duct
C25.4  Islets of Langerhans
C25.7  Other specified parts of pancreas
C25.8  Overlapping lesion of pancreas
C25.9  Pancreas, NOS
Note:  For tumors of the islet cells, determine which subsite of the pancreas is involved and use that primary site code and the corresponding Collaborative Stage schema.  If the subsite cannot be determined, use the general code for Islets of Langerhans, C25.4, and use the Collaborative Stage schema for Pancreas, Other and Unspecified. */
 when primary_site in ('C253', 'C254', 'C257', 'C258', 'C259')
  then 'PancreasOther'

/* Sarcoma (Leiomyosarcoma and Endometrial Stromal Sarcoma) of the Corpus Uteri; Uterus, NOS (excluding Placenta and Adenosarcoma, Carcinoma, and Carcinosaroma)
C54.0-C54.3, C54.8-C54.9, C55.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
M-8800-8932,8934-8941,8959-8974, 8982-9136,9141-9582 
C54.0  Isthmus uteri
C54.1  Endometrium
C54.2  Myometrium
C54.3  Fundus uteri
C54.8  Overlapping lesion of corpus uteri
C54.9  Corpus uteri
C55.9  Uterus, NOS
Note1:  AJCC 7th Edition TNM staging reflects the new staging adopted by the Federation Internationale de Gynecologie et d'Obstetrique (FIGO) and utilizes three new staging schemas for cancer of the Corpus Uteri based on histology.  This is a change from the AJCC 6th Edition TNM staging.  The three new schemas are: 

1.     Carcinoma and carcinosarcoma
2.     Leiomyosarcoma and endometrial stromal sarcoma (ESS)
3.     Adenosarcoma
Note 2:  This schema is for leiomyosarcoma and endometrial stromal sarcoma (ESS). 
Note 3:  When coding pay attention to the FIGO Editions and descriptions about cell and tumor types, disease extension, lymph node status and metastasis.
Note 4:  AJCC TNM values correspond to the stages accepted by FIGO.  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when (primary_site in ('C540', 'C541', 'C542', 'C543', 'C548', 'C549', 'C559') and (histology between '8800' and '8932'
  or histology between '8934' and '8941'
  or histology between '8959' and '8974'
  or histology between '8982' and '9136'
  or histology between '9141' and '9582'))
  then 'CorpusSarcoma'

/* Lip, Lower (excluding Malignant Melanoma)
C00.1, C00.4, C00.6
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C00.1  External lower lip (vermilion)
C00.4  Mucosa of lower lip
C00.6  Commissure of lip */
 when primary_site in ('C001', 'C004', 'C006')
  then 'LipLower'

/* Heart, Mediastinum
C38.0-C38.3, C38.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF4
C38.0  Heart
C38.1  Anterior mediastinum
C38.2  Posterior mediastinum
C38.3  Mediastinum, NOS
C38.8  Overlapping lesion of heart, mediastinum and pleura */
 when primary_site in ('C380', 'C381', 'C382', 'C383', 'C388')
  then 'HeartMediastinum'

/* Malignant Melanoma of Supraglottic Larynx
C32.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C32.1  Supraglottis */
 when (primary_site in ('C321') and histology between '8720' and '8790')
  then 'MelanomaLarynxSupraglottic'

/* Gastrointestinal Stromal Tumor of Stomach
C16.0-C16.6, C16.8-C16.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF9, SSF10
M-8935-8936
C16.0 Cardia of stomach
C16.1  Fundus of stomach
C16.2  Body of stomach
C16.3  Gastric antrum
C16.4  Pylorus
C16.5  Lesser curvature of stomach, NOS
C16.6  Greater curvature of stomach, NOS
C16.8  Overlapping lesion of stomach
C16.9  Stomach, NOS
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when (primary_site in ('C160', 'C161', 'C162', 'C163', 'C164', 'C165', 'C166', 'C168', 'C169') and histology between '8935' and '8936')
  then 'GISTStomach'

/* Gum, Lower and Retromolar Area (excluding Malignant Melanoma)
C03.1, C06.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C03.1  Lower gum
C06.2  Retromolar area (gingiva, trigone) */
 when primary_site in ('C031', 'C062')
  then 'GumLower'

/* Malignant Melanoma of Anterior Tongue
C02.0-C02.3, C02.8-C02.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C02.0 Dorsal surface of tongue, NOS
C02.1 Border of tongue
C02.2 Ventral surface of tongue, NOS
C02.3 Anterior 2/3 of tongue, NOS
C02.8 Overlapping lesion of tongue
C02.9 Tongue, NOS */
 when (primary_site in ('C020', 'C021', 'C022', 'C023', 'C028', 'C029') and histology between '8720' and '8790')
  then 'MelanomaTongueAnterior'

/* Malignant Melanoma of Pharynx, NOS and Overlapping Lesions of Lip, Oral Cavity, and Pharynx
C14.0, C14.2, C14.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C14.0 Pharynx, NOS
C14.2 Waldeyer ring
C14.8 Overlapping lesion of lip, oral cavity, and pharynx */
 when (primary_site in ('C140', 'C142', 'C148') and histology between '8720' and '8790')
  then 'MelanomaPharynxOther'

/* Gastrointestinal Stromal Tumors of Colon (excluding Appendix)
C18.0, C18.2-C18.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF13, SSF14, SSF15
M-8935-8936
C18.0 Cecum
C18.2 Ascending colon
C18.3 Hepatic flexure of colon
C18.4 Transverse colon
C18.5 Splenic flexure of colon
C18.6 Descending colon
C18.7 Sigmoid colon
C18.8 Overlapping lesion of colon
C18.9 Colon, NOS
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when (primary_site in ('C180', 'C182', 'C183', 'C184', 'C185', 'C186', 'C187', 'C188', 'C189') and histology between '8935' and '8936')
  then 'GISTColon'

/* Thymus and Other Endocrine Glands (excluding Adrenal Gland, Pituitary Gland, Craniopharyngeal Duct, and Pineal Gland
C37.9, C75.0, C75.4-C75.5, C75.8-C75.9
C37.9  Thymus
C75.0  Parathyroid gland
C75.4  Carotid body
C75.5  Aortic body and other paraganglia
C75.8  Overlapping lesion of endocrine glands and related structures
C75.9  Endocrine gland, NOS
Note 1:  Laterality must be coded for C75.4.
Note 2:  AJCC does not define TNM staging for these sites.
Note 3:  Adrenal gland primaries (C74.0, C74.1, and C74.9) are included in the Adrenal Gland schema.
Note 4:  Pituitary gland (C75.1), craniopharyngeal duct (C75.2), and pineal gland (C75.3) primaries are included in the Intracranial Gland schema. */
 when primary_site in ('C379', 'C750', 'C754', 'C755', 'C758', 'C759')
  then 'EndocrineOther'

/* Lacrimal Gland (excluding Lymphoma)
C69.5
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF1, SSF2, SSF3, SSF5, SSF7
C69.5  Lacrimal gland [excluding lacrimal sac, lacrimal duct] 
Note 1:  CS Site-Specific Factor 25 is used to discriminate between lacrimal gland, staged by AJCC, and lacrimal sac, not staged by AJCC. Both sites are coded to ICD-O-3 code C69.5.
Note 2:  Laterality must be coded for this site. */
 when primary_site in ('C695')
  then 'LacrimalGland'

/* Malignant Melanoma of Lower Gum and Retromolar Area
C03.1, C06.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C03.1 Lower gum
C06.2 Retromolar area */
 when (primary_site in ('C031', 'C062') and histology between '8720' and '8790')
  then 'MelanomaGumLower'

/* Malignant Melanoma of Buccal Mucosa
C06.0-C06.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C06.0 Cheek mucosa
C06.1 Vestibule of mouth */
 when (primary_site in ('C060', 'C061') and histology between '8720' and '8790')
  then 'MelanomaBuccalMucosa'

/* Ethmoid Sinus (excluding Malignant Melanoma)
C31.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C31.1  Ethmoid sinus */
 when primary_site in ('C311')
  then 'SinusEthmoid'

/* Malignant Melanoma of Tonsil and Oropharynx
C09.0-C09.1, C09.8-C09.9, C10.0, C10.2-C10.4, C10.8-C10.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C09.0 Tonsillar fossa
C09.1 Tonsillar pillar
C09.8 Overlapping lesion of tonsil
C09.9 Tonsil, NOS (excluding lingual tonsil C02.4)
C10.0 Vallecula
C10.2 Lateral wall of oropharynx
C10.3 Posterior wall of oropharynx
C10.4 Branchial cleft (site of neoplasm)
C10.8 Overlapping lesion of oropharynx
C10.9 Oropharynx, NOS
Note: Laterality must be coded for C09.0, C09.1, C09.8, and C09.9 */
 when ((primary_site in ('C090', 'C091', 'C098', 'C099', 'C100', 'C102', 'C103', 'C104', 'C108', 'C109') and (not primary_site in ('C024'))) and histology between '8720' and '8790')
  then 'MelanomaOropharynx'

/* Neuroendocrine Tumors of Colon (excluding Appendix)
C18.0, C18.2-C18.9 
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF11
M-8153, 8240-8242, 8246, 8249
C18.0 Cecum
C18.2 Ascending colon
C18.3 Hepatic flexure of colon
C18.4 Transverse colon
C18.5 Splenic flexure of colon
C18.6 Descending colon
C18.7 Sigmoid colon
C18.8 Overlapping lesion of colon
C18.9 Colon, NOS
Note 1: For this schema, AJCC only stages well-differentiated neuroendocrine tumors. Note that the "concept" of well-differentiated is reflected in the histology code. The grade code is not needed in order to select the correct schema, but does need to be coded.
Note 2: This schema is also used for carcinoid tumors and malignant gastrinomas. */
 when (primary_site in ('C180', 'C182', 'C183', 'C184', 'C185', 'C186', 'C187', 'C188', 'C189') and (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249'))
  then 'NETColon'

/* Malignant Melanoma of Other Mouth
C05.8-C05.9, C06.8-C06.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C05.8 Overlapping lesion of palate
C05.9 Palate, NOS
C06.8 Overlapping lesion of other and unspecified parts of mouth
C06.9 Mouth, NOS */
 when (primary_site in ('C058', 'C059', 'C068', 'C069') and histology between '8720' and '8790')
  then 'MelanomaMouthOther'

/* Carcinoid Tumor and Neuroendocrine Carcinoma of Appendix
C18.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF13
M-8153, 8240-8242, 8246, 8249
Note 1: Carcinoid tumor of the appendix is typically not reportable.  Use this schema if your institution collects this tumor as reportable by agreement.
Note 2: This schema is also used for neurendocrine carcinoma and malignant gastrinomas.
Note 3: Not all histologies included in this schema were staged in AJCC 6th Edition.  The algorithm will derive an AJCC 6 TNM and stage group only for histology codes 8153 and 8246. */
 when (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249')
  then 'CarcinoidAppendix'

/* Gastrointestinal Stromal Tumor of Appendix
C18.1 Appendix
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF13, SSF14, SSF15
M- 8935-8936
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when histology between '8935' and '8936'
  then 'GISTAppendix'

/* Retinoblastoma
C69.0-C69.6, C69.8-C69.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4, SSF5, SSF6
M-9510-9514
C69.0  Conjunctiva
C69.1  Cornea, NOS
C69.2  Retina
C69.3  Choroid
C69.4  Ciliary Body
C69.5  Lacrimal Gland
C69.6  Orbit, NOS
C69.8  Overlapping lesion of eye and adnexa
C69.9  Eye, NOS
Note 1:  Laterality must be coded for this site.
Note 2: Code all retinoblastomas using this schema, including retinoblastomas described in other parts of eye.  AJCC TNM categories will be derived for retinoblastomas with a primary site code of C69.2 only. */
 when (primary_site in ('C690', 'C691', 'C692', 'C693', 'C694', 'C695', 'C696', 'C698', 'C699') and histology between '9510' and '9514')
  then 'Retinoblastoma'

/* Pituitary Gland, Craniopharyngeal Duct, and Pineal Gland
C75.1, C75.2, C75.3
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2
C75.1 Pituitary gland
C75.2 Craniopharyngeal duct
C75.3 Pineal Gland
Note: AJCC does not define TNM staging for this schema. */
 when primary_site in ('C751', 'C752', 'C753')
  then 'IntracranialGland'

/* Skin (excluding Skin of Eyelid and Malignant Melanoma, Merkel Cell Carcinoma, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease, and Other Lymphomas)
C44.0, C44.2-C44.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF10
C44.0  Skin of lip, NOS
C44.2  External ear
C44.3  Skin of other and unspecified parts of face
C44.4  Skin of scalp and neck
C44.5  Skin of trunk
C44.6  Skin of upper limb and shoulder
C44.7  Skin of lower limb and hip
C44.8  Overlapping lesion of skin
C44.9  Skin, NOS
Note:  Laterality must be coded for C44.2-C44.3 and C44.5-C44.7.  For codes C44.3 and C44.5, code 5 (midline) in the laterality field if the tumor is midline (e.g., chin). */
 when primary_site in ('C440', 'C442', 'C443', 'C444', 'C445', 'C446', 'C447', 'C448', 'C449')
  then 'Skin'

/* Floor of Mouth (excluding Malignant Melanoma)
C04.0-C04.1, C04.8-C04.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C04.0  Anterior floor of mouth
C04.1  Lateral floor of mouth
C04.8  Overlapping lesion of floor of mouth
C04.9  Floor of mouth, NOS */
 when primary_site in ('C040', 'C041', 'C048', 'C049')
  then 'FloorMouth'

/* Colon (excluding Appendix, Gastrointestinal Stromal Tumor, and Neuroendocrine Tumor)
C18.0, C18.2-C18.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF5, SSF7, SSF10
M-8000-8152,8154-8231,8243-8245,8247,8248,8250-8934,8940-9136,9141-9582,9700-9701
C18.0 Cecum
C18.2 Ascending colon
C18.3 Hepatic flexure of colon
C18.4 Transverse colon
C18.5 Splenic flexure of colon
C18.6 Descending colon
C18.7 Sigmoid colon
C18.8 Overlapping lesion of colon
C18.9 Colon, NOS */
 when (primary_site in ('C180', 'C182', 'C183', 'C184', 'C185', 'C186', 'C187', 'C188', 'C189') and (histology between '8000' and '8152'
  or histology between '8154' and '8231'
  or histology between '8243' and '8245'
  or histology = '8247'
  or histology = '8248'
  or histology between '8250' and '8934'
  or histology between '8940' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'Colon'

/* Peripheral Nerves and Autonomic Nervous System; Connective, Subcutaneous, and Other Soft Tissues
C47.0-C47.6, C47.8-C47.9, C49.0-C49.6, C49.8-C49.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF4
M-8000-9136,9141-9582,9700-9701
C47.0  Peripheral nerves and autonomic nervous system of head, face and neck
C47.1  Peripheral nerves and autonomic nervous system of upper limb and shoulder
C47.2  Peripheral nerves and autonomic nervous system of lower limb and hip
C47.3  Peripheral nerves and autonomic nervous system of thorax
C47.4  Peripheral nerves and autonomic nervous system of abdomen
C47.5  Peripheral nerves and autonomic nervous system of pelvis
C47.6  Peripheral nerves and autonomic nervous system of trunk, NOS
C47.8  Overlapping lesion of peripheral nerves and autonomic nervous system
C47.9  Autonomic nervous system, NOS
C49.0  Connective, subcutaneous and other soft tissues of head, face, and neck
C49.1  Connective, subcutaneous and other soft tissues of upper limb and shoulder
C49.2  Connective, subcutaneous and other soft tissues of lower limb and hip
C49.3  Connective, subcutaneous and other soft tissues of thorax
C49.4  Connective, subcutaneous and other soft tissues of abdomen
C49.5  Connective, subcutaneous and other soft tissues of pelvis
C49.6  Connective, subcutaneous and other soft tissues of trunk
C49.9  Connective, subcutaneous and other soft tissues, NOS
C49.8  Overlapping lesion of connective, subcutaneous and other soft tissues
Note 1:  Laterality must be coded for C47.1-C47.2 and C49.1-C49.2.
Note 2:  Soft tissue sarcomas of the heart and mediastinum (C38.0-C38.3 and C38.9) use the Heart, Mediastinum schema. */
 when (primary_site in ('C470', 'C471', 'C472', 'C473', 'C474', 'C475', 'C476', 'C478', 'C479', 'C490', 'C491', 'C492', 'C493', 'C494', 'C495', 'C496', 'C499', 'C498') and (histology between '8000' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'SoftTissue'

/* Cornea, Retina, Choroid, Ciliary Body, Iris, Overlapping Lesion of Eye, and Other Eye (excluding Melanoma and Retinoblastoma)
C69.1-C69.4, C69.8-C69.9
C69.1  Cornea, NOS
C69.2  Retina
C69.3  Choroid
C69.4  Ciliary body
C69.8  Overlapping lesion of eye and adnexa
C69.9  Eye, NOS
Note 1:  Laterality must be coded for this site.
Note 2:  AJCC does not define TNM staging for these sites.
Note 3:  AJCC includes primary site C69.8 (overlapping lesion of eye and adnexa) in its chapter 46, Sarcoma of the Orbit.  Collaborative Staging excludes melanomas and retinoblastomas from this schema. */
 when primary_site in ('C691', 'C692', 'C693', 'C694', 'C698', 'C699')
  then 'EyeOther'

/* Malignant Melanoma of Glottic Larynx
C32.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C32.0  Glottis */
 when (primary_site in ('C320') and histology between '8720' and '8790')
  then 'MelanomaLarynxGlottic'

/* Subglottic Larynx (excluding Malignant Melanoma)
C32.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C32.2  Subglottis */
 when primary_site in ('C322')
  then 'LarynxSubglottic'

/* Vulva (including Skin of Vulva) (excluding Malignant Melanoma, Merkel Cell Carcinoma, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease, and Other Lymphomas)
C51.0-C51.2, C51.8-C51.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF12, SSF13, SSF14, SSF15
C51.0  Labium majus
C51.1  Labium minus
C51.2  Clitoris
C51.8  Overlapping lesion of vulva
C51.9  Vulva, NOS
Note 1: This schema includes skin of Vulva but is NOT used for Malignant Melanoma, Merkel Cell Carcinoma, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease, or Other Lymphomas. Each of these diseases has a separate schema.
Note 2:  Involvement of the vulva and perineum should be assigned to vulva as the primary site in the absence of a statement that the tumor extended from the perineum to the vulva.  Collaborative Stage only includes C51.0-C51.9 (vulva) and does not include primaries of the perineum in this schema.  Basal and squamous cell carcinomas of the skin of the vulva are coded to C51.9 and are reportable; basal and squamous carcinomas of the skin of the perineum would be coded to C44.5 and would not be reportable.
Note 3:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when primary_site in ('C510', 'C511', 'C512', 'C518', 'C519')
  then 'Vulva'

/* Penis (excludes Malignant Melanoma, Merkel Cell Carcinoma, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease and Other Lymphomas)
C60.0-C60.2, C60.8-C60.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF11, SSF12, SSF16
C60.0  Prepuce
C60.1  Glans penis
C60.2  Body of penis
C60.8  Overlapping lesion of penis
C60.9  Penis, NOS
Note 1: This schema is NOT used for Malignant Melanoma, Merkel Cell Carcinoma of Penis, Kaposi Sarcoma, Mycosis Fungoides, Sezary Disease, or other Lymphomas. Each of these diseases has a separate schema.
Note 2: Primary carcinoma of the urethra is to be coded C68.0 and assigned Collaborative Stage codes according to the Urethra schema. */
 when primary_site in ('C600', 'C601', 'C602', 'C608', 'C609')
  then 'Penis'

/* Malignant Melanoma of Gum, NOS
C03.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C03.9 Gum, NOS */
 when (primary_site in ('C039') and histology between '8720' and '8790')
  then 'MelanomaGumOther'

/* Pharynx, NOS, and Overlapping Lesion of Lip, Oral Cavity, and Pharynx (excluding Malignant Melanoma)
C14.0, C14.2, C14.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C14.0  Pharynx, NOS
C14.2  Waldeyer ring
C14.8  Overlapping lesion of lip, oral cavity
Note:  AJCC does not define TNM staging for these sites. */
 when primary_site in ('C140', 'C142', 'C148')
  then 'PharynxOther'

/* Malignant Melanoma of Nasopharynx (including Pharyngeal Tonsil)
C11.0-C11.3, C11.8-C11.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C11.0 Superior wall of nasopharynx
C11.1 Posterior wall of nasopharynx (including pharyngeal tonsil)
C11.2 Lateral wall of nasopharynx
C11.3 Anterior wall of nasopharynx
C11.8 Overlapping lesion of nasopharynx
C11.9 Nasopharynx, NOS */
 when (primary_site in ('C110', 'C111', 'C112', 'C113', 'C118', 'C119') and histology between '8720' and '8790')
  then 'MelanomaNasopharynx'

/* Laryngeal Cartilage, Overlapping Lesion of Larynx, and Larynx, NOS (excluding Malignant Melanoma)
C32.3, C32.8-C32.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C32.3  Laryngeal cartilage
C32.8  Overlapping lesion of larynx
C32.9  Larynx, NOS
Note: AJCC 7 TNM staging will be derived for primary site codes of C32.8 and C32.9 only */
 when primary_site in ('C323', 'C328', 'C329')
  then 'LarynxOther'

/*   Malignant Melanoma of Skin, Vulva, Penis, Scrotum
C44.0-C44.9, C51.0-C51.2, C51.8-C51.9, C60.0-C60.2, C60.8-C60.9, C63.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF9
M-8720-8790
C44.0  Skin of lip, NOS
C44.1  Eyelid
C44.2  External ear
C44.3  Skin of other and unspecified parts of face
C44.4  Skin of scalp and neck
C44.5  Skin of trunk
C44.6  Skin of upper limb and shoulder
C44.7  Skin of lower limb and hip
C44.8  Overlapping lesion of skin
C44.9  Skin, NOS
C51.0  Labium majus
C51.1  Labium minus
C51.2  Clitoris
C51.8  Overlapping lesion of vulva
C51.9  Vulva, NOS
C60.0  Prepuce
C60.1  Glans penis
C60.2  Body of penis
C60.8  Overlapping lesion of penis
C60.9  Penis
C63.2  Scrotum, NOS
Note 1: Laterality must be coded for C44.1-C44.3, and C44.5-C44.7. For codes C44.3 and C44.5, if the tumor is midline (e.g., chin), use code  5 (midline) in the laterality field. 
Note 2: For melanoma of sites other than those above, use the appropriate site-specific schema.   */
 when (primary_site in ('C440', 'C441', 'C442', 'C443', 'C444', 'C445', 'C446', 'C447', 'C448', 'C449', 'C510', 'C511', 'C512', 'C518', 'C519', 'C600', 'C601', 'C602', 'C608', 'C609', 'C632') and histology between '8720' and '8790')
  then 'MelanomaSkin'

/* Malignant Melanoma of Iris (excluding Ciliary Body)
C69.4
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF8, SSF14
M-8720-8790
C69.4  Iris
Note 1:  CS Site-Specific Factor 25 is used to discriminate between tumors arising in the ciliary body and the iris, both coded to C69.4.
Note 2:  Laterality must be coded for this site. */
 when (primary_site in ('C694') and histology between '8720' and '8790')
  then 'MelanomaIris'

/* Liver
C22.0, C22.1
C22.0 Liver
C22.1 Intrahepatic bile duct 
Note 1:  For C22.0, the Liver schema includes only M-8000-8157,8162-8175,8190-9136,9141-9582, and 9700-9701. For cholangiocarcinoma (M-8160,8161, and 8180), see BileDuctsIntraHepat schema.
Note 2:  For C22.1, the Liver schema includes only M-8170-8175.  For other histologies, see BileDuctsIntraHepat schema.
Note 3:  AJCC 7 TNM staging will be derived for hepatocellular carcinoma only, M-8170-8175 for liver (C22.0). */
 when primary_site in ('C220', 'C221')
  then 'Liver'

/* Supraglottic Larynx (excluding Malignant Melanoma)
C32.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C32.1  Supraglottis
Note:  Excludes Anterior Surface of Epiglottis -  see separate schema (C10.1). */
 when primary_site in ('C321')
  then 'LarynxSupraglottic'

/* Malignant Melanoma of Ciliary Body (excluding Iris)
C69.4
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF14
M-8720-8790
C69.4  Ciliary body [excluding iris]
Note 1:  CS Site-Specific Factor 25 is used to discriminate between tumors arising in the ciliary body and the iris, both coded to C69.4.
Note 2:  Laterality must be coded for this site. */
 when (primary_site in ('C694') and histology between '8720' and '8790')
  then 'MelanomaCiliaryBody'

/* Pyriform Sinus, Hypopharynx, Laryngopharynx (excluding Malignant Melanoma)
C12.9, C13.0-C13.2, C13.8-C13.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C12.9  Pyriform sinus
C13.0  Postcricoid region
C13.1  Hypopharyngeal aspect of aryepiglottic fold
C13.2  Posterior wall of hypopharynx
C13.8  Overlapping lesion of hypopharynx
C13.9  Hypopharynx, NOS (laryngopharynx) */
 when primary_site in ('C129', 'C130', 'C131', 'C132', 'C138', 'C139')
  then 'Hypopharynx'

/* Malignant Melanoma of Pyriform Sinus, Hypopharynx, and Laryngopharynx
C12.9, C13.0-C13.2, C13.8-C13.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C12.9 Pyriform sinus
C13.0 Postcricoid region
C13.1 Hypopharyngeal aspect of aryepiglottic fold
C13.2 Posterior wall of hypopharynx
C13.8 Overlapping lesion of hypopharynx
C13.9 Hypopharynx, NOS (laryngopharynx) */
 when (primary_site in ('C129', 'C130', 'C131', 'C132', 'C138', 'C139') and histology between '8720' and '8790')
  then 'MelanomaHypopharynx'

/* Lip, Upper (excluding Malignant Melanoma)
C00.0, C00.3
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C00.0  External upper lip (vermilion)
C00.3  Mucosa of upper lip */
 when primary_site in ('C000', 'C003')
  then 'LipUpper'

/* Other and Unspecified Major Salivary Glands
C08.1, C08.8-C08.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C08.1  Sublingual gland
C08.8  Overlapping lesion of major salivary glands
C08.9  Major salivary gland, NOS
Note:  Laterality must be coded for C08.1. */
 when primary_site in ('C081', 'C088', 'C089')
  then 'SalivaryGlandOther'

/* Base of Tongue and Lingual Tonsil (excluding Malignant Melanoma)
C01.9, C02.4
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C01.9  Base of tongue, NOS
C02.4  Lingual tonsil
Note:  AJCC includes base of tongue (C01.9) with oropharynx (C10._). */
 when primary_site in ('C019', 'C024')
  then 'TongueBase'

/* Merkel Cell Carcinoma of the Scrotum
C63.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF19, SSF20, SSF21
M-8247
C63.2  Scrotum, NOS
Note: This schema is used for Merkel cell carcinoma only. */
 when (primary_site in ('C632') and histology = '8247')
  then 'MerkelCellScrotum'

/* Gum, NOS (excluding Malignant Melanoma)
C03.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C03.9  Gum, NOS */
 when primary_site in ('C039')
  then 'GumOther'

/* Peritoneum for Females Only
C48.1-C48.2, C48.8
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF5
M-8000-8576, 8590-8671, 8930-8934, 8940-9110
C48.1 Specified parts of peritoneum (including omentum and mesentery)
C48.2 Peritoneum, NOS
C48.8 Overlapping lesion of retroperitoneum and peritoneum
Note 1: AJCC only stages the carcinomas.
Note 2:  Information for female primary peritoneal carcinomas was collected using codes appropriate for staging sarcoma of the peritoneum in CS Version 1.  AJCC 6 stage was assigned to some but not all of the histologies that are assigned AJCC 7 stage using this schema, PeritoneumFemaleGen, in CS Version 2.  AJCC 6 stage continues to be assigned as sarcoma of the peritoneum for the histologies that were so assigned in CS Version 1.
Note 3:  AJCC TNM values correspond to the stages accepted by the Federation Internationale de Gynecologic et d'Obstetrique (FIGO).  Note that FIGO uses a single stage value which corresponds to different groupings of T, N, and M values. */
 when (primary_site in ('C481', 'C482', 'C488') and (histology between '8000' and '8576'
  or histology between '8590' and '8671'
  or histology between '8930' and '8934'
  or histology between '8940' and '9110'))
  then 'PeritoneumFemaleGen'

/* Other and Unspecified Female Genital Organs
C57.7-C57.9
C57.7  Other specified parts of female genital organs
C57.8  Overlapping lesion of female genital organs
C57.9  Female genital tract, NOS
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C577', 'C578', 'C579')
  then 'GenitalFemaleOther'

/* Anterior 2/3 of Tongue, Tip, Border, and Tongue, NOS (excluding Malignant Melanoma)
C02.0-C02.3, C02.8-C02.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C02.0  Dorsal surface of tongue, NOS
C02.1  Border of tongue (Tip)
C02.2  Ventral surface of tongue, NOS
C02.3  Anterior 2/3 of tongue, NOS
C02.8  Overlapping lesion of tongue
C02.9  Tongue, NOS */
 when primary_site in ('C020', 'C021', 'C022', 'C023', 'C028', 'C029')
  then 'TongueAnterior'

/* Gastrointestinal Stromal Tumor of Rectum and Rectosigmoid Junction
C19.9, C20.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF13, SSF14, SSF15
M-8935-8936
C19.9 Rectosigmoid junction
C20.9 Rectum, NOS
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when (primary_site in ('C199', 'C209') and histology between '8935' and '8936')
  then 'GISTRectum'

/* Orbit (excluding Lymphoma)
C69.6
C69.6  Orbit, NOS
Note 1:  Laterality must be coded for this site.
Note 2:  AJCC uses this scheme only for sarcomas of the orbit. */
 when primary_site in ('C696')
  then 'Orbit'

/* Other Biliary and Biliary, NOS
C24.8-C24.9
C24.8  Overlapping lesion of biliary tract (neoplasms involving both intrahepatic and extrahepatic bile ducts)
C24.9  Biliary tract, NOS
Note: AJCC does not define TNM staging for this site. */
 when primary_site in ('C248', 'C249')
  then 'BiliaryOther'

/* Accessory (Paranasal) Sinuses (excluding Malignant Melanoma)
C31.2-C31.3, C31.8-C31.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
C31.2  Frontal sinus
C31.3  Sphenoid sinus
C31.8  Overlapping lesion of accessory sinuses
C31.9  Accessory sinus, NOS
Note 1:  Laterality must be coded for frontal sinus, C31.2
Note 2:  AJCC does not define TNM staging for these sites. */
 when primary_site in ('C312', 'C313', 'C318', 'C319')
  then 'SinusOther'

/* Intrahepatic Bile Ducts
C22.0, C22.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF3, SSF12
C22.0 Liver
C22.1  Intrahepatic bile duct
Note 1: For C22.0, the BileDuctsIntraHepat schema includes only M-8160, 8161, 8180. For other histologies, see the Liver schema.
Note 2: For C22.1, the BileDuctsIntraHepat schema only includes M-8000-8162, 8180-9136, 9141-9582, and 9700-9701.  For hepatocellular carcinoma, M-8170-8175, see the Liver schema.
Note 3:  AJCC TNM 7 staging will be derived for cases with primary site code of C22.1 and histology code of 8160, 8161, and 8180 only.
Note 4: Staging for intrahepatic bile ducts was included in the Liver chapter in the AJCC 6th Edition.  Intrahepatic Bile Ducts is a separate chapter in the AJCC 7th Edition. */
 when primary_site in ('C220', 'C221')
  then 'BileDuctsIntraHepat'

/* PharyngealTonsil (excluding Malignant Melanoma)
C11.1
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C11.1 Pharyngeal tonsil 
Note:  CS Site-Specific Factor 25 is used to discriminate between posterior wall of nasopharynx, staged with nasopharynx, and pharyngeal tonsil, staged with oropharynx.  Both sites are coded to ICD-O-3 code C11.1. */
 when primary_site in ('C111')
  then 'PharyngealTonsil'

/* Breast
C50.0-C50.6, C50.8-C50.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF17, SSF18, SSF19, SSF20, SSF24
C50.0  Nipple
C50.1  Central portion of breast
C50.2  Upper-inner quadrant of breast
C50.3  Lower-inner quadrant of breast
C50.4  Upper-outer quadrant of breast
C50.5  Lower-outer quadrant of breast
C50.6  Axillary Tail of breast
C50.8  Overlapping lesion of breast
C50.9  Breast, NOS
Note:  Laterality must be coded for this site. */
 when primary_site in ('C500', 'C501', 'C502', 'C503', 'C504', 'C505', 'C506', 'C508', 'C509')
  then 'Breast'

/* Testis
C62.0-C62.1, C62.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF12, SSF14
C62.0  Undescended testis
C62.1  Descended testis
C62.9  Testis, NOS
Note 1: Instructions for coding pre- or post-orchiectomy tumor marker values were ambiguous for CS version 1 and there was variation in data collection by registrars. Furthermore, AJCC 7th Edition clarified that S value stage group IS is to be calculated based on the persistence of elevated serum tumor markers after surgery. As a consequence, there is uncertainly about the reliability of the data for the S parameter in data collected with CS version 1. The data elements and codes have been modified in CS version 2 to calculate the S value correctly. Any analysis of testis staging over time relying on the tumor marker data collected in CS version 1 might require review of medical records to verify the appropriate preoperative tumor marker values and the presence of persistent tumor markers post-orchiectomy.
Note 2: CS version 2 corrects some version 1 errors in the calculation of the N category. For this reason, analysis of data originally collected in version 1 may show a different distribution of N categories and stage groups once the version 2 algorithm is run to re-derive AJCC 6th edition staging.
Note 3: For cases collected in CSv1, the T category is derived using the Extension Orchiectomy LVI AJCC 6 Table CSv1, the S value is derived using the Serum Tumor Marker S Value Table Based on CS SSF 1, 2, 3, and the AJCC 6th Edition stage is derived using the AJCC TNM 6 Stage CSv1 table.
Note 4: For cases collected in CSv2, the T category is derived using the Extension Orchiectomy LVI AJCC 6 Table CSv2 or the Extension Orchiectomy LVI AJCC 7 Table, the S value is derived using the Post-orchiectomy Serum Marker S Value Table Based on SSF 13, 15, 16, and the AJCC 6th and 7th Edition stages are derived using the AJCC TNM 6 Stage table and AJCC TNM 7 Stage table.
Note 5: Laterality must be coded for this site. */
 when primary_site in ('C620', 'C621', 'C629')
  then 'Testis'

/* Small Intestine (excluding Gastrointestinal Stromal Tumor and Neuroendocrine Tumor)
C17.0-C17.3, C17.8-C17.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF5
M-8000-8152,8154-8231,8243-8245,8247,8248,8250-8934,8940-9136,9141-9582,9700-9701 
C17.0  Duodenum
C17.1  Jejunum
C17.2  Ileum (excluding ileocecal valve C18.0)
C17.3  Meckel diverticulum (site of neoplasm)
C17.8  Overlapping lesion of small intestine
C17.9  Small intestine, NOS */
 when ((primary_site in ('C170', 'C171', 'C172', 'C173', 'C178', 'C179') and (not primary_site in ('C180'))) and (histology between '8000' and '8152'
  or histology between '8154' and '8231'
  or histology between '8243' and '8245'
  or histology = '8247'
  or histology = '8248'
  or histology between '8250' and '8934'
  or histology between '8940' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'SmallIntestine'

/* Gastrointestinal Stromal Tumor of Esophagus
C15.0-C15.5, C15.8-C15.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF8, SSF9, SSF10
M- 8935-8936
C15.0  Cervical esophagus
C15.1  Thoracic esophagus
C15.2  Abdominal esophagus
C15.3  Upper third of esophagus
C15.4  Middle third of esophagus
C15.5  Lower third of esophagus
C15.8  Overlapping lesion of esophagus
C15.9  Esophagus, NOS
Note: The histologies included in this schema were not staged with AJCC 6th Edition.  Therefore, the algorithm will not derive an AJCC 6th TNM or stage group. */
 when (primary_site in ('C150', 'C151', 'C152', 'C153', 'C154', 'C155', 'C158', 'C159') and histology between '8935' and '8936')
  then 'GISTEsophagus'

/* Nasopharynx (excluding Malignant Melanoma)
C11.0-C11.3, C11.8-C11.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C11.0  Superior wall of nasopharynx
C11.1  Posterior wall of nasopharynx (excluding pharyngeal tonsil)
C11.2  Lateral wall of nasopharynx
C11.3  Anterior wall of nasopharynx
C11.8  Overlapping lesion of nasopharynx
C11.9  Nasopharynx, NOS */
 when primary_site in ('C110', 'C111', 'C112', 'C113', 'C118', 'C119')
  then 'Nasopharynx'

/* Malignant Melanoma of Floor of Mouth
C04.0-C04.1, C04.8-C04.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C04.0 Anterior floor of mouth
C04.1 Lateral floor of mouth
C04.8 Overlapping lesion of floor of mouth
C04.9 Floor of mouth, NOS */
 when (primary_site in ('C040', 'C041', 'C048', 'C049') and histology between '8720' and '8790')
  then 'MelanomaFloorMouth'

/* Malignant Melanoma of Other Lip
C00.2, C00.5, C00.8-C00.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8, SSF10
M-8720-8790
C00.2 External lip, NOS
C00.5 Mucosa of lip, NOS
C00.8 Overlapping lesion of lip
C00.9 Lip, NOS (excludes skin of lip C44.0) */
 when ((primary_site in ('C002', 'C005', 'C008', 'C009') and (not primary_site in ('C440'))) and histology between '8720' and '8790')
  then 'MelanomaLipOther'

/* Paraurethral Gland, Overlapping Lesion of Urinary Organs, and Unspecified Urinary Organs
C68.1, C68.8-C68.9
C68.1  Paraurethral gland
C68.8  Overlapping lesion of urinary organs
C68.9  Urinary system, NOS
Note:  AJCC does not define TNM staging for this site. */
 when primary_site in ('C681', 'C688', 'C689')
  then 'UrinaryOther'

/* Neuroendocrine Tumors of Stomach
C16.0-C16.6, C16.8-C16.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF6
M-8153, 8240-8242, 8246, 8249
C16.0 Cardia of stomach
C16.1 Fundus of stomach
C16.2 Body of stomach
C16.3 Gastric antrum
C16.4 Pylorus
C16.5 Lesser curvature of stomach, NOS
C16.6 Greater curvature of stomach, NOS
C16.8 Overlapping lesion of stomach
C16.9 Stomach, NOS
Note 1: For this schema, AJCC only stages well-differentiated neuroendocrine tumors. Note that the "concept" of well-differentiated is reflected in the histology code. The grade code is not needed in order to select the correct schema, but does need to be coded.
Note 2: This schema is also used for carcinoid tumors and malignant gastrinomas. */
 when (primary_site in ('C160', 'C161', 'C162', 'C163', 'C164', 'C165', 'C166', 'C168', 'C169') and (histology = '8153'
  or histology between '8240' and '8242'
  or histology = '8246'
  or histology = '8249'))
  then 'NETStomach'

/* Hodgkin and Non-Hodgkin Lymphomas of All Sites (excluding Mycosis Fungoides and Sezary Disease)
None
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF4, SSF5
M-9590-9699,9702-9729,9735,9737,9738 (EXCEPT C44.1, C69.0, C69.5-C69.6)
M-9811-9818,9823,9827,9837 (EXCEPT C42.0, C42.1, C42.4, C44.1, C69.0, C69.5-C69.6)  */
 when (((histology between '9590' and '9699'
  or histology between '9702' and '9729'
  or histology = '9735'
  or histology = '9737'
  or histology = '9738') and (not (primary_site = 'C441'
  or primary_site = 'C690'
  or primary_site between 'C695' and 'C696')))
  or ((histology between '9811' and '9818'
  or histology = '9823'
  or histology = '9827'
  or histology = '9837') and (not (primary_site = 'C420'
  or primary_site = 'C421'
  or primary_site = 'C424'
  or primary_site = 'C441'
  or primary_site = 'C690'
  or primary_site between 'C695' and 'C696'))))
  then 'Lymphoma'

/* Parotid Gland
C07.9
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF7, SSF8
C07.9  Parotid gland
Note:  Laterality must be coded for this site. */
 when primary_site in ('C079')
  then 'ParotidGland'

/* Distal Bile Duct
C24.0 Extrahepatic bile duct
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF12 SSF13, SSF14
C24.0 Extrahepatic bile duct
Note: Extrahepatic Bile Duct was a single chapter in the AJCC 6th Edition. It has been divided into two chapters in the AJCC 7th Edition: Perihilar Bile Ducts and Distal Bile Duct. */
 when primary_site in ('C240')
  then 'BileDuctsDistal'

/* Pleura (Including pleural mesothelioma)
C38.4
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF3, SSF4, SSF5
M-8000-9136,9141-9582,9700-9701 
C38.4 Pleura, NOS */
 when (primary_site in ('C384') and (histology between '8000' and '9136'
  or histology between '9141' and '9582'
  or histology between '9700' and '9701'))
  then 'Pleura'

/* EsophagusGEJunction
C16.0, C16.1, C16.2
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF3, SSF4, SSF5
C16.0 Cardia, esophagogastric junction (EGJ)
C16.1 Fundus of stomach, proximal 5 centimeters (cm) only
C16.2 Body of stomach, proximal 5 cm only
Note 1: The cardia/EGJ and the proximal 5 cm of the fundus and body of the stomach have been removed from the Stomach chapter and added to the Esophagus chapter effective with AJCC TNM 7th Edition. Due to differences between the schemas for Esophagus and Stomach, a new schema was created in CSv2 to accommodate these changes. Since primary site codes C16.1 (fundus of stomach) and C16.2 (body of stomach) can be assigned to either schema, EsophagusGEJunction or Stomach, a schema discriminator field is needed for the CS algorithm to determine which schema to select. 
In AJCC 7th Edition, cancers with a midpoint in the lower thoracic esophagus, in the EGJ, or within the proximal 5 cm of the stomach (cardia) and extending into the EGJ or esophagus, are staged similarly to cancers of the esophagus. All other cancers with a midpoint in the stomach greater than 5 cm distal to the EGJ, or those within 5 cm of the EGJ but not extending into the EGJ or esophagus, are staged using the gastric cancer staging system.
Note 2: Effective with AJCC TNM 7th Edition, there are separate stage groupings for squamous cell carcinoma and adenocarcinoma of the esophagus. Since squamous cell carcinoma typically has a poorer prognosis than adenocarcinoma, a tumor of mixed histopathologic type or a type that is not otherwise specified should be classified as squamous cell carcinoma. 
Note 3: Effective with AJCC TNM 7th Edition, histologic grade is required for stage grouping. */
 when primary_site in ('C160', 'C161', 'C162')
  then 'EsophagusGEJunction'

/* Retroperitoneum
C48.0
DISCONTINUED SITE-SPECIFIC FACTORS:  SSF2, SSF3, SSF4
C48.0 Retroperitioneum */
 when primary_site in ('C480')
  then 'Retroperitoneum'


         end schemaid
from
 (select ne."Patient ID Number" as MRN
       , ne.case_index
       , ne."Primary Site" as primary_site
       , substr(ne."Morph--Type&Behav ICD-O-3", 1, 4) histology
       -- TODO: push start_date into tumor_item_value
       , to_date(case length(ne."Date of Diagnosis")
               when 8 then ne."Date of Diagnosis"
               when 6 then ne."Date of Diagnosis" || '01'
               when 4 then ne."Date of Diagnosis" || '0101'
               end, 'yyyymmdd') as start_date
  from naacr.extract ne
  where ne."Date of Diagnosis" is not null
    and ne."Accession Number--Hosp" is not null) ne
;

create or replace view cs_site_factor_facts as

with ssf_item as (
  select min(itemnbr) lo, max(itemnbr) hi
  from tumor_item_type
  where itemname like 'CS Site-Specific Factor%'
)

select cs.mrn
     , cs.case_index
     , 'CS|' || cs.schemaid || '|' ||
       trim(substr(ssf.itemname, length('CS Site-Specific Factor '))) ||
       ':' || ssf.codenbr concept_cd
     , '@' item_name
     , '@' provider_id
     , cs.start_date
     , '@' modifier_cd  -- hmm... modifier for synthesized info?
     , 1 instance_num
     , '@' valtype_cd
     , null tval_char
     , to_number(null) nval_num
     , null as valueflag_cd
     , null as units_cd
     , cs.start_date as end_date
     , '@' location_cd
     , to_date(null) as update_date
from tumor_item_value ssf
join tumor_cs_schema cs on cs.case_index = ssf.case_index
join ssf_item on ssf.itemnbr between ssf_item.lo and ssf_item.hi
where cs.schemaid is not null
;


/* Eyeball it:

select *
from tumor_cs_schema
;

select *
from cs_site_factor_facts
;

*/
