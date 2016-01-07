create materialized view "NAACR"."EXTRACT_EAV" as 
select case_index, 
10 as ItemNbr,
'Record Type' as ItemName,
"Record Type" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
30 as ItemNbr,
'Registry Type' as ItemName,
"Registry Type" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
35 as ItemNbr,
'FIN Coding System' as ItemName,
"FIN Coding System" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
37 as ItemNbr,
'Reserved 00' as ItemName,
"Reserved 00" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
50 as ItemNbr,
'NAACCR Record Version' as ItemName,
"NAACCR Record Version" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
45 as ItemNbr,
'NPI--Registry ID' as ItemName,
"NPI--Registry ID" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
40 as ItemNbr,
'Registry ID' as ItemName,
"Registry ID" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
60 as ItemNbr,
'Tumor Record Number' as ItemName,
"Tumor Record Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
20 as ItemNbr,
'Patient ID Number' as ItemName,
"Patient ID Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
21 as ItemNbr,
'Patient System ID-Hosp' as ItemName,
"Patient System ID-Hosp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
370 as ItemNbr,
'Reserved 01' as ItemName,
"Reserved 01" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
70 as ItemNbr,
'Addr at DX--City' as ItemName,
"Addr at DX--City" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
80 as ItemNbr,
'Addr at DX--State' as ItemName,
"Addr at DX--State" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
100 as ItemNbr,
'Addr at DX--Postal Code' as ItemName,
"Addr at DX--Postal Code" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
90 as ItemNbr,
'County at DX' as ItemName,
"County at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
110 as ItemNbr,
'Census Tract 1970/80/90' as ItemName,
"Census Tract 1970/80/90" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
368 as ItemNbr,
'CensusBlockGroup 70/80/90' as ItemName,
"CensusBlockGroup 70/80/90" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
120 as ItemNbr,
'Census Cod Sys 1970/80/90' as ItemName,
"Census Cod Sys 1970/80/90" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
364 as ItemNbr,
'Census Tr Cert 1970/80/90' as ItemName,
"Census Tr Cert 1970/80/90" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
130 as ItemNbr,
'Census Tract 2000' as ItemName,
"Census Tract 2000" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
362 as ItemNbr,
'Census Block Group 2000' as ItemName,
"Census Block Group 2000" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
365 as ItemNbr,
'Census Tr Certainty 2000' as ItemName,
"Census Tr Certainty 2000" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
150 as ItemNbr,
'Marital Status at DX' as ItemName,
"Marital Status at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
160 as ItemNbr,
'Race 1' as ItemName,
"Race 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
161 as ItemNbr,
'Race 2' as ItemName,
"Race 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
162 as ItemNbr,
'Race 3' as ItemName,
"Race 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
163 as ItemNbr,
'Race 4' as ItemName,
"Race 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
164 as ItemNbr,
'Race 5' as ItemName,
"Race 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
170 as ItemNbr,
'Race Coding Sys--Current' as ItemName,
"Race Coding Sys--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
180 as ItemNbr,
'Race Coding Sys--Original' as ItemName,
"Race Coding Sys--Original" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
190 as ItemNbr,
'Spanish/Hispanic Origin' as ItemName,
"Spanish/Hispanic Origin" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
200 as ItemNbr,
'Computed Ethnicity' as ItemName,
"Computed Ethnicity" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
210 as ItemNbr,
'Computed Ethnicity Source' as ItemName,
"Computed Ethnicity Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
220 as ItemNbr,
'Sex' as ItemName,
"Sex" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
230 as ItemNbr,
'Age at Diagnosis' as ItemName,
"Age at Diagnosis" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
240 as ItemNbr,
'Date of Birth' as ItemName,
"Date of Birth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
241 as ItemNbr,
'Date of Birth Flag' as ItemName,
"Date of Birth Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
250 as ItemNbr,
'Birthplace' as ItemName,
"Birthplace" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
270 as ItemNbr,
'Occupation Code--Census' as ItemName,
"Occupation Code--Census" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
280 as ItemNbr,
'Industry Code--Census' as ItemName,
"Industry Code--Census" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
290 as ItemNbr,
'Occupation Source' as ItemName,
"Occupation Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
300 as ItemNbr,
'Industry Source' as ItemName,
"Industry Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
310 as ItemNbr,
'Text--Usual Occupation' as ItemName,
"Text--Usual Occupation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
320 as ItemNbr,
'Text--Usual Industry' as ItemName,
"Text--Usual Industry" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
330 as ItemNbr,
'Occup/Ind Coding System' as ItemName,
"Occup/Ind Coding System" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
191 as ItemNbr,
'NHIA Derived Hisp Origin' as ItemName,
"NHIA Derived Hisp Origin" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
193 as ItemNbr,
'Race--NAPIIA(derived API)' as ItemName,
"Race--NAPIIA(derived API)" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
192 as ItemNbr,
'IHS Link' as ItemName,
"IHS Link" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
366 as ItemNbr,
'GIS Coordinate Quality' as ItemName,
"GIS Coordinate Quality" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3300 as ItemNbr,
'RuralUrban Continuum 1993' as ItemName,
"RuralUrban Continuum 1993" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3310 as ItemNbr,
'RuralUrban Continuum 2003' as ItemName,
"RuralUrban Continuum 2003" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
135 as ItemNbr,
'Census Tract 2010' as ItemName,
"Census Tract 2010" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
363 as ItemNbr,
'Census Block Group 2010' as ItemName,
"Census Block Group 2010" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
367 as ItemNbr,
'Census Tr Certainty 2010' as ItemName,
"Census Tr Certainty 2010" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
530 as ItemNbr,
'Reserved 02' as ItemName,
"Reserved 02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
380 as ItemNbr,
'Sequence Number--Central' as ItemName,
"Sequence Number--Central" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
390 as ItemNbr,
'Date of Diagnosis' as ItemName,
"Date of Diagnosis" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
391 as ItemNbr,
'Date of Diagnosis Flag' as ItemName,
"Date of Diagnosis Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
400 as ItemNbr,
'Primary Site' as ItemName,
"Primary Site" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
410 as ItemNbr,
'Laterality' as ItemName,
"Laterality" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
419 as ItemNbr,
'Morph--Type&Behav ICD-O-2' as ItemName,
"Morph--Type&Behav ICD-O-2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
420 as ItemNbr,
'Histology (92-00) ICD-O-2' as ItemName,
"Histology (92-00) ICD-O-2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
430 as ItemNbr,
'Behavior (92-00) ICD-O-2' as ItemName,
"Behavior (92-00) ICD-O-2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
521 as ItemNbr,
'Morph--Type&Behav ICD-O-3' as ItemName,
"Morph--Type&Behav ICD-O-3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
522 as ItemNbr,
'Histologic Type ICD-O-3' as ItemName,
"Histologic Type ICD-O-3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
523 as ItemNbr,
'Behavior Code ICD-O-3' as ItemName,
"Behavior Code ICD-O-3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
440 as ItemNbr,
'Grade' as ItemName,
"Grade" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
441 as ItemNbr,
'Grade Path Value' as ItemName,
"Grade Path Value" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
449 as ItemNbr,
'Grade Path System' as ItemName,
"Grade Path System" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
450 as ItemNbr,
'Site Coding Sys--Current' as ItemName,
"Site Coding Sys--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
460 as ItemNbr,
'Site Coding Sys--Original' as ItemName,
"Site Coding Sys--Original" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
470 as ItemNbr,
'Morph Coding Sys--Current' as ItemName,
"Morph Coding Sys--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
480 as ItemNbr,
'Morph Coding Sys--Originl' as ItemName,
"Morph Coding Sys--Originl" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
490 as ItemNbr,
'Diagnostic Confirmation' as ItemName,
"Diagnostic Confirmation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
500 as ItemNbr,
'Type of Reporting Source' as ItemName,
"Type of Reporting Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
501 as ItemNbr,
'Casefinding Source' as ItemName,
"Casefinding Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
442 as ItemNbr,
'Ambiguous Terminology DX' as ItemName,
"Ambiguous Terminology DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
443 as ItemNbr,
'Date of Conclusive DX' as ItemName,
"Date of Conclusive DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
448 as ItemNbr,
'Date Conclusive DX Flag' as ItemName,
"Date Conclusive DX Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
444 as ItemNbr,
'Mult Tum Rpt as One Prim' as ItemName,
"Mult Tum Rpt as One Prim" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
445 as ItemNbr,
'Date of Multiple Tumors' as ItemName,
"Date of Multiple Tumors" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
439 as ItemNbr,
'Date of Mult Tumors Flag' as ItemName,
"Date of Mult Tumors Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
446 as ItemNbr,
'Multiplicity Counter' as ItemName,
"Multiplicity Counter" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
680 as ItemNbr,
'Reserved 03' as ItemName,
"Reserved 03" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
545 as ItemNbr,
'NPI--Reporting Facility' as ItemName,
"NPI--Reporting Facility" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
540 as ItemNbr,
'Reporting Facility' as ItemName,
"Reporting Facility" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3105 as ItemNbr,
'NPI--Archive FIN' as ItemName,
"NPI--Archive FIN" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3100 as ItemNbr,
'Archive FIN' as ItemName,
"Archive FIN" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
550 as ItemNbr,
'Accession Number--Hosp' as ItemName,
"Accession Number--Hosp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
560 as ItemNbr,
'Sequence Number--Hospital' as ItemName,
"Sequence Number--Hospital" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
570 as ItemNbr,
'Abstracted By' as ItemName,
"Abstracted By" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
580 as ItemNbr,
'Date of 1st Contact' as ItemName,
"Date of 1st Contact" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
581 as ItemNbr,
'Date of 1st Contact Flag' as ItemName,
"Date of 1st Contact Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
590 as ItemNbr,
'Date of Inpatient Adm' as ItemName,
"Date of Inpatient Adm" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
591 as ItemNbr,
'Date of Inpt Adm Flag' as ItemName,
"Date of Inpt Adm Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
600 as ItemNbr,
'Date of Inpatient Disch' as ItemName,
"Date of Inpatient Disch" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
601 as ItemNbr,
'Date of Inpt Disch Flag' as ItemName,
"Date of Inpt Disch Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
605 as ItemNbr,
'Inpatient Status' as ItemName,
"Inpatient Status" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
610 as ItemNbr,
'Class of Case' as ItemName,
"Class of Case" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
630 as ItemNbr,
'Primary Payer at DX' as ItemName,
"Primary Payer at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2400 as ItemNbr,
'Reserved 16' as ItemName,
"Reserved 16" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
668 as ItemNbr,
'RX Hosp--Surg App 2010' as ItemName,
"RX Hosp--Surg App 2010" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
670 as ItemNbr,
'RX Hosp--Surg Prim Site' as ItemName,
"RX Hosp--Surg Prim Site" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
672 as ItemNbr,
'RX Hosp--Scope Reg LN Sur' as ItemName,
"RX Hosp--Scope Reg LN Sur" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
674 as ItemNbr,
'RX Hosp--Surg Oth Reg/Dis' as ItemName,
"RX Hosp--Surg Oth Reg/Dis" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
676 as ItemNbr,
'RX Hosp--Reg LN Removed' as ItemName,
"RX Hosp--Reg LN Removed" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
690 as ItemNbr,
'RX Hosp--Radiation' as ItemName,
"RX Hosp--Radiation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
700 as ItemNbr,
'RX Hosp--Chemo' as ItemName,
"RX Hosp--Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
710 as ItemNbr,
'RX Hosp--Hormone' as ItemName,
"RX Hosp--Hormone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
720 as ItemNbr,
'RX Hosp--BRM' as ItemName,
"RX Hosp--BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
730 as ItemNbr,
'RX Hosp--Other' as ItemName,
"RX Hosp--Other" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
740 as ItemNbr,
'RX Hosp--DX/Stg Proc' as ItemName,
"RX Hosp--DX/Stg Proc" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3280 as ItemNbr,
'RX Hosp--Palliative Proc' as ItemName,
"RX Hosp--Palliative Proc" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
746 as ItemNbr,
'RX Hosp--Surg Site 98-02' as ItemName,
"RX Hosp--Surg Site 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
747 as ItemNbr,
'RX Hosp--Scope Reg 98-02' as ItemName,
"RX Hosp--Scope Reg 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
748 as ItemNbr,
'RX Hosp--Surg Oth 98-02' as ItemName,
"RX Hosp--Surg Oth 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
750 as ItemNbr,
'Reserved 04' as ItemName,
"Reserved 04" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
759 as ItemNbr,
'SEER Summary Stage 2000' as ItemName,
"SEER Summary Stage 2000" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
760 as ItemNbr,
'SEER Summary Stage 1977' as ItemName,
"SEER Summary Stage 1977" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
779 as ItemNbr,
'Extent of Disease 10-Dig' as ItemName,
"Extent of Disease 10-Dig" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
780 as ItemNbr,
'EOD--Tumor Size' as ItemName,
"EOD--Tumor Size" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
790 as ItemNbr,
'EOD--Extension' as ItemName,
"EOD--Extension" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
800 as ItemNbr,
'EOD--Extension Prost Path' as ItemName,
"EOD--Extension Prost Path" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
810 as ItemNbr,
'EOD--Lymph Node Involv' as ItemName,
"EOD--Lymph Node Involv" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
820 as ItemNbr,
'Regional Nodes Positive' as ItemName,
"Regional Nodes Positive" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
830 as ItemNbr,
'Regional Nodes Examined' as ItemName,
"Regional Nodes Examined" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
840 as ItemNbr,
'EOD--Old 13 Digit' as ItemName,
"EOD--Old 13 Digit" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
850 as ItemNbr,
'EOD--Old 2 Digit' as ItemName,
"EOD--Old 2 Digit" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
860 as ItemNbr,
'EOD--Old 4 Digit' as ItemName,
"EOD--Old 4 Digit" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
870 as ItemNbr,
'Coding System for EOD' as ItemName,
"Coding System for EOD" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1060 as ItemNbr,
'TNM Edition Number' as ItemName,
"TNM Edition Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
880 as ItemNbr,
'TNM Path T' as ItemName,
"TNM Path T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
890 as ItemNbr,
'TNM Path N' as ItemName,
"TNM Path N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
900 as ItemNbr,
'TNM Path M' as ItemName,
"TNM Path M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
910 as ItemNbr,
'TNM Path Stage Group' as ItemName,
"TNM Path Stage Group" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
920 as ItemNbr,
'TNM Path Descriptor' as ItemName,
"TNM Path Descriptor" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
930 as ItemNbr,
'TNM Path Staged By' as ItemName,
"TNM Path Staged By" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
940 as ItemNbr,
'TNM Clin T' as ItemName,
"TNM Clin T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
950 as ItemNbr,
'TNM Clin N' as ItemName,
"TNM Clin N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
960 as ItemNbr,
'TNM Clin M' as ItemName,
"TNM Clin M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
970 as ItemNbr,
'TNM Clin Stage Group' as ItemName,
"TNM Clin Stage Group" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
980 as ItemNbr,
'TNM Clin Descriptor' as ItemName,
"TNM Clin Descriptor" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
990 as ItemNbr,
'TNM Clin Staged By' as ItemName,
"TNM Clin Staged By" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1120 as ItemNbr,
'Pediatric Stage' as ItemName,
"Pediatric Stage" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1130 as ItemNbr,
'Pediatric Staging System' as ItemName,
"Pediatric Staging System" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1140 as ItemNbr,
'Pediatric Staged By' as ItemName,
"Pediatric Staged By" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1150 as ItemNbr,
'Tumor Marker 1' as ItemName,
"Tumor Marker 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1160 as ItemNbr,
'Tumor Marker 2' as ItemName,
"Tumor Marker 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1170 as ItemNbr,
'Tumor Marker 3' as ItemName,
"Tumor Marker 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1182 as ItemNbr,
'Lymph-vascular Invasion' as ItemName,
"Lymph-vascular Invasion" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2800 as ItemNbr,
'CS Tumor Size' as ItemName,
"CS Tumor Size" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2810 as ItemNbr,
'CS Extension' as ItemName,
"CS Extension" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2820 as ItemNbr,
'CS Tumor Size/Ext Eval' as ItemName,
"CS Tumor Size/Ext Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2830 as ItemNbr,
'CS Lymph Nodes' as ItemName,
"CS Lymph Nodes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2840 as ItemNbr,
'CS Lymph Nodes Eval' as ItemName,
"CS Lymph Nodes Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2850 as ItemNbr,
'CS Mets at DX' as ItemName,
"CS Mets at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2860 as ItemNbr,
'CS Mets Eval' as ItemName,
"CS Mets Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2851 as ItemNbr,
'CS Mets at Dx-Bone' as ItemName,
"CS Mets at Dx-Bone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2852 as ItemNbr,
'CS Mets at Dx-Brain' as ItemName,
"CS Mets at Dx-Brain" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2853 as ItemNbr,
'CS Mets at Dx-Liver' as ItemName,
"CS Mets at Dx-Liver" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2854 as ItemNbr,
'CS Mets at Dx-Lung' as ItemName,
"CS Mets at Dx-Lung" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2880 as ItemNbr,
'CS Site-Specific Factor 1' as ItemName,
"CS Site-Specific Factor 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2890 as ItemNbr,
'CS Site-Specific Factor 2' as ItemName,
"CS Site-Specific Factor 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2900 as ItemNbr,
'CS Site-Specific Factor 3' as ItemName,
"CS Site-Specific Factor 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2910 as ItemNbr,
'CS Site-Specific Factor 4' as ItemName,
"CS Site-Specific Factor 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2920 as ItemNbr,
'CS Site-Specific Factor 5' as ItemName,
"CS Site-Specific Factor 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2930 as ItemNbr,
'CS Site-Specific Factor 6' as ItemName,
"CS Site-Specific Factor 6" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2861 as ItemNbr,
'CS Site-Specific Factor 7' as ItemName,
"CS Site-Specific Factor 7" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2862 as ItemNbr,
'CS Site-Specific Factor 8' as ItemName,
"CS Site-Specific Factor 8" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2863 as ItemNbr,
'CS Site-Specific Factor 9' as ItemName,
"CS Site-Specific Factor 9" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2864 as ItemNbr,
'CS Site-Specific Factor10' as ItemName,
"CS Site-Specific Factor10" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2865 as ItemNbr,
'CS Site-Specific Factor11' as ItemName,
"CS Site-Specific Factor11" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2866 as ItemNbr,
'CS Site-Specific Factor12' as ItemName,
"CS Site-Specific Factor12" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2867 as ItemNbr,
'CS Site-Specific Factor13' as ItemName,
"CS Site-Specific Factor13" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2868 as ItemNbr,
'CS Site-Specific Factor14' as ItemName,
"CS Site-Specific Factor14" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2869 as ItemNbr,
'CS Site-Specific Factor15' as ItemName,
"CS Site-Specific Factor15" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2870 as ItemNbr,
'CS Site-Specific Factor16' as ItemName,
"CS Site-Specific Factor16" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2871 as ItemNbr,
'CS Site-Specific Factor17' as ItemName,
"CS Site-Specific Factor17" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2872 as ItemNbr,
'CS Site-Specific Factor18' as ItemName,
"CS Site-Specific Factor18" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2873 as ItemNbr,
'CS Site-Specific Factor19' as ItemName,
"CS Site-Specific Factor19" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2874 as ItemNbr,
'CS Site-Specific Factor20' as ItemName,
"CS Site-Specific Factor20" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2875 as ItemNbr,
'CS Site-Specific Factor21' as ItemName,
"CS Site-Specific Factor21" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2876 as ItemNbr,
'CS Site-Specific Factor22' as ItemName,
"CS Site-Specific Factor22" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2877 as ItemNbr,
'CS Site-Specific Factor23' as ItemName,
"CS Site-Specific Factor23" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2878 as ItemNbr,
'CS Site-Specific Factor24' as ItemName,
"CS Site-Specific Factor24" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2879 as ItemNbr,
'CS Site-Specific Factor25' as ItemName,
"CS Site-Specific Factor25" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2730 as ItemNbr,
'CS PreRx Tumor Size' as ItemName,
"CS PreRx Tumor Size" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2735 as ItemNbr,
'CS PreRx Extension' as ItemName,
"CS PreRx Extension" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2740 as ItemNbr,
'CS PreRx Tum Sz/Ext Eval' as ItemName,
"CS PreRx Tum Sz/Ext Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2750 as ItemNbr,
'CS PreRx Lymph Nodes' as ItemName,
"CS PreRx Lymph Nodes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2755 as ItemNbr,
'CS PreRx Reg Nodes Eval' as ItemName,
"CS PreRx Reg Nodes Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2760 as ItemNbr,
'CS PreRx Mets at DX' as ItemName,
"CS PreRx Mets at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2765 as ItemNbr,
'CS PreRx Mets Eval' as ItemName,
"CS PreRx Mets Eval" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2770 as ItemNbr,
'CS PostRx Tumor Size' as ItemName,
"CS PostRx Tumor Size" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2775 as ItemNbr,
'CS PostRx Extension' as ItemName,
"CS PostRx Extension" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2780 as ItemNbr,
'CS PostRx Lymph Nodes' as ItemName,
"CS PostRx Lymph Nodes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2785 as ItemNbr,
'CS PostRx Mets at DX' as ItemName,
"CS PostRx Mets at DX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2940 as ItemNbr,
'Derived AJCC-6 T' as ItemName,
"Derived AJCC-6 T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2950 as ItemNbr,
'Derived AJCC-6 T Descript' as ItemName,
"Derived AJCC-6 T Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2960 as ItemNbr,
'Derived AJCC-6 N' as ItemName,
"Derived AJCC-6 N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2970 as ItemNbr,
'Derived AJCC-6 N Descript' as ItemName,
"Derived AJCC-6 N Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2980 as ItemNbr,
'Derived AJCC-6 M' as ItemName,
"Derived AJCC-6 M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2990 as ItemNbr,
'Derived AJCC-6 M Descript' as ItemName,
"Derived AJCC-6 M Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3000 as ItemNbr,
'Derived AJCC-6 Stage Grp' as ItemName,
"Derived AJCC-6 Stage Grp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3400 as ItemNbr,
'Derived AJCC-7 T' as ItemName,
"Derived AJCC-7 T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3402 as ItemNbr,
'Derived AJCC-7 T Descript' as ItemName,
"Derived AJCC-7 T Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3410 as ItemNbr,
'Derived AJCC-7 N' as ItemName,
"Derived AJCC-7 N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3412 as ItemNbr,
'Derived AJCC-7 N Descript' as ItemName,
"Derived AJCC-7 N Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3420 as ItemNbr,
'Derived AJCC-7 M' as ItemName,
"Derived AJCC-7 M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3422 as ItemNbr,
'Derived AJCC-7 M Descript' as ItemName,
"Derived AJCC-7 M Descript" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3430 as ItemNbr,
'Derived AJCC-7 Stage Grp' as ItemName,
"Derived AJCC-7 Stage Grp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3440 as ItemNbr,
'Derived PreRx-7 T' as ItemName,
"Derived PreRx-7 T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3442 as ItemNbr,
'Derived PreRx-7 T Descrip' as ItemName,
"Derived PreRx-7 T Descrip" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3450 as ItemNbr,
'Derived PreRx-7 N' as ItemName,
"Derived PreRx-7 N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3452 as ItemNbr,
'Derived PreRx-7 N Descrip' as ItemName,
"Derived PreRx-7 N Descrip" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3460 as ItemNbr,
'Derived PreRx-7 M' as ItemName,
"Derived PreRx-7 M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3462 as ItemNbr,
'Derived PreRx-7 M Descrip' as ItemName,
"Derived PreRx-7 M Descrip" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3470 as ItemNbr,
'Derived PreRx-7 Stage Grp' as ItemName,
"Derived PreRx-7 Stage Grp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3480 as ItemNbr,
'Derived PostRx-7 T' as ItemName,
"Derived PostRx-7 T" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3482 as ItemNbr,
'Derived PostRx-7 N' as ItemName,
"Derived PostRx-7 N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3490 as ItemNbr,
'Derived PostRx-7 M' as ItemName,
"Derived PostRx-7 M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3492 as ItemNbr,
'Derived PostRx-7 Stge Grp' as ItemName,
"Derived PostRx-7 Stge Grp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3010 as ItemNbr,
'Derived SS1977' as ItemName,
"Derived SS1977" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3020 as ItemNbr,
'Derived SS2000' as ItemName,
"Derived SS2000" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3600 as ItemNbr,
'Derived Neoadjuv Rx Flag' as ItemName,
"Derived Neoadjuv Rx Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3030 as ItemNbr,
'Derived AJCC--Flag' as ItemName,
"Derived AJCC--Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3040 as ItemNbr,
'Derived SS1977--Flag' as ItemName,
"Derived SS1977--Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3050 as ItemNbr,
'Derived SS2000--Flag' as ItemName,
"Derived SS2000--Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2937 as ItemNbr,
'CS Version Input Current' as ItemName,
"CS Version Input Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2935 as ItemNbr,
'CS Version Input Original' as ItemName,
"CS Version Input Original" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2936 as ItemNbr,
'CS Version Derived' as ItemName,
"CS Version Derived" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3700 as ItemNbr,
'SEER Site-Specific Fact 1' as ItemName,
"SEER Site-Specific Fact 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3702 as ItemNbr,
'SEER Site-Specific Fact 2' as ItemName,
"SEER Site-Specific Fact 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3704 as ItemNbr,
'SEER Site-Specific Fact 3' as ItemName,
"SEER Site-Specific Fact 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3706 as ItemNbr,
'SEER Site-Specific Fact 4' as ItemName,
"SEER Site-Specific Fact 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3708 as ItemNbr,
'SEER Site-Specific Fact 5' as ItemName,
"SEER Site-Specific Fact 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3710 as ItemNbr,
'SEER Site-Specific Fact 6' as ItemName,
"SEER Site-Specific Fact 6" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3165 as ItemNbr,
'ICD Revision Comorbid' as ItemName,
"ICD Revision Comorbid" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3110 as ItemNbr,
'Comorbid/Complication 1' as ItemName,
"Comorbid/Complication 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3120 as ItemNbr,
'Comorbid/Complication 2' as ItemName,
"Comorbid/Complication 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3130 as ItemNbr,
'Comorbid/Complication 3' as ItemName,
"Comorbid/Complication 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3140 as ItemNbr,
'Comorbid/Complication 4' as ItemName,
"Comorbid/Complication 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3150 as ItemNbr,
'Comorbid/Complication 5' as ItemName,
"Comorbid/Complication 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3160 as ItemNbr,
'Comorbid/Complication 6' as ItemName,
"Comorbid/Complication 6" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3161 as ItemNbr,
'Comorbid/Complication 7' as ItemName,
"Comorbid/Complication 7" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3162 as ItemNbr,
'Comorbid/Complication 8' as ItemName,
"Comorbid/Complication 8" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3163 as ItemNbr,
'Comorbid/Complication 9' as ItemName,
"Comorbid/Complication 9" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3164 as ItemNbr,
'Comorbid/Complication 10' as ItemName,
"Comorbid/Complication 10" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1180 as ItemNbr,
'Reserved 05' as ItemName,
"Reserved 05" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1260 as ItemNbr,
'Date of Initial RX--SEER' as ItemName,
"Date of Initial RX--SEER" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1261 as ItemNbr,
'Date of Initial RX Flag' as ItemName,
"Date of Initial RX Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1270 as ItemNbr,
'Date of 1st Crs RX--CoC' as ItemName,
"Date of 1st Crs RX--CoC" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1271 as ItemNbr,
'Date of 1st Crs Rx Flag' as ItemName,
"Date of 1st Crs Rx Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1200 as ItemNbr,
'RX Date--Surgery' as ItemName,
"RX Date--Surgery" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1201 as ItemNbr,
'RX Date--Surgery Flag' as ItemName,
"RX Date--Surgery Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3170 as ItemNbr,
'RX Date--Most Defin Surg' as ItemName,
"RX Date--Most Defin Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3171 as ItemNbr,
'RX Date Mst Defn Srg Flag' as ItemName,
"RX Date Mst Defn Srg Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3180 as ItemNbr,
'RX Date--Surgical Disch' as ItemName,
"RX Date--Surgical Disch" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3181 as ItemNbr,
'RX Date Surg Disch Flag' as ItemName,
"RX Date Surg Disch Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1210 as ItemNbr,
'RX Date--Radiation' as ItemName,
"RX Date--Radiation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1211 as ItemNbr,
'RX Date--Radiation Flag' as ItemName,
"RX Date--Radiation Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3220 as ItemNbr,
'RX Date--Radiation Ended' as ItemName,
"RX Date--Radiation Ended" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3221 as ItemNbr,
'RX Date Rad Ended Flag' as ItemName,
"RX Date Rad Ended Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3230 as ItemNbr,
'RX Date--Systemic' as ItemName,
"RX Date--Systemic" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3231 as ItemNbr,
'RX Date Systemic Flag' as ItemName,
"RX Date Systemic Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1220 as ItemNbr,
'RX Date--Chemo' as ItemName,
"RX Date--Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1221 as ItemNbr,
'RX Date--Chemo Flag' as ItemName,
"RX Date--Chemo Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1230 as ItemNbr,
'RX Date--Hormone' as ItemName,
"RX Date--Hormone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1231 as ItemNbr,
'RX Date--Hormone Flag' as ItemName,
"RX Date--Hormone Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1240 as ItemNbr,
'RX Date--BRM' as ItemName,
"RX Date--BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1241 as ItemNbr,
'RX Date--BRM Flag' as ItemName,
"RX Date--BRM Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1250 as ItemNbr,
'RX Date--Other' as ItemName,
"RX Date--Other" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1251 as ItemNbr,
'RX Date--Other Flag' as ItemName,
"RX Date--Other Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1280 as ItemNbr,
'RX Date--DX/Stg Proc' as ItemName,
"RX Date--DX/Stg Proc" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1281 as ItemNbr,
'RX Date--Dx/Stg Proc Flag' as ItemName,
"RX Date--Dx/Stg Proc Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1285 as ItemNbr,
'RX Summ--Treatment Status' as ItemName,
"RX Summ--Treatment Status" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1290 as ItemNbr,
'RX Summ--Surg Prim Site' as ItemName,
"RX Summ--Surg Prim Site" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1292 as ItemNbr,
'RX Summ--Scope Reg LN Sur' as ItemName,
"RX Summ--Scope Reg LN Sur" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1294 as ItemNbr,
'RX Summ--Surg Oth Reg/Dis' as ItemName,
"RX Summ--Surg Oth Reg/Dis" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1296 as ItemNbr,
'RX Summ--Reg LN Examined' as ItemName,
"RX Summ--Reg LN Examined" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1310 as ItemNbr,
'RX Summ--Surgical Approch' as ItemName,
"RX Summ--Surgical Approch" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1320 as ItemNbr,
'RX Summ--Surgical Margins' as ItemName,
"RX Summ--Surgical Margins" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1330 as ItemNbr,
'RX Summ--Reconstruct 1st' as ItemName,
"RX Summ--Reconstruct 1st" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1340 as ItemNbr,
'Reason for No Surgery' as ItemName,
"Reason for No Surgery" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1350 as ItemNbr,
'RX Summ--DX/Stg Proc' as ItemName,
"RX Summ--DX/Stg Proc" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3270 as ItemNbr,
'RX Summ--Palliative Proc' as ItemName,
"RX Summ--Palliative Proc" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1360 as ItemNbr,
'RX Summ--Radiation' as ItemName,
"RX Summ--Radiation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1370 as ItemNbr,
'RX Summ--Rad to CNS' as ItemName,
"RX Summ--Rad to CNS" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1380 as ItemNbr,
'RX Summ--Surg/Rad Seq' as ItemName,
"RX Summ--Surg/Rad Seq" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3250 as ItemNbr,
'RX Summ--Transplnt/Endocr' as ItemName,
"RX Summ--Transplnt/Endocr" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1390 as ItemNbr,
'RX Summ--Chemo' as ItemName,
"RX Summ--Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1400 as ItemNbr,
'RX Summ--Hormone' as ItemName,
"RX Summ--Hormone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1410 as ItemNbr,
'RX Summ--BRM' as ItemName,
"RX Summ--BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1420 as ItemNbr,
'RX Summ--Other' as ItemName,
"RX Summ--Other" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1430 as ItemNbr,
'Reason for No Radiation' as ItemName,
"Reason for No Radiation" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1460 as ItemNbr,
'RX Coding System--Current' as ItemName,
"RX Coding System--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1500 as ItemNbr,
'First Course Calc Method' as ItemName,
"First Course Calc Method" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1510 as ItemNbr,
'Rad--Regional Dose: CGY' as ItemName,
"Rad--Regional Dose: CGY" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1520 as ItemNbr,
'Rad--No of Treatment Vol' as ItemName,
"Rad--No of Treatment Vol" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1540 as ItemNbr,
'Rad--Treatment Volume' as ItemName,
"Rad--Treatment Volume" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1550 as ItemNbr,
'Rad--Location of RX' as ItemName,
"Rad--Location of RX" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1570 as ItemNbr,
'Rad--Regional RX Modality' as ItemName,
"Rad--Regional RX Modality" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3200 as ItemNbr,
'Rad--Boost RX Modality' as ItemName,
"Rad--Boost RX Modality" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3210 as ItemNbr,
'Rad--Boost Dose cGy' as ItemName,
"Rad--Boost Dose cGy" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1639 as ItemNbr,
'RX Summ--Systemic/Sur Seq' as ItemName,
"RX Summ--Systemic/Sur Seq" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1640 as ItemNbr,
'RX Summ--Surgery Type' as ItemName,
"RX Summ--Surgery Type" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3190 as ItemNbr,
'Readm Same Hosp 30 Days' as ItemName,
"Readm Same Hosp 30 Days" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1646 as ItemNbr,
'RX Summ--Surg Site 98-02' as ItemName,
"RX Summ--Surg Site 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1647 as ItemNbr,
'RX Summ--Scope Reg 98-02' as ItemName,
"RX Summ--Scope Reg 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1648 as ItemNbr,
'RX Summ--Surg Oth 98-02' as ItemName,
"RX Summ--Surg Oth 98-02" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1190 as ItemNbr,
'Reserved 06' as ItemName,
"Reserved 06" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1660 as ItemNbr,
'Subsq RX 2nd Course Date' as ItemName,
"Subsq RX 2nd Course Date" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1661 as ItemNbr,
'Subsq RX 2ndCrs Date Flag' as ItemName,
"Subsq RX 2ndCrs Date Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1670 as ItemNbr,
'Subsq RX 2nd Course Codes' as ItemName,
"Subsq RX 2nd Course Codes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1671 as ItemNbr,
'Subsq RX 2nd Course Surg' as ItemName,
"Subsq RX 2nd Course Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1677 as ItemNbr,
'Subsq RX 2nd--Scope LN SU' as ItemName,
"Subsq RX 2nd--Scope LN SU" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1678 as ItemNbr,
'Subsq RX 2nd--Surg Oth' as ItemName,
"Subsq RX 2nd--Surg Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1679 as ItemNbr,
'Subsq RX 2nd--Reg LN Rem' as ItemName,
"Subsq RX 2nd--Reg LN Rem" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1672 as ItemNbr,
'Subsq RX 2nd Course Rad' as ItemName,
"Subsq RX 2nd Course Rad" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1673 as ItemNbr,
'Subsq RX 2nd Course Chemo' as ItemName,
"Subsq RX 2nd Course Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1674 as ItemNbr,
'Subsq RX 2nd Course Horm' as ItemName,
"Subsq RX 2nd Course Horm" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1675 as ItemNbr,
'Subsq RX 2nd Course BRM' as ItemName,
"Subsq RX 2nd Course BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1676 as ItemNbr,
'Subsq RX 2nd Course Oth' as ItemName,
"Subsq RX 2nd Course Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1680 as ItemNbr,
'Subsq RX 3rd Course Date' as ItemName,
"Subsq RX 3rd Course Date" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1681 as ItemNbr,
'Subsq RX 3rdCrs Date Flag' as ItemName,
"Subsq RX 3rdCrs Date Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1690 as ItemNbr,
'Subsq RX 3rd Course Codes' as ItemName,
"Subsq RX 3rd Course Codes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1691 as ItemNbr,
'Subsq RX 3rd Course Surg' as ItemName,
"Subsq RX 3rd Course Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1697 as ItemNbr,
'Subsq RX 3rd--Scope LN Su' as ItemName,
"Subsq RX 3rd--Scope LN Su" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1698 as ItemNbr,
'Subsq RX 3rd--Surg Oth' as ItemName,
"Subsq RX 3rd--Surg Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1699 as ItemNbr,
'Subsq RX 3rd--Reg LN Rem' as ItemName,
"Subsq RX 3rd--Reg LN Rem" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1692 as ItemNbr,
'Subsq RX 3rd Course Rad' as ItemName,
"Subsq RX 3rd Course Rad" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1693 as ItemNbr,
'Subsq RX 3rd Course Chemo' as ItemName,
"Subsq RX 3rd Course Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1694 as ItemNbr,
'Subsq RX 3rd Course Horm' as ItemName,
"Subsq RX 3rd Course Horm" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1695 as ItemNbr,
'Subsq RX 3rd Course BRM' as ItemName,
"Subsq RX 3rd Course BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1696 as ItemNbr,
'Subsq RX 3rd Course Oth' as ItemName,
"Subsq RX 3rd Course Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1700 as ItemNbr,
'Subsq RX 4th Course Date' as ItemName,
"Subsq RX 4th Course Date" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1701 as ItemNbr,
'Subsq RX 4thCrs Date Flag' as ItemName,
"Subsq RX 4thCrs Date Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1710 as ItemNbr,
'Subsq RX 4th Course Codes' as ItemName,
"Subsq RX 4th Course Codes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1711 as ItemNbr,
'Subsq RX 4th Course Surg' as ItemName,
"Subsq RX 4th Course Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1717 as ItemNbr,
'Subsq RX 4th--Scope LN Su' as ItemName,
"Subsq RX 4th--Scope LN Su" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1718 as ItemNbr,
'Subsq RX 4th--Surg Oth' as ItemName,
"Subsq RX 4th--Surg Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1719 as ItemNbr,
'Subsq RX 4th--Reg LN Rem' as ItemName,
"Subsq RX 4th--Reg LN Rem" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1712 as ItemNbr,
'Subsq RX 4th Course Rad' as ItemName,
"Subsq RX 4th Course Rad" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1713 as ItemNbr,
'Subsq RX 4th Course Chemo' as ItemName,
"Subsq RX 4th Course Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1714 as ItemNbr,
'Subsq RX 4th Course Horm' as ItemName,
"Subsq RX 4th Course Horm" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1715 as ItemNbr,
'Subsq RX 4th Course BRM' as ItemName,
"Subsq RX 4th Course BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1716 as ItemNbr,
'Subsq RX 4th Course Oth' as ItemName,
"Subsq RX 4th Course Oth" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1741 as ItemNbr,
'Subsq RX--Reconstruct Del' as ItemName,
"Subsq RX--Reconstruct Del" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1300 as ItemNbr,
'Reserved 07' as ItemName,
"Reserved 07" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1981 as ItemNbr,
'Over-ride SS/NodesPos' as ItemName,
"Over-ride SS/NodesPos" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1982 as ItemNbr,
'Over-ride SS/TNM-N' as ItemName,
"Over-ride SS/TNM-N" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1983 as ItemNbr,
'Over-ride SS/TNM-M' as ItemName,
"Over-ride SS/TNM-M" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1985 as ItemNbr,
'Over-ride Acsn/Class/Seq' as ItemName,
"Over-ride Acsn/Class/Seq" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1986 as ItemNbr,
'Over-ride HospSeq/DxConf' as ItemName,
"Over-ride HospSeq/DxConf" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1987 as ItemNbr,
'Over-ride CoC-Site/Type' as ItemName,
"Over-ride CoC-Site/Type" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1988 as ItemNbr,
'Over-ride HospSeq/Site' as ItemName,
"Over-ride HospSeq/Site" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1989 as ItemNbr,
'Over-ride Site/TNM-StgGrp' as ItemName,
"Over-ride Site/TNM-StgGrp" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1990 as ItemNbr,
'Over-ride Age/Site/Morph' as ItemName,
"Over-ride Age/Site/Morph" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2000 as ItemNbr,
'Over-ride SeqNo/DxConf' as ItemName,
"Over-ride SeqNo/DxConf" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2010 as ItemNbr,
'Over-ride Site/Lat/SeqNo' as ItemName,
"Over-ride Site/Lat/SeqNo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2020 as ItemNbr,
'Over-ride Surg/DxConf' as ItemName,
"Over-ride Surg/DxConf" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2030 as ItemNbr,
'Over-ride Site/Type' as ItemName,
"Over-ride Site/Type" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2040 as ItemNbr,
'Over-ride Histology' as ItemName,
"Over-ride Histology" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2050 as ItemNbr,
'Over-ride Report Source' as ItemName,
"Over-ride Report Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2060 as ItemNbr,
'Over-ride Ill-define Site' as ItemName,
"Over-ride Ill-define Site" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2070 as ItemNbr,
'Over-ride Leuk, Lymphoma' as ItemName,
"Over-ride Leuk, Lymphoma" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2071 as ItemNbr,
'Over-ride Site/Behavior' as ItemName,
"Over-ride Site/Behavior" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2072 as ItemNbr,
'Over-ride Site/EOD/DX Dt' as ItemName,
"Over-ride Site/EOD/DX Dt" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2073 as ItemNbr,
'Over-ride Site/Lat/EOD' as ItemName,
"Over-ride Site/Lat/EOD" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2074 as ItemNbr,
'Over-ride Site/Lat/Morph' as ItemName,
"Over-ride Site/Lat/Morph" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1960 as ItemNbr,
'Site (73-91) ICD-O-1' as ItemName,
"Site (73-91) ICD-O-1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1970 as ItemNbr,
'Morph (73-91) ICD-O-1' as ItemName,
"Morph (73-91) ICD-O-1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1971 as ItemNbr,
'Histology (73-91) ICD-O-1' as ItemName,
"Histology (73-91) ICD-O-1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1972 as ItemNbr,
'Behavior (73-91) ICD-O-1' as ItemName,
"Behavior (73-91) ICD-O-1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1973 as ItemNbr,
'Grade (73-91) ICD-O-1' as ItemName,
"Grade (73-91) ICD-O-1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1980 as ItemNbr,
'ICD-O-2 Conversion Flag' as ItemName,
"ICD-O-2 Conversion Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2081 as ItemNbr,
'CRC CHECKSUM' as ItemName,
"CRC CHECKSUM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2120 as ItemNbr,
'SEER Coding Sys--Current' as ItemName,
"SEER Coding Sys--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2130 as ItemNbr,
'SEER Coding Sys--Original' as ItemName,
"SEER Coding Sys--Original" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2140 as ItemNbr,
'CoC Coding Sys--Current' as ItemName,
"CoC Coding Sys--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2150 as ItemNbr,
'CoC Coding Sys--Original' as ItemName,
"CoC Coding Sys--Original" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2170 as ItemNbr,
'Vendor Name' as ItemName,
"Vendor Name" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2180 as ItemNbr,
'SEER Type of Follow-Up' as ItemName,
"SEER Type of Follow-Up" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2190 as ItemNbr,
'SEER Record Number' as ItemName,
"SEER Record Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2200 as ItemNbr,
'Diagnostic Proc 73-87' as ItemName,
"Diagnostic Proc 73-87" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2085 as ItemNbr,
'Date Case Initiated' as ItemName,
"Date Case Initiated" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2090 as ItemNbr,
'Date Case Completed' as ItemName,
"Date Case Completed" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2092 as ItemNbr,
'Date Case Completed--CoC' as ItemName,
"Date Case Completed--CoC" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2100 as ItemNbr,
'Date Case Last Changed' as ItemName,
"Date Case Last Changed" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2110 as ItemNbr,
'Date Case Report Exported' as ItemName,
"Date Case Report Exported" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2111 as ItemNbr,
'Date Case Report Received' as ItemName,
"Date Case Report Received" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2112 as ItemNbr,
'Date Case Report Loaded' as ItemName,
"Date Case Report Loaded" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2113 as ItemNbr,
'Date Tumor Record Availbl' as ItemName,
"Date Tumor Record Availbl" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2116 as ItemNbr,
'ICD-O-3 Conversion Flag' as ItemName,
"ICD-O-3 Conversion Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3750 as ItemNbr,
'Over-ride CS 1' as ItemName,
"Over-ride CS 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3751 as ItemNbr,
'Over-ride CS 2' as ItemName,
"Over-ride CS 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3752 as ItemNbr,
'Over-ride CS 3' as ItemName,
"Over-ride CS 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3753 as ItemNbr,
'Over-ride CS 4' as ItemName,
"Over-ride CS 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3754 as ItemNbr,
'Over-ride CS 5' as ItemName,
"Over-ride CS 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3755 as ItemNbr,
'Over-ride CS 6' as ItemName,
"Over-ride CS 6" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3756 as ItemNbr,
'Over-ride CS 7' as ItemName,
"Over-ride CS 7" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3757 as ItemNbr,
'Over-ride CS 8' as ItemName,
"Over-ride CS 8" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3758 as ItemNbr,
'Over-ride CS 9' as ItemName,
"Over-ride CS 9" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3759 as ItemNbr,
'Over-ride CS 10' as ItemName,
"Over-ride CS 10" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3760 as ItemNbr,
'Over-ride CS 11' as ItemName,
"Over-ride CS 11" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3761 as ItemNbr,
'Over-ride CS 12' as ItemName,
"Over-ride CS 12" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3762 as ItemNbr,
'Over-ride CS 13' as ItemName,
"Over-ride CS 13" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3763 as ItemNbr,
'Over-ride CS 14' as ItemName,
"Over-ride CS 14" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3764 as ItemNbr,
'Over-ride CS 15' as ItemName,
"Over-ride CS 15" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3765 as ItemNbr,
'Over-ride CS 16' as ItemName,
"Over-ride CS 16" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3766 as ItemNbr,
'Over-ride CS 17' as ItemName,
"Over-ride CS 17" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3767 as ItemNbr,
'Over-ride CS 18' as ItemName,
"Over-ride CS 18" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3768 as ItemNbr,
'Over-ride CS 19' as ItemName,
"Over-ride CS 19" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
3769 as ItemNbr,
'Over-ride CS 20' as ItemName,
"Over-ride CS 20" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1650 as ItemNbr,
'Reserved 08' as ItemName,
"Reserved 08" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1750 as ItemNbr,
'Date of Last Contact' as ItemName,
"Date of Last Contact" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1751 as ItemNbr,
'Date of Last Contact Flag' as ItemName,
"Date of Last Contact Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1760 as ItemNbr,
'Vital Status' as ItemName,
"Vital Status" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1770 as ItemNbr,
'Cancer Status' as ItemName,
"Cancer Status" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1780 as ItemNbr,
'Quality of Survival' as ItemName,
"Quality of Survival" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1790 as ItemNbr,
'Follow-Up Source' as ItemName,
"Follow-Up Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1800 as ItemNbr,
'Next Follow-Up Source' as ItemName,
"Next Follow-Up Source" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1810 as ItemNbr,
'Addr Current--City' as ItemName,
"Addr Current--City" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1820 as ItemNbr,
'Addr Current--State' as ItemName,
"Addr Current--State" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1830 as ItemNbr,
'Addr Current--Postal Code' as ItemName,
"Addr Current--Postal Code" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1840 as ItemNbr,
'County--Current' as ItemName,
"County--Current" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1850 as ItemNbr,
'Unusual Follow-Up Method' as ItemName,
"Unusual Follow-Up Method" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1860 as ItemNbr,
'Recurrence Date--1st' as ItemName,
"Recurrence Date--1st" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1861 as ItemNbr,
'Recurrence Date--1st Flag' as ItemName,
"Recurrence Date--1st Flag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1880 as ItemNbr,
'Recurrence Type--1st' as ItemName,
"Recurrence Type--1st" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1842 as ItemNbr,
'Follow-Up Contact--City' as ItemName,
"Follow-Up Contact--City" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1844 as ItemNbr,
'Follow-Up Contact--State' as ItemName,
"Follow-Up Contact--State" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1846 as ItemNbr,
'Follow-Up Contact--Postal' as ItemName,
"Follow-Up Contact--Postal" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1910 as ItemNbr,
'Cause of Death' as ItemName,
"Cause of Death" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1920 as ItemNbr,
'ICD Revision Number' as ItemName,
"ICD Revision Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1930 as ItemNbr,
'Autopsy' as ItemName,
"Autopsy" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1940 as ItemNbr,
'Place of Death' as ItemName,
"Place of Death" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1791 as ItemNbr,
'Follow-up Source Central' as ItemName,
"Follow-up Source Central" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1755 as ItemNbr,
'Date of Death--Canada' as ItemName,
"Date of Death--Canada" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1756 as ItemNbr,
'Date of Death--CanadaFlag' as ItemName,
"Date of Death--CanadaFlag" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1740 as ItemNbr,
'Reserved 09' as ItemName,
"Reserved 09" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2220 as ItemNbr,
'State/Requestor Items' as ItemName,
"State/Requestor Items" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2230 as ItemNbr,
'Name--Last' as ItemName,
"Name--Last" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2240 as ItemNbr,
'Name--First' as ItemName,
"Name--First" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2250 as ItemNbr,
'Name--Middle' as ItemName,
"Name--Middle" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2260 as ItemNbr,
'Name--Prefix' as ItemName,
"Name--Prefix" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2270 as ItemNbr,
'Name--Suffix' as ItemName,
"Name--Suffix" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2280 as ItemNbr,
'Name--Alias' as ItemName,
"Name--Alias" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2390 as ItemNbr,
'Name--Maiden' as ItemName,
"Name--Maiden" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2290 as ItemNbr,
'Name--Spouse/Parent' as ItemName,
"Name--Spouse/Parent" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2300 as ItemNbr,
'Medical Record Number' as ItemName,
"Medical Record Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2310 as ItemNbr,
'Military Record No Suffix' as ItemName,
"Military Record No Suffix" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2320 as ItemNbr,
'Social Security Number' as ItemName,
"Social Security Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2330 as ItemNbr,
'Addr at DX--No & Street' as ItemName,
"Addr at DX--No & Street" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2335 as ItemNbr,
'Addr at DX--Supplementl' as ItemName,
"Addr at DX--Supplementl" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2350 as ItemNbr,
'Addr Current--No & Street' as ItemName,
"Addr Current--No & Street" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2355 as ItemNbr,
'Addr Current--Supplementl' as ItemName,
"Addr Current--Supplementl" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2360 as ItemNbr,
'Telephone' as ItemName,
"Telephone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2380 as ItemNbr,
'DC State File Number' as ItemName,
"DC State File Number" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2394 as ItemNbr,
'Follow-Up Contact--Name' as ItemName,
"Follow-Up Contact--Name" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2392 as ItemNbr,
'Follow-Up Contact--No&St' as ItemName,
"Follow-Up Contact--No&St" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2393 as ItemNbr,
'Follow-Up Contact--Suppl' as ItemName,
"Follow-Up Contact--Suppl" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2352 as ItemNbr,
'Latitude' as ItemName,
"Latitude" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2354 as ItemNbr,
'Longitude' as ItemName,
"Longitude" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1835 as ItemNbr,
'Reserved 10' as ItemName,
"Reserved 10" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2445 as ItemNbr,
'NPI--Following Registry' as ItemName,
"NPI--Following Registry" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2440 as ItemNbr,
'Following Registry' as ItemName,
"Following Registry" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2415 as ItemNbr,
'NPI--Inst Referred From' as ItemName,
"NPI--Inst Referred From" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2410 as ItemNbr,
'Institution Referred From' as ItemName,
"Institution Referred From" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2425 as ItemNbr,
'NPI--Inst Referred To' as ItemName,
"NPI--Inst Referred To" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2420 as ItemNbr,
'Institution Referred To' as ItemName,
"Institution Referred To" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
1900 as ItemNbr,
'Reserved 11' as ItemName,
"Reserved 11" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2465 as ItemNbr,
'NPI--Physician--Managing' as ItemName,
"NPI--Physician--Managing" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2460 as ItemNbr,
'Physician--Managing' as ItemName,
"Physician--Managing" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2475 as ItemNbr,
'NPI--Physician--Follow-Up' as ItemName,
"NPI--Physician--Follow-Up" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2470 as ItemNbr,
'Physician--Follow-Up' as ItemName,
"Physician--Follow-Up" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2485 as ItemNbr,
'NPI--Physician--Primary Surg' as ItemName,
"NPI--Physician--Primary Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2480 as ItemNbr,
'Physician--Primary Surg' as ItemName,
"Physician--Primary Surg" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2495 as ItemNbr,
'NPI--Physician 3' as ItemName,
"NPI--Physician 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2490 as ItemNbr,
'Physician 3' as ItemName,
"Physician 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2505 as ItemNbr,
'NPI--Physician 4' as ItemName,
"NPI--Physician 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2500 as ItemNbr,
'Physician 4' as ItemName,
"Physician 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2510 as ItemNbr,
'Reserved 12' as ItemName,
"Reserved 12" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7010 as ItemNbr,
'Path Reporting Fac ID 1' as ItemName,
"Path Reporting Fac ID 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7090 as ItemNbr,
'Path Report Number 1' as ItemName,
"Path Report Number 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7320 as ItemNbr,
'Path Date Spec Collect 1' as ItemName,
"Path Date Spec Collect 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7480 as ItemNbr,
'Path Report Type 1' as ItemName,
"Path Report Type 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7190 as ItemNbr,
'Path Ordering Fac No 1' as ItemName,
"Path Ordering Fac No 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7100 as ItemNbr,
'Path Order Phys Lic No 1' as ItemName,
"Path Order Phys Lic No 1" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7011 as ItemNbr,
'Path Reporting Fac ID 2' as ItemName,
"Path Reporting Fac ID 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7091 as ItemNbr,
'Path Report Number 2' as ItemName,
"Path Report Number 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7321 as ItemNbr,
'Path Date Spec Collect 2' as ItemName,
"Path Date Spec Collect 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7481 as ItemNbr,
'Path Report Type 2' as ItemName,
"Path Report Type 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7191 as ItemNbr,
'Path Ordering Fac No 2' as ItemName,
"Path Ordering Fac No 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7101 as ItemNbr,
'Path Order Phys Lic No 2' as ItemName,
"Path Order Phys Lic No 2" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7012 as ItemNbr,
'Path Reporting Fac ID 3' as ItemName,
"Path Reporting Fac ID 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7092 as ItemNbr,
'Path Report Number 3' as ItemName,
"Path Report Number 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7322 as ItemNbr,
'Path Date Spec Collect 3' as ItemName,
"Path Date Spec Collect 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7482 as ItemNbr,
'Path Report Type 3' as ItemName,
"Path Report Type 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7192 as ItemNbr,
'Path Ordering Fac No 3' as ItemName,
"Path Ordering Fac No 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7102 as ItemNbr,
'Path Order Phys Lic No 3' as ItemName,
"Path Order Phys Lic No 3" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7013 as ItemNbr,
'Path Reporting Fac ID 4' as ItemName,
"Path Reporting Fac ID 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7093 as ItemNbr,
'Path Report Number 4' as ItemName,
"Path Report Number 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7323 as ItemNbr,
'Path Date Spec Collect 4' as ItemName,
"Path Date Spec Collect 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7483 as ItemNbr,
'Path Report Type 4' as ItemName,
"Path Report Type 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7193 as ItemNbr,
'Path Ordering Fac No 4' as ItemName,
"Path Ordering Fac No 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7103 as ItemNbr,
'Path Order Phys Lic No 4' as ItemName,
"Path Order Phys Lic No 4" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7014 as ItemNbr,
'Path Reporting Fac ID 5' as ItemName,
"Path Reporting Fac ID 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7094 as ItemNbr,
'Path Report Number 5' as ItemName,
"Path Report Number 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7324 as ItemNbr,
'Path Date Spec Collect 5' as ItemName,
"Path Date Spec Collect 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7484 as ItemNbr,
'Path Report Type 5' as ItemName,
"Path Report Type 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7194 as ItemNbr,
'Path Ordering Fac No 5' as ItemName,
"Path Ordering Fac No 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
7104 as ItemNbr,
'Path Order Phys Lic No 5' as ItemName,
"Path Order Phys Lic No 5" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2080 as ItemNbr,
'Reserved 13' as ItemName,
"Reserved 13" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2520 as ItemNbr,
'Text--DX Proc--PE' as ItemName,
"Text--DX Proc--PE" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2530 as ItemNbr,
'Text--DX Proc--X-ray/Scan' as ItemName,
"Text--DX Proc--X-ray/Scan" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2540 as ItemNbr,
'Text--DX Proc--Scopes' as ItemName,
"Text--DX Proc--Scopes" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2550 as ItemNbr,
'Text--DX Proc--Lab Tests' as ItemName,
"Text--DX Proc--Lab Tests" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2560 as ItemNbr,
'Text--DX Proc--Op' as ItemName,
"Text--DX Proc--Op" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2570 as ItemNbr,
'Text--DX Proc--Path' as ItemName,
"Text--DX Proc--Path" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2580 as ItemNbr,
'Text--Primary Site Title' as ItemName,
"Text--Primary Site Title" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2590 as ItemNbr,
'Text--Histology Title' as ItemName,
"Text--Histology Title" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2600 as ItemNbr,
'Text--Staging' as ItemName,
"Text--Staging" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2610 as ItemNbr,
'RX Text--Surgery' as ItemName,
"RX Text--Surgery" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2620 as ItemNbr,
'RX Text--Radiation (Beam)' as ItemName,
"RX Text--Radiation (Beam)" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2630 as ItemNbr,
'RX Text--Radiation Other' as ItemName,
"RX Text--Radiation Other" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2640 as ItemNbr,
'RX Text--Chemo' as ItemName,
"RX Text--Chemo" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2650 as ItemNbr,
'RX Text--Hormone' as ItemName,
"RX Text--Hormone" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2660 as ItemNbr,
'RX Text--BRM' as ItemName,
"RX Text--BRM" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2670 as ItemNbr,
'RX Text--Other' as ItemName,
"RX Text--Other" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2680 as ItemNbr,
'Text--Remarks' as ItemName,
"Text--Remarks" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2690 as ItemNbr,
'Text--Place of Diagnosis' as ItemName,
"Text--Place of Diagnosis" as value
from "NAACR"."EXTRACT"
union all
select case_index, 
2210 as ItemNbr,
'Reserved 14' as ItemName,
"Reserved 14" as value
from "NAACR"."EXTRACT";
