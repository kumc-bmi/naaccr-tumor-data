drop view if exists naaccr_fields_raw;
create view naaccr_fields_raw as
select substr(value, 1, 1) as recordType
     , substr(value, 2, 1) as registryType
     , substr(value, 17, 3) as naaccrRecordVersion
     , substr(value, 20, 10) as npiRegistryId
     , substr(value, 30, 10) as registryId
     , substr(value, 40, 2) as tumorRecordNumber
     , substr(value, 42, 8) as patientIdNumber
     , substr(value, 50, 8) as patientSystemIdHosp
     , substr(value, 74, 50) as addrAtDxCity
     , substr(value, 124, 2) as addrAtDxState
     , substr(value, 126, 9) as addrAtDxPostalCode
     , substr(value, 135, 3) as countyAtDx
     , substr(value, 150, 3) as countyAtDxAnalysis
     , substr(value, 153, 2) as stateAtDxGeocode19708090
     , substr(value, 155, 3) as countyAtDxGeocode1990
     , substr(value, 158, 6) as censusTract19708090
     , substr(value, 164, 1) as censusBlockGrp197090
     , substr(value, 165, 1) as censusCodSys19708090
     , substr(value, 166, 1) as censusTrCert19708090
     , substr(value, 167, 2) as stateAtDxGeocode2000
     , substr(value, 169, 3) as countyAtDxGeocode2000
     , substr(value, 172, 6) as censusTract2000
     , substr(value, 178, 1) as censusBlockGroup2000
     , substr(value, 179, 1) as censusTrCertainty2000
     , substr(value, 180, 2) as stateAtDxGeocode2010
     , substr(value, 182, 3) as countyAtDxGeocode2010
     , substr(value, 185, 6) as censusTract2010
     , substr(value, 191, 1) as censusBlockGroup2010
     , substr(value, 192, 1) as censusTrCertainty2010
     , substr(value, 193, 2) as stateAtDxGeocode2020
     , substr(value, 195, 3) as countyAtDxGeocode2020
     , substr(value, 198, 6) as censusTract2020
     , substr(value, 204, 1) as censusBlockGroup2020
     , substr(value, 205, 1) as censusTractCertainty2020
     , substr(value, 206, 1) as maritalStatusAtDx
     , substr(value, 207, 2) as race1
     , substr(value, 209, 2) as race2
     , substr(value, 211, 2) as race3
     , substr(value, 213, 2) as race4
     , substr(value, 215, 2) as race5
     , substr(value, 217, 1) as raceCodingSysCurrent
     , substr(value, 218, 1) as raceCodingSysOriginal
     , substr(value, 219, 1) as spanishHispanicOrigin
     , substr(value, 220, 1) as computedEthnicity
     , substr(value, 221, 1) as computedEthnicitySource
     , substr(value, 222, 1) as sex
     , substr(value, 223, 3) as ageAtDiagnosis
     , substr(value, 226, 8) as dateOfBirth
     , substr(value, 234, 2) as dateOfBirthFlag
     , substr(value, 236, 3) as birthplace
     , substr(value, 239, 3) as censusOccCode19702000
     , substr(value, 242, 3) as censusIndCode19702000
     , substr(value, 245, 1) as occupationSource
     , substr(value, 246, 1) as industrySource
     , substr(value, 247, 100) as textUsualOccupation
     , substr(value, 347, 100) as textUsualIndustry
     , substr(value, 447, 1) as censusOccIndSys7000
     , substr(value, 448, 1) as nhiaDerivedHispOrigin
     , substr(value, 449, 2) as raceNapiia
     , substr(value, 451, 1) as ihsLink
     , substr(value, 452, 2) as gisCoordinateQuality
     , substr(value, 454, 2) as ruralurbanContinuum1993
     , substr(value, 456, 2) as ruralurbanContinuum2003
     , substr(value, 458, 2) as ruralurbanContinuum2013
     , substr(value, 460, 1) as ruca2000
     , substr(value, 461, 1) as ruca2010
     , substr(value, 462, 1) as uric2000
     , substr(value, 463, 1) as uric2010
     , substr(value, 464, 3) as addrAtDxCountry
     , substr(value, 467, 3) as addrCurrentCountry
     , substr(value, 470, 2) as birthplaceState
     , substr(value, 472, 3) as birthplaceCountry
     , substr(value, 475, 3) as followupContactCountry
     , substr(value, 478, 2) as placeOfDeathState
     , substr(value, 480, 3) as placeOfDeathCountry
     , substr(value, 483, 4) as censusIndCode2010
     , substr(value, 487, 4) as censusOccCode2010
     , substr(value, 491, 1) as censusTrPovertyIndictr
     , substr(value, 542, 2) as sequenceNumberCentral
     , substr(value, 544, 8) as dateOfDiagnosis
     , substr(value, 552, 2) as dateOfDiagnosisFlag
     , substr(value, 554, 4) as primarySite
     , substr(value, 558, 1) as laterality
     , substr(value, 559, 4) as histologyIcdO2
     , substr(value, 563, 1) as behaviorIcdO2
     , substr(value, 564, 4) as histologicTypeIcdO3
     , substr(value, 568, 1) as behaviorCodeIcdO3
     , substr(value, 569, 1) as grade
     , substr(value, 570, 1) as gradePathValue
     , substr(value, 571, 1) as gradePathSystem
     , substr(value, 572, 1) as siteCodingSysCurrent
     , substr(value, 573, 1) as siteCodingSysOriginal
     , substr(value, 574, 1) as morphCodingSysCurrent
     , substr(value, 575, 1) as morphCodingSysOriginl
     , substr(value, 576, 1) as diagnosticConfirmation
     , substr(value, 577, 1) as typeOfReportingSource
     , substr(value, 578, 2) as casefindingSource
     , substr(value, 580, 1) as ambiguousTerminologyDx
     , substr(value, 581, 8) as dateConclusiveDx
     , substr(value, 589, 2) as dateConclusiveDxFlag
     , substr(value, 591, 2) as multTumRptAsOnePrim
     , substr(value, 593, 8) as dateOfMultTumors
     , substr(value, 601, 2) as dateOfMultTumorsFlag
     , substr(value, 603, 2) as multiplicityCounter
     , substr(value, 705, 10) as npiReportingFacility
     , substr(value, 715, 10) as reportingFacility
     , substr(value, 725, 10) as npiArchiveFin
     , substr(value, 735, 10) as archiveFin
     , substr(value, 745, 9) as accessionNumberHosp
     , substr(value, 754, 2) as sequenceNumberHospital
     , substr(value, 756, 3) as abstractedBy
     , substr(value, 759, 8) as dateOf1stContact
     , substr(value, 767, 2) as dateOf1stContactFlag
     , substr(value, 769, 8) as dateOfInptAdm
     , substr(value, 777, 2) as dateOfInptAdmFlag
     , substr(value, 779, 8) as dateOfInptDisch
     , substr(value, 787, 2) as dateOfInptDischFlag
     , substr(value, 789, 1) as inpatientStatus
     , substr(value, 790, 2) as classOfCase
     , substr(value, 792, 2) as primaryPayerAtDx
     , substr(value, 794, 1) as rxHospSurgApp2010
     , substr(value, 795, 2) as rxHospSurgPrimSite
     , substr(value, 797, 1) as rxHospScopeRegLnSur
     , substr(value, 798, 1) as rxHospSurgOthRegDis
     , substr(value, 799, 2) as rxHospRegLnRemoved
     , substr(value, 801, 1) as rxHospRadiation
     , substr(value, 802, 2) as rxHospChemo
     , substr(value, 804, 2) as rxHospHormone
     , substr(value, 806, 2) as rxHospBrm
     , substr(value, 808, 1) as rxHospOther
     , substr(value, 809, 2) as rxHospDxStgProc
     , substr(value, 811, 1) as rxHospPalliativeProc
     , substr(value, 812, 2) as rxHospSurgSite9802
     , substr(value, 814, 1) as rxHospScopeReg9802
     , substr(value, 815, 1) as rxHospSurgOth9802
     , substr(value, 866, 2) as tnmPathStagedBy
     , substr(value, 868, 2) as tnmClinStagedBy
     , substr(value, 870, 1) as metsAtDxBone
     , substr(value, 871, 1) as metsAtDxBrain
     , substr(value, 872, 1) as metsAtDxDistantLn
     , substr(value, 873, 1) as metsAtDxLiver
     , substr(value, 874, 1) as metsAtDxLung
     , substr(value, 875, 1) as metsAtDxOther
     , substr(value, 876, 3) as tumorSizeClinical
     , substr(value, 879, 3) as tumorSizePathologic
     , substr(value, 882, 3) as tumorSizeSummary
     , substr(value, 885, 5) as derivedSeerPathStgGrp
     , substr(value, 890, 5) as derivedSeerClinStgGrp
     , substr(value, 895, 5) as derivedSeerCmbStgGrp
     , substr(value, 900, 5) as derivedSeerCombinedT
     , substr(value, 905, 5) as derivedSeerCombinedN
     , substr(value, 910, 5) as derivedSeerCombinedM
     , substr(value, 915, 1) as derivedSeerCmbTSrc
     , substr(value, 916, 1) as derivedSeerCmbNSrc
     , substr(value, 917, 1) as derivedSeerCmbMSrc
     , substr(value, 918, 3) as eodPrimaryTumor
     , substr(value, 921, 3) as eodRegionalNodes
     , substr(value, 924, 2) as eodMets
     , substr(value, 926, 15) as derivedEod2018T
     , substr(value, 941, 15) as derivedEod2018N
     , substr(value, 956, 15) as derivedEod2018M
     , substr(value, 971, 15) as derivedEod2018StageGroup
     , substr(value, 986, 1) as derivedSummaryStage2018
     , substr(value, 987, 1) as summaryStage2018
     , substr(value, 988, 1) as seerSummaryStage2000
     , substr(value, 989, 1) as seerSummaryStage1977
     , substr(value, 990, 3) as eodTumorSize
     , substr(value, 993, 2) as eodExtension
     , substr(value, 995, 2) as eodExtensionProstPath
     , substr(value, 997, 1) as eodLymphNodeInvolv
     , substr(value, 998, 2) as regionalNodesPositive
     , substr(value, 1000, 2) as regionalNodesExamined
     , substr(value, 1002, 8) as dateRegionalLNDissection
     , substr(value, 1010, 2) as dateRegionalLNDissectionFlag
     , substr(value, 1012, 2) as sentinelLymphNodesPositive
     , substr(value, 1014, 2) as sentinelLymphNodesExamined
     , substr(value, 1016, 8) as dateSentinelLymphNodeBiopsy
     , substr(value, 1024, 2) as dateSentinelLymphNodeBiopsyFlag
     , substr(value, 1026, 13) as eodOld13Digit
     , substr(value, 1039, 2) as eodOld2Digit
     , substr(value, 1041, 4) as eodOld4Digit
     , substr(value, 1045, 1) as codingSystemForEod
     , substr(value, 1046, 2) as tnmEditionNumber
     , substr(value, 1048, 4) as tnmPathT
     , substr(value, 1052, 4) as tnmPathN
     , substr(value, 1056, 4) as tnmPathM
     , substr(value, 1060, 4) as tnmPathStageGroup
     , substr(value, 1064, 1) as tnmPathDescriptor
     , substr(value, 1065, 4) as tnmClinT
     , substr(value, 1069, 4) as tnmClinN
     , substr(value, 1073, 4) as tnmClinM
     , substr(value, 1077, 4) as tnmClinStageGroup
     , substr(value, 1081, 1) as tnmClinDescriptor
     , substr(value, 1082, 15) as ajccTnmClinT
     , substr(value, 1097, 4) as ajccTnmClinTSuffix
     , substr(value, 1101, 15) as ajccTnmClinN
     , substr(value, 1116, 4) as ajccTnmClinNSuffix
     , substr(value, 1120, 15) as ajccTnmClinM
     , substr(value, 1135, 15) as ajccTnmClinStageGroup
     , substr(value, 1150, 15) as ajccTnmPathT
     , substr(value, 1165, 4) as ajccTnmPathTSuffix
     , substr(value, 1169, 15) as ajccTnmPathN
     , substr(value, 1184, 4) as ajccTnmPathNSuffix
     , substr(value, 1188, 15) as ajccTnmPathM
     , substr(value, 1203, 15) as ajccTnmPathStageGroup
     , substr(value, 1218, 15) as ajccTnmPostTherapyT
     , substr(value, 1233, 4) as ajccTnmPostTherapyTSuffix
     , substr(value, 1237, 15) as ajccTnmPostTherapyN
     , substr(value, 1252, 4) as ajccTnmPostTherapyNSuffix
     , substr(value, 1256, 15) as ajccTnmPostTherapyM
     , substr(value, 1271, 15) as ajccTnmPostTherapyStageGroup
     , substr(value, 1286, 1) as gradeClinical
     , substr(value, 1287, 1) as gradePathological
     , substr(value, 1288, 1) as gradePostTherapy
     , substr(value, 1289, 2) as pediatricStage
     , substr(value, 1291, 2) as pediatricStagingSystem
     , substr(value, 1293, 1) as pediatricStagedBy
     , substr(value, 1294, 1) as tumorMarker1
     , substr(value, 1295, 1) as tumorMarker2
     , substr(value, 1296, 1) as tumorMarker3
     , substr(value, 1297, 1) as lymphVascularInvasion
     , substr(value, 1298, 3) as csTumorSize
     , substr(value, 1301, 3) as csExtension
     , substr(value, 1304, 1) as csTumorSizeExtEval
     , substr(value, 1305, 3) as csLymphNodes
     , substr(value, 1308, 1) as csLymphNodesEval
     , substr(value, 1309, 2) as csMetsAtDx
     , substr(value, 1311, 1) as csMetsEval
     , substr(value, 1312, 1) as csMetsAtDxBone
     , substr(value, 1313, 1) as csMetsAtDxBrain
     , substr(value, 1314, 1) as csMetsAtDxLiver
     , substr(value, 1315, 1) as csMetsAtDxLung
     , substr(value, 1316, 3) as csSiteSpecificFactor1
     , substr(value, 1319, 3) as csSiteSpecificFactor2
     , substr(value, 1322, 3) as csSiteSpecificFactor3
     , substr(value, 1325, 3) as csSiteSpecificFactor4
     , substr(value, 1328, 3) as csSiteSpecificFactor5
     , substr(value, 1331, 3) as csSiteSpecificFactor6
     , substr(value, 1334, 3) as csSiteSpecificFactor7
     , substr(value, 1337, 3) as csSiteSpecificFactor8
     , substr(value, 1340, 3) as csSiteSpecificFactor9
     , substr(value, 1343, 3) as csSiteSpecificFactor10
     , substr(value, 1346, 3) as csSiteSpecificFactor11
     , substr(value, 1349, 3) as csSiteSpecificFactor12
     , substr(value, 1352, 3) as csSiteSpecificFactor13
     , substr(value, 1355, 3) as csSiteSpecificFactor14
     , substr(value, 1358, 3) as csSiteSpecificFactor15
     , substr(value, 1361, 3) as csSiteSpecificFactor16
     , substr(value, 1364, 3) as csSiteSpecificFactor17
     , substr(value, 1367, 3) as csSiteSpecificFactor18
     , substr(value, 1370, 3) as csSiteSpecificFactor19
     , substr(value, 1373, 3) as csSiteSpecificFactor20
     , substr(value, 1376, 3) as csSiteSpecificFactor21
     , substr(value, 1379, 3) as csSiteSpecificFactor22
     , substr(value, 1382, 3) as csSiteSpecificFactor23
     , substr(value, 1385, 3) as csSiteSpecificFactor24
     , substr(value, 1388, 3) as csSiteSpecificFactor25
     , substr(value, 1391, 2) as derivedAjcc6T
     , substr(value, 1393, 1) as derivedAjcc6TDescript
     , substr(value, 1394, 2) as derivedAjcc6N
     , substr(value, 1396, 1) as derivedAjcc6NDescript
     , substr(value, 1397, 2) as derivedAjcc6M
     , substr(value, 1399, 1) as derivedAjcc6MDescript
     , substr(value, 1400, 2) as derivedAjcc6StageGrp
     , substr(value, 1402, 3) as derivedAjcc7T
     , substr(value, 1405, 1) as derivedAjcc7TDescript
     , substr(value, 1406, 3) as derivedAjcc7N
     , substr(value, 1409, 1) as derivedAjcc7NDescript
     , substr(value, 1410, 3) as derivedAjcc7M
     , substr(value, 1413, 1) as derivedAjcc7MDescript
     , substr(value, 1414, 3) as derivedAjcc7StageGrp
     , substr(value, 1417, 3) as derivedPrerx7T
     , substr(value, 1420, 1) as derivedPrerx7TDescrip
     , substr(value, 1421, 3) as derivedPrerx7N
     , substr(value, 1424, 1) as derivedPrerx7NDescrip
     , substr(value, 1425, 3) as derivedPrerx7M
     , substr(value, 1428, 1) as derivedPrerx7MDescrip
     , substr(value, 1429, 3) as derivedPrerx7StageGrp
     , substr(value, 1432, 3) as derivedPostrx7T
     , substr(value, 1435, 3) as derivedPostrx7N
     , substr(value, 1438, 2) as derivedPostrx7M
     , substr(value, 1440, 3) as derivedPostrx7StgeGrp
     , substr(value, 1443, 1) as derivedSs1977
     , substr(value, 1444, 1) as derivedSs2000
     , substr(value, 1445, 1) as derivedNeoadjuvRxFlag
     , substr(value, 1446, 1) as derivedAjccFlag
     , substr(value, 1447, 1) as derivedSs1977Flag
     , substr(value, 1448, 1) as derivedSs2000Flag
     , substr(value, 1449, 4) as npcrDerivedClinStgGrp
     , substr(value, 1453, 4) as npcrDerivedPathStgGrp
     , substr(value, 1457, 15) as npcrDerivedAjcc8TnmClinStgGrp
     , substr(value, 1472, 15) as npcrDerivedAjcc8TnmPathStgGrp
     , substr(value, 1487, 15) as npcrDerivedAjcc8TnmPostStgGrp
     , substr(value, 1502, 6) as csVersionInputCurrent
     , substr(value, 1508, 6) as csVersionInputOriginal
     , substr(value, 1514, 6) as csVersionDerived
     , substr(value, 1520, 1) as seerSiteSpecificFact1
     , substr(value, 1521, 1) as seerSiteSpecificFact2
     , substr(value, 1522, 1) as seerSiteSpecificFact3
     , substr(value, 1523, 1) as seerSiteSpecificFact4
     , substr(value, 1524, 1) as seerSiteSpecificFact5
     , substr(value, 1525, 1) as seerSiteSpecificFact6
     , substr(value, 1526, 1) as icdRevisionComorbid
     , substr(value, 1527, 5) as comorbidComplication1
     , substr(value, 1532, 5) as comorbidComplication2
     , substr(value, 1537, 5) as comorbidComplication3
     , substr(value, 1542, 5) as comorbidComplication4
     , substr(value, 1547, 5) as comorbidComplication5
     , substr(value, 1552, 5) as comorbidComplication6
     , substr(value, 1557, 5) as comorbidComplication7
     , substr(value, 1562, 5) as comorbidComplication8
     , substr(value, 1567, 5) as comorbidComplication9
     , substr(value, 1572, 5) as comorbidComplication10
     , substr(value, 1577, 7) as secondaryDiagnosis1
     , substr(value, 1584, 7) as secondaryDiagnosis2
     , substr(value, 1591, 7) as secondaryDiagnosis3
     , substr(value, 1598, 7) as secondaryDiagnosis4
     , substr(value, 1605, 7) as secondaryDiagnosis5
     , substr(value, 1612, 7) as secondaryDiagnosis6
     , substr(value, 1619, 7) as secondaryDiagnosis7
     , substr(value, 1626, 7) as secondaryDiagnosis8
     , substr(value, 1633, 7) as secondaryDiagnosis9
     , substr(value, 1640, 7) as secondaryDiagnosis10
     , substr(value, 1722, 4) as ajccId
     , substr(value, 1726, 5) as schemaId
     , substr(value, 1731, 1) as schemaDiscriminator1
     , substr(value, 1732, 1) as schemaDiscriminator2
     , substr(value, 1733, 1) as schemaDiscriminator3
     , substr(value, 1734, 5) as percentNecrosisPostNeoadjuvant
     , substr(value, 1740, 1) as chromosome1pLossHeterozygosity
     , substr(value, 1741, 1) as chromosome19qLossHeterozygosity
     , substr(value, 1742, 1) as methylationOfO6MGMT
     , substr(value, 1743, 1) as estrogenReceptorSummary
     , substr(value, 1744, 1) as her2OverallSummary
     , substr(value, 1745, 2) as lnPositiveAxillaryLevel1To2
     , substr(value, 1747, 1) as multigeneSignatureMethod
     , substr(value, 1748, 2) as multigeneSignatureResults
     , substr(value, 1750, 1) as progesteroneRecepSummary
     , substr(value, 1751, 1) as responseToNeoadjuvantTherapy
     , substr(value, 1752, 3) as estrogenReceptorPercntPosOrRange
     , substr(value, 1755, 2) as estrogenReceptorTotalAllredScore
     , substr(value, 1757, 1) as her2IhcSummary
     , substr(value, 1758, 4) as her2IshDualProbeCopyNumber
     , substr(value, 1762, 4) as her2IshDualProbeRatio
     , substr(value, 1766, 4) as her2IshSingleProbeCopyNumber
     , substr(value, 1770, 1) as her2IshSummary
     , substr(value, 1771, 5) as ki67
     , substr(value, 1776, 3) as oncotypeDxRecurrenceScoreDcis
     , substr(value, 1779, 3) as oncotypeDxRecurrenceScoreInvasiv
     , substr(value, 1782, 1) as oncotypeDxRiskLevelDcis
     , substr(value, 1783, 1) as oncotypeDxRiskLevelInvasive
     , substr(value, 1784, 3) as progesteroneRecepPrcntPosOrRange
     , substr(value, 1787, 2) as progesteroneRecepTotalAllredScor
     , substr(value, 1789, 1) as ceaPretreatmentInterpretation
     , substr(value, 1790, 6) as ceaPretreatmentLabValue
     , substr(value, 1796, 4) as circumferentialResectionMargin
     , substr(value, 1800, 1) as kras
     , substr(value, 1801, 1) as microsatelliteInstability
     , substr(value, 1802, 1) as perineuralInvasion
     , substr(value, 1803, 2) as tumorDeposits
     , substr(value, 1805, 2) as numberOfPositiveParaAorticNodes
     , substr(value, 1807, 2) as numberOfExaminedParaAorticNodes
     , substr(value, 1809, 2) as numberOfPositivePelvicNodes
     , substr(value, 1811, 2) as numberOfExaminedPelvicNodes
     , substr(value, 1813, 1) as peritonealCytology
     , substr(value, 1814, 1) as esophagusAndEgjTumorEpicenter
     , substr(value, 1815, 1) as kitGeneImmunohistochemistry
     , substr(value, 1816, 2) as figoStage
     , substr(value, 1818, 1) as extranodalExtensionHeadNeckClin
     , substr(value, 1819, 3) as extranodalExtensionHeadNeckPath
     , substr(value, 1822, 1) as lnHeadAndNeckLevels1To3
     , substr(value, 1823, 1) as lnHeadAndNeckLevels4To5
     , substr(value, 1824, 1) as lnHeadAndNeckLevels6To7
     , substr(value, 1825, 1) as lnHeadAndNeckOther
     , substr(value, 1826, 4) as lnSize
     , substr(value, 1830, 1) as jak2
     , substr(value, 1831, 1) as primarySclerosingCholangitis
     , substr(value, 1832, 1) as tumorGrowthPattern
     , substr(value, 1833, 1) as ipsilateralAdrenalGlandInvolve
     , substr(value, 1834, 1) as invasionBeyondCapsule
     , substr(value, 1835, 1) as majorVeinInvolvement
     , substr(value, 1836, 3) as sarcomatoidFeatures
     , substr(value, 1839, 5) as adenoidCysticBasaloidPattern
     , substr(value, 1844, 1) as afpPretreatmentInterpretation
     , substr(value, 1845, 6) as afpPretreatmentLabValue
     , substr(value, 1851, 5) as bilirubinPretxTotalLabValue
     , substr(value, 1856, 1) as bilirubinPretxUnitOfMeasure
     , substr(value, 1857, 4) as creatininePretreatmentLabValue
     , substr(value, 1861, 1) as creatininePretxUnitOfMeasure
     , substr(value, 1862, 1) as fibrosisScore
     , substr(value, 1863, 3) as iNRProthrombinTime
     , substr(value, 1866, 1) as separateTumorNodules
     , substr(value, 1867, 1) as visceralParietalPleuralInvasion
     , substr(value, 1868, 1) as bSymptoms
     , substr(value, 1869, 1) as hivStatus
     , substr(value, 1870, 2) as nccnInternationalPrognosticIndex
     , substr(value, 1872, 2) as mitoticRateMelanoma
     , substr(value, 1874, 1) as chromosome3Status
     , substr(value, 1875, 1) as chromosome8qStatus
     , substr(value, 1876, 1) as extravascularMatrixPatterns
     , substr(value, 1877, 4) as measuredBasalDiameter
     , substr(value, 1881, 4) as measuredThickness
     , substr(value, 1885, 2) as microvascularDensity
     , substr(value, 1887, 4) as mitoticCountUvealMelanoma
     , substr(value, 1891, 4) as breslowTumorThickness
     , substr(value, 1895, 3) as ldhUpperLimitsOfNormal
     , substr(value, 1898, 7) as ldhPretreatmentLabValue
     , substr(value, 1905, 1) as ulceration
     , substr(value, 1906, 1) as lnIsolatedTumorCells
     , substr(value, 1907, 1) as profoundImmuneSuppression
     , substr(value, 1908, 1) as peripheralBloodInvolvement
     , substr(value, 1909, 1) as heritableTrait
     , substr(value, 1910, 1) as adenopathy
     , substr(value, 1911, 1) as anemia
     , substr(value, 1912, 1) as lymphocytosis
     , substr(value, 1913, 1) as organomegaly
     , substr(value, 1914, 1) as thrombocytopenia
     , substr(value, 1915, 1) as highRiskCytogenetics
     , substr(value, 1916, 1) as ldhPretreatmentLevel
     , substr(value, 1917, 1) as serumAlbuminPretreatmentLevel
     , substr(value, 1918, 1) as serumBeta2MicroglobulinPretxLvl
     , substr(value, 1919, 1) as ca125PretreatmentInterpretation
     , substr(value, 1920, 2) as residualTumVolPostCytoreduction
     , substr(value, 1922, 1) as extranodalExtensionClin
     , substr(value, 1923, 1) as extranodalExtensionPath
     , substr(value, 1924, 2) as gestationalTrophoblasticPxIndex
     , substr(value, 1926, 1) as pleuralEffusion
     , substr(value, 1927, 2) as gleasonPatternsClinical
     , substr(value, 1929, 2) as gleasonPatternsPathological
     , substr(value, 1931, 2) as gleasonScoreClinical
     , substr(value, 1933, 2) as gleasonScorePathological
     , substr(value, 1935, 2) as gleasonTertiaryPattern
     , substr(value, 1937, 2) as numberOfCoresExamined
     , substr(value, 1939, 2) as numberOfCoresPositive
     , substr(value, 1941, 3) as prostatePathologicalExtension
     , substr(value, 1944, 5) as psaLabValue
     , substr(value, 1949, 1) as highRiskHistologicFeatures
     , substr(value, 1950, 1) as boneInvasion
     , substr(value, 1951, 7) as afpPreOrchiectomyLabValue
     , substr(value, 1958, 1) as afpPreOrchiectomyRange
     , substr(value, 1959, 7) as afpPostOrchiectomyLabValue
     , substr(value, 1966, 1) as afpPostOrchiectomyRange
     , substr(value, 1967, 7) as hcgPreOrchiectomyLabValue
     , substr(value, 1974, 1) as hcgPreOrchiectomyRange
     , substr(value, 1975, 7) as hcgPostOrchiectomyLabValue
     , substr(value, 1982, 1) as hcgPostOrchiectomyRange
     , substr(value, 1983, 1) as ldhPreOrchiectomyRange
     , substr(value, 1984, 1) as ldhPostOrchiectomyRange
     , substr(value, 1985, 1) as sCategoryClinical
     , substr(value, 1986, 1) as sCategoryPathological
     , substr(value, 1987, 1) as lnAssessMethodParaaortic
     , substr(value, 1988, 1) as lnAssessMethodPelvic
     , substr(value, 1989, 1) as lnDistantAssessMethod
     , substr(value, 1990, 1) as lnDistantMediastinalScalene
     , substr(value, 1991, 1) as lnStatusFemorInguinParaaortPelv
     , substr(value, 1992, 1) as lnAssessMethodFemoralInguinal
     , substr(value, 1993, 1) as lnLaterality
     , substr(value, 1994, 2) as brainMolecularMarkers
     , substr(value, 2094, 8) as dateInitialRxSeer
     , substr(value, 2102, 2) as dateInitialRxSeerFlag
     , substr(value, 2104, 8) as date1stCrsRxCoc
     , substr(value, 2112, 2) as date1stCrsRxCocFlag
     , substr(value, 2114, 8) as rxDateSurgery
     , substr(value, 2122, 2) as rxDateSurgeryFlag
     , substr(value, 2124, 8) as rxDateMostDefinSurg
     , substr(value, 2132, 2) as rxDateMostDefinSurgFlag
     , substr(value, 2134, 8) as rxDateSurgicalDisch
     , substr(value, 2142, 2) as rxDateSurgicalDischFlag
     , substr(value, 2144, 8) as rxDateRadiation
     , substr(value, 2152, 2) as rxDateRadiationFlag
     , substr(value, 2154, 8) as rxDateRadiationEnded
     , substr(value, 2162, 2) as rxDateRadiationEndedFlag
     , substr(value, 2164, 8) as rxDateSystemic
     , substr(value, 2172, 2) as rxDateSystemicFlag
     , substr(value, 2174, 8) as rxDateChemo
     , substr(value, 2182, 2) as rxDateChemoFlag
     , substr(value, 2184, 8) as rxDateHormone
     , substr(value, 2192, 2) as rxDateHormoneFlag
     , substr(value, 2194, 8) as rxDateBrm
     , substr(value, 2202, 2) as rxDateBrmFlag
     , substr(value, 2204, 8) as rxDateOther
     , substr(value, 2212, 2) as rxDateOtherFlag
     , substr(value, 2214, 8) as rxDateDxStgProc
     , substr(value, 2222, 2) as rxDateDxStgProcFlag
     , substr(value, 2224, 1) as rxSummTreatmentStatus
     , substr(value, 2225, 2) as rxSummSurgPrimSite
     , substr(value, 2227, 1) as rxSummScopeRegLnSur
     , substr(value, 2228, 1) as rxSummSurgOthRegDis
     , substr(value, 2229, 2) as rxSummRegLnExamined
     , substr(value, 2231, 1) as rxSummSurgicalApproch
     , substr(value, 2232, 1) as rxSummSurgicalMargins
     , substr(value, 2233, 1) as rxSummReconstruct1st
     , substr(value, 2234, 1) as reasonForNoSurgery
     , substr(value, 2235, 2) as rxSummDxStgProc
     , substr(value, 2237, 1) as rxSummPalliativeProc
     , substr(value, 2238, 1) as rxSummRadiation
     , substr(value, 2239, 1) as rxSummRadToCns
     , substr(value, 2240, 1) as rxSummSurgRadSeq
     , substr(value, 2241, 2) as rxSummTransplntEndocr
     , substr(value, 2243, 2) as rxSummChemo
     , substr(value, 2245, 2) as rxSummHormone
     , substr(value, 2247, 2) as rxSummBrm
     , substr(value, 2249, 1) as rxSummOther
     , substr(value, 2250, 1) as reasonForNoRadiation
     , substr(value, 2251, 2) as rxCodingSystemCurrent
     , substr(value, 2253, 5) as radRegionalDoseCgy
     , substr(value, 2258, 3) as radNoOfTreatmentVol
     , substr(value, 2261, 2) as radTreatmentVolume
     , substr(value, 2263, 1) as radLocationOfRx
     , substr(value, 2264, 2) as radRegionalRxModality
     , substr(value, 2266, 2) as radBoostRxModality
     , substr(value, 2268, 5) as radBoostDoseCgy
     , substr(value, 2273, 1) as rxSummSystemicSurSeq
     , substr(value, 2274, 2) as rxSummSurgeryType
     , substr(value, 2276, 1) as readmSameHosp30Days
     , substr(value, 2277, 2) as rxSummSurgSite9802
     , substr(value, 2279, 1) as rxSummScopeReg9802
     , substr(value, 2280, 1) as rxSummSurgOth9802
     , substr(value, 2281, 2) as phase1RadiationPrimaryTxVolume
     , substr(value, 2283, 2) as phase1RadiationToDrainingLN
     , substr(value, 2285, 2) as phase1RadiationTreatmentModality
     , substr(value, 2287, 2) as phase1RadiationExternalBeamTech
     , substr(value, 2289, 5) as phase1DosePerFraction
     , substr(value, 2294, 3) as phase1NumberOfFractions
     , substr(value, 2297, 6) as phase1TotalDose
     , substr(value, 2303, 2) as phase2RadiationPrimaryTxVolume
     , substr(value, 2305, 2) as phase2RadiationToDrainingLN
     , substr(value, 2307, 2) as phase2RadiationTreatmentModality
     , substr(value, 2309, 2) as phase2RadiationExternalBeamTech
     , substr(value, 2311, 5) as phase2DosePerFraction
     , substr(value, 2316, 3) as phase2NumberOfFractions
     , substr(value, 2319, 6) as phase2TotalDose
     , substr(value, 2325, 2) as phase3RadiationPrimaryTxVolume
     , substr(value, 2327, 2) as phase3RadiationToDrainingLN
     , substr(value, 2329, 2) as phase3RadiationTreatmentModality
     , substr(value, 2331, 2) as phase3RadiationExternalBeamTech
     , substr(value, 2333, 5) as phase3DosePerFraction
     , substr(value, 2338, 3) as phase3NumberOfFractions
     , substr(value, 2341, 6) as phase3TotalDose
     , substr(value, 2347, 2) as numberPhasesOfRadTxToVolume
     , substr(value, 2349, 2) as radiationTxDiscontinuedEarly
     , substr(value, 2351, 6) as totalDose
     , substr(value, 2457, 8) as subsqRx2ndCourseDate
     , substr(value, 2465, 2) as subsqRx2ndcrsDateFlag
     , substr(value, 2467, 2) as subsqRx2ndCourseSurg
     , substr(value, 2469, 1) as subsqRx2ndScopeLnSu
     , substr(value, 2470, 1) as subsqRx2ndSurgOth
     , substr(value, 2471, 2) as subsqRx2ndRegLnRem
     , substr(value, 2473, 1) as subsqRx2ndCourseRad
     , substr(value, 2474, 1) as subsqRx2ndCourseChemo
     , substr(value, 2475, 1) as subsqRx2ndCourseHorm
     , substr(value, 2476, 1) as subsqRx2ndCourseBrm
     , substr(value, 2477, 1) as subsqRx2ndCourseOth
     , substr(value, 2478, 8) as subsqRx3rdCourseDate
     , substr(value, 2486, 2) as subsqRx3rdcrsDateFlag
     , substr(value, 2488, 2) as subsqRx3rdCourseSurg
     , substr(value, 2490, 1) as subsqRx3rdScopeLnSu
     , substr(value, 2491, 1) as subsqRx3rdSurgOth
     , substr(value, 2492, 2) as subsqRx3rdRegLnRem
     , substr(value, 2494, 1) as subsqRx3rdCourseRad
     , substr(value, 2495, 1) as subsqRx3rdCourseChemo
     , substr(value, 2496, 1) as subsqRx3rdCourseHorm
     , substr(value, 2497, 1) as subsqRx3rdCourseBrm
     , substr(value, 2498, 1) as subsqRx3rdCourseOth
     , substr(value, 2499, 8) as subsqRx4thCourseDate
     , substr(value, 2507, 2) as subsqRx4thcrsDateFlag
     , substr(value, 2509, 2) as subsqRx4thCourseSurg
     , substr(value, 2511, 1) as subsqRx4thScopeLnSu
     , substr(value, 2512, 1) as subsqRx4thSurgOth
     , substr(value, 2513, 2) as subsqRx4thRegLnRem
     , substr(value, 2515, 1) as subsqRx4thCourseRad
     , substr(value, 2516, 1) as subsqRx4thCourseChemo
     , substr(value, 2517, 1) as subsqRx4thCourseHorm
     , substr(value, 2518, 1) as subsqRx4thCourseBrm
     , substr(value, 2519, 1) as subsqRx4thCourseOth
     , substr(value, 2520, 1) as subsqRxReconstructDel
     , substr(value, 2571, 1) as overRideSsNodespos
     , substr(value, 2572, 1) as overRideSsTnmN
     , substr(value, 2573, 1) as overRideSsTnmM
     , substr(value, 2574, 1) as overRideAcsnClassSeq
     , substr(value, 2575, 1) as overRideHospseqDxconf
     , substr(value, 2576, 1) as overRideCocSiteType
     , substr(value, 2577, 1) as overRideHospseqSite
     , substr(value, 2578, 1) as overRideSiteTnmStggrp
     , substr(value, 2579, 1) as overRideAgeSiteMorph
     , substr(value, 2580, 1) as overRideTnmStage
     , substr(value, 2581, 1) as overRideTnmTis
     , substr(value, 2582, 1) as overRideTnm3
     , substr(value, 2583, 1) as overRideSeqnoDxconf
     , substr(value, 2584, 1) as overRideSiteLatSeqno
     , substr(value, 2585, 1) as overRideSurgDxconf
     , substr(value, 2586, 1) as overRideSiteType
     , substr(value, 2587, 1) as overRideHistology
     , substr(value, 2588, 1) as overRideReportSource
     , substr(value, 2589, 1) as overRideIllDefineSite
     , substr(value, 2590, 1) as overRideLeukLymphoma
     , substr(value, 2591, 1) as overRideSiteBehavior
     , substr(value, 2592, 1) as overRideSiteEodDxDt
     , substr(value, 2593, 1) as overRideSiteLatEod
     , substr(value, 2594, 1) as overRideSiteLatMorph
     , substr(value, 2595, 1) as overRideNameSex
     , substr(value, 2596, 4) as siteIcdO1
     , substr(value, 2600, 4) as histologyIcdO1
     , substr(value, 2604, 1) as behaviorIcdO1
     , substr(value, 2605, 1) as gradeIcdO1
     , substr(value, 2606, 1) as icdO2ConversionFlag
     , substr(value, 2607, 10) as crcChecksum
     , substr(value, 2617, 1) as seerCodingSysCurrent
     , substr(value, 2618, 1) as seerCodingSysOriginal
     , substr(value, 2619, 2) as cocCodingSysCurrent
     , substr(value, 2621, 2) as cocCodingSysOriginal
     , substr(value, 2623, 1) as rqrsNcdbSubmissionFlag
     , substr(value, 2624, 1) as cocAccreditedFlag
     , substr(value, 2625, 10) as vendorName
     , substr(value, 2635, 1) as seerTypeOfFollowUp
     , substr(value, 2636, 2) as seerRecordNumber
     , substr(value, 2638, 2) as diagnosticProc7387
     , substr(value, 2640, 8) as dateCaseInitiated
     , substr(value, 2648, 8) as dateCaseCompleted
     , substr(value, 2656, 8) as dateCaseCompletedCoc
     , substr(value, 2664, 8) as dateCaseLastChanged
     , substr(value, 2672, 8) as dateCaseReportExported
     , substr(value, 2680, 8) as dateCaseReportReceived
     , substr(value, 2688, 8) as dateCaseReportLoaded
     , substr(value, 2696, 8) as dateTumorRecordAvailbl
     , substr(value, 2704, 1) as icdO3ConversionFlag
     , substr(value, 2705, 1) as overRideCs1
     , substr(value, 2706, 1) as overRideCs2
     , substr(value, 2707, 1) as overRideCs3
     , substr(value, 2708, 1) as overRideCs4
     , substr(value, 2709, 1) as overRideCs5
     , substr(value, 2710, 1) as overRideCs6
     , substr(value, 2711, 1) as overRideCs7
     , substr(value, 2712, 1) as overRideCs8
     , substr(value, 2713, 1) as overRideCs9
     , substr(value, 2714, 1) as overRideCs10
     , substr(value, 2715, 1) as overRideCs11
     , substr(value, 2716, 1) as overRideCs12
     , substr(value, 2717, 1) as overRideCs13
     , substr(value, 2718, 1) as overRideCs14
     , substr(value, 2719, 1) as overRideCs15
     , substr(value, 2720, 1) as overRideCs16
     , substr(value, 2721, 1) as overRideCs17
     , substr(value, 2722, 1) as overRideCs18
     , substr(value, 2723, 1) as overRideCs19
     , substr(value, 2724, 1) as overRideCs20
     , substr(value, 2775, 8) as dateOfLastContact
     , substr(value, 2783, 2) as dateOfLastContactFlag
     , substr(value, 2785, 1) as vitalStatus
     , substr(value, 2786, 1) as vitalStatusRecode
     , substr(value, 2787, 1) as cancerStatus
     , substr(value, 2788, 8) as dateOfLastCancerStatus
     , substr(value, 2796, 2) as dateOfLastCancerStatusFlag
     , substr(value, 2798, 2) as recordNumberRecode
     , substr(value, 2800, 1) as qualityOfSurvival
     , substr(value, 2801, 1) as followUpSource
     , substr(value, 2802, 1) as nextFollowUpSource
     , substr(value, 2803, 50) as addrCurrentCity
     , substr(value, 2853, 2) as addrCurrentState
     , substr(value, 2855, 9) as addrCurrentPostalCode
     , substr(value, 2864, 3) as countyCurrent
     , substr(value, 2867, 8) as recurrenceDate1st
     , substr(value, 2875, 2) as recurrenceDate1stFlag
     , substr(value, 2877, 2) as recurrenceType1st
     , substr(value, 2879, 50) as followUpContactCity
     , substr(value, 2929, 2) as followUpContactState
     , substr(value, 2931, 9) as followUpContactPostal
     , substr(value, 2940, 4) as causeOfDeath
     , substr(value, 2944, 1) as seerCauseSpecificCod
     , substr(value, 2945, 1) as seerOtherCod
     , substr(value, 2946, 1) as icdRevisionNumber
     , substr(value, 2947, 1) as autopsy
     , substr(value, 2948, 3) as placeOfDeath
     , substr(value, 2951, 2) as followUpSourceCentral
     , substr(value, 2953, 8) as dateOfDeathCanada
     , substr(value, 2961, 2) as dateOfDeathCanadaFlag
     , substr(value, 2963, 2) as unusualFollowUpMethod
     , substr(value, 2965, 8) as survDateActiveFollowup
     , substr(value, 2973, 1) as survFlagActiveFollowup
     , substr(value, 2974, 4) as survMosActiveFollowup
     , substr(value, 2978, 8) as survDatePresumedAlive
     , substr(value, 2986, 1) as survFlagPresumedAlive
     , substr(value, 2987, 4) as survMosPresumedAlive
     , substr(value, 2991, 8) as survDateDxRecode
     , substr(value, 4049, 40) as nameLast
     , substr(value, 4089, 40) as nameFirst
     , substr(value, 4129, 40) as nameMiddle
     , substr(value, 4169, 3) as namePrefix
     , substr(value, 4172, 3) as nameSuffix
     , substr(value, 4175, 40) as nameAlias
     , substr(value, 4215, 40) as nameMaiden
     , substr(value, 4255, 60) as nameSpouseParent
     , substr(value, 4315, 11) as medicalRecordNumber
     , substr(value, 4326, 2) as militaryRecordNoSuffix
     , substr(value, 4328, 9) as socialSecurityNumber
     , substr(value, 4337, 11) as medicareBeneficiaryIdentifier
     , substr(value, 4348, 60) as addrAtDxNoStreet
     , substr(value, 4408, 60) as addrAtDxSupplementl
     , substr(value, 4468, 60) as addrCurrentNoStreet
     , substr(value, 4528, 60) as addrCurrentSupplementl
     , substr(value, 4588, 10) as telephone
     , substr(value, 4598, 6) as dcStateFileNumber
     , substr(value, 4604, 60) as followUpContactName
     , substr(value, 4664, 60) as followUpContactNost
     , substr(value, 4724, 60) as followUpContactSuppl
     , substr(value, 4784, 10) as latitude
     , substr(value, 4794, 11) as longitude
     , substr(value, 4905, 10) as npiFollowingRegistry
     , substr(value, 4915, 10) as followingRegistry
     , substr(value, 4925, 10) as npiInstReferredFrom
     , substr(value, 4935, 10) as institutionReferredFrom
     , substr(value, 4945, 10) as npiInstReferredTo
     , substr(value, 4955, 10) as institutionReferredTo
     , substr(value, 5015, 10) as npiPhysicianManaging
     , substr(value, 5025, 8) as physicianManaging
     , substr(value, 5033, 10) as npiPhysicianFollowUp
     , substr(value, 5043, 8) as physicianFollowUp
     , substr(value, 5051, 10) as npiPhysicianPrimarySurg
     , substr(value, 5061, 8) as physicianPrimarySurg
     , substr(value, 5069, 10) as npiPhysician3
     , substr(value, 5079, 8) as physician3
     , substr(value, 5087, 10) as npiPhysician4
     , substr(value, 5097, 8) as physician4
     , substr(value, 5105, 1000) as ehrReporting
     , substr(value, 6155, 25) as pathReportingFacId1
     , substr(value, 6180, 20) as pathReportNumber1
     , substr(value, 6200, 14) as pathDateSpecCollect1
     , substr(value, 6214, 2) as pathReportType1
     , substr(value, 6216, 25) as pathOrderingFacNo1
     , substr(value, 6241, 20) as pathOrderPhysLicNo1
     , substr(value, 6261, 25) as pathReportingFacId2
     , substr(value, 6286, 20) as pathReportNumber2
     , substr(value, 6306, 14) as pathDateSpecCollect2
     , substr(value, 6320, 2) as pathReportType2
     , substr(value, 6322, 25) as pathOrderingFacNo2
     , substr(value, 6347, 20) as pathOrderPhysLicNo2
     , substr(value, 6367, 25) as pathReportingFacId3
     , substr(value, 6392, 20) as pathReportNumber3
     , substr(value, 6412, 14) as pathDateSpecCollect3
     , substr(value, 6426, 2) as pathReportType3
     , substr(value, 6428, 25) as pathOrderingFacNo3
     , substr(value, 6453, 20) as pathOrderPhysLicNo3
     , substr(value, 6473, 25) as pathReportingFacId4
     , substr(value, 6498, 20) as pathReportNumber4
     , substr(value, 6518, 14) as pathDateSpecCollect4
     , substr(value, 6532, 2) as pathReportType4
     , substr(value, 6534, 25) as pathOrderingFacNo4
     , substr(value, 6559, 20) as pathOrderPhysLicNo4
     , substr(value, 6579, 25) as pathReportingFacId5
     , substr(value, 6604, 20) as pathReportNumber5
     , substr(value, 6624, 14) as pathDateSpecCollect5
     , substr(value, 6638, 2) as pathReportType5
     , substr(value, 6640, 25) as pathOrderingFacNo5
     , substr(value, 6665, 20) as pathOrderPhysLicNo5
     , substr(value, 6935, 1000) as textDxProcPe
     , substr(value, 7935, 1000) as textDxProcXRayScan
     , substr(value, 8935, 1000) as textDxProcScopes
     , substr(value, 9935, 1000) as textDxProcLabTests
     , substr(value, 10935, 1000) as textDxProcOp
     , substr(value, 11935, 1000) as textDxProcPath
     , substr(value, 12935, 100) as textPrimarySiteTitle
     , substr(value, 13035, 100) as textHistologyTitle
     , substr(value, 13135, 1000) as textStaging
     , substr(value, 14135, 1000) as rxTextSurgery
     , substr(value, 15135, 1000) as rxTextRadiation
     , substr(value, 16135, 1000) as rxTextRadiationOther
     , substr(value, 17135, 1000) as rxTextChemo
     , substr(value, 18135, 1000) as rxTextHormone
     , substr(value, 19135, 1000) as rxTextBrm
     , substr(value, 20135, 1000) as rxTextOther
     , substr(value, 21135, 1000) as textRemarks
     , substr(value, 22135, 60) as textPlaceOfDiagnosis
        from naaccr_lines;

drop view if exists naaccr_fields;
create view naaccr_fields as
select row_number() over (order by dateOfDiagnosis, dateOfBirth, dateOfLastContact, dateCaseLastChanged, dateCaseCompleted, dateCaseReportExported) tumor_id
             , patientSystemIdHosp, patientIdNumber
             , case when trim(sequenceNumberCentral) > '' then trim(sequenceNumberCentral) end as sequenceNumberCentral
  , case when trim(dateOfDiagnosis) > '' then date(substr(dateOfDiagnosis, 1, 4) || '-' || coalesce(substr(dateOfDiagnosis, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfDiagnosis, 7, 2), '01')) end as dateOfDiagnosis 
  , case when trim(dateOfDiagnosisFlag) > '' then trim(dateOfDiagnosisFlag) end as dateOfDiagnosisFlag
  , case when trim(primarySite) > '' then trim(primarySite) end as primarySite
  , case when trim(laterality) > '' then trim(laterality) end as laterality
  , case when trim(histologyIcdO2) > '' then trim(histologyIcdO2) end as histologyIcdO2
  , case when trim(behaviorIcdO2) > '' then trim(behaviorIcdO2) end as behaviorIcdO2
  , case when trim(dateOfMultTumorsFlag) > '' then trim(dateOfMultTumorsFlag) end as dateOfMultTumorsFlag
  , case when trim(grade) > '' then trim(grade) end as grade
  , case when trim(gradePathValue) > '' then trim(gradePathValue) end as gradePathValue
  , case when trim(ambiguousTerminologyDx) > '' then trim(ambiguousTerminologyDx) end as ambiguousTerminologyDx
  , case when trim(dateConclusiveDx) > '' then date(substr(dateConclusiveDx, 1, 4) || '-' || coalesce(substr(dateConclusiveDx, 5, 2), '01') || '-'
                     || coalesce(substr(dateConclusiveDx, 7, 2), '01')) end as dateConclusiveDx 
  , case when trim(multTumRptAsOnePrim) > '' then trim(multTumRptAsOnePrim) end as multTumRptAsOnePrim
  , case when trim(dateOfMultTumors) > '' then date(substr(dateOfMultTumors, 1, 4) || '-' || coalesce(substr(dateOfMultTumors, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfMultTumors, 7, 2), '01')) end as dateOfMultTumors 
  , case when trim(multiplicityCounter) > '' then 0 + multiplicityCounter end as multiplicityCounter
  , case when trim(dateConclusiveDxFlag) > '' then trim(dateConclusiveDxFlag) end as dateConclusiveDxFlag
  , case when trim(gradePathSystem) > '' then trim(gradePathSystem) end as gradePathSystem
  , case when trim(siteCodingSysCurrent) > '' then trim(siteCodingSysCurrent) end as siteCodingSysCurrent
  , case when trim(siteCodingSysOriginal) > '' then trim(siteCodingSysOriginal) end as siteCodingSysOriginal
  , case when trim(morphCodingSysCurrent) > '' then trim(morphCodingSysCurrent) end as morphCodingSysCurrent
  , case when trim(morphCodingSysOriginl) > '' then trim(morphCodingSysOriginl) end as morphCodingSysOriginl
  , case when trim(diagnosticConfirmation) > '' then trim(diagnosticConfirmation) end as diagnosticConfirmation
  , case when trim(typeOfReportingSource) > '' then trim(typeOfReportingSource) end as typeOfReportingSource
  , case when trim(casefindingSource) > '' then trim(casefindingSource) end as casefindingSource
  , case when trim(histologicTypeIcdO3) > '' then trim(histologicTypeIcdO3) end as histologicTypeIcdO3
  , case when trim(behaviorCodeIcdO3) > '' then trim(behaviorCodeIcdO3) end as behaviorCodeIcdO3
  , case when trim(addrAtDxState) > '' then trim(addrAtDxState) end as addrAtDxState
  , case when trim(stateAtDxGeocode19708090) > '' then trim(stateAtDxGeocode19708090) end as stateAtDxGeocode19708090
  , case when trim(stateAtDxGeocode2000) > '' then trim(stateAtDxGeocode2000) end as stateAtDxGeocode2000
  , case when trim(stateAtDxGeocode2010) > '' then trim(stateAtDxGeocode2010) end as stateAtDxGeocode2010
  , case when trim(stateAtDxGeocode2020) > '' then trim(stateAtDxGeocode2020) end as stateAtDxGeocode2020
  , case when trim(addrAtDxCountry) > '' then trim(addrAtDxCountry) end as addrAtDxCountry
  , case when trim(censusCodSys19708090) > '' then trim(censusCodSys19708090) end as censusCodSys19708090
  , case when trim(censusTrPovertyIndictr) > '' then trim(censusTrPovertyIndictr) end as censusTrPovertyIndictr
  , case when trim(maritalStatusAtDx) > '' then trim(maritalStatusAtDx) end as maritalStatusAtDx
  , case when trim(race1) > '' then trim(race1) end as race1
  , case when trim(race2) > '' then trim(race2) end as race2
  , case when trim(race3) > '' then trim(race3) end as race3
  , case when trim(race4) > '' then trim(race4) end as race4
  , case when trim(race5) > '' then trim(race5) end as race5
  , case when trim(raceCodingSysCurrent) > '' then trim(raceCodingSysCurrent) end as raceCodingSysCurrent
  , case when trim(raceCodingSysOriginal) > '' then trim(raceCodingSysOriginal) end as raceCodingSysOriginal
  , case when trim(spanishHispanicOrigin) > '' then trim(spanishHispanicOrigin) end as spanishHispanicOrigin
  , case when trim(nhiaDerivedHispOrigin) > '' then trim(nhiaDerivedHispOrigin) end as nhiaDerivedHispOrigin
  , case when trim(ihsLink) > '' then trim(ihsLink) end as ihsLink
  , case when trim(raceNapiia) > '' then trim(raceNapiia) end as raceNapiia
  , case when trim(computedEthnicity) > '' then trim(computedEthnicity) end as computedEthnicity
  , case when trim(computedEthnicitySource) > '' then trim(computedEthnicitySource) end as computedEthnicitySource
  , case when trim(sex) > '' then trim(sex) end as sex
  , case when trim(dateOfBirth) > '' then date(substr(dateOfBirth, 1, 4) || '-' || coalesce(substr(dateOfBirth, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfBirth, 7, 2), '01')) end as dateOfBirth 
  , case when trim(dateOfBirthFlag) > '' then trim(dateOfBirthFlag) end as dateOfBirthFlag
  , case when trim(birthplace) > '' then trim(birthplace) end as birthplace
  , case when trim(birthplaceState) > '' then trim(birthplaceState) end as birthplaceState
  , case when trim(birthplaceCountry) > '' then trim(birthplaceCountry) end as birthplaceCountry
  , case when trim(censusOccCode19702000) > '' then trim(censusOccCode19702000) end as censusOccCode19702000
  , case when trim(censusIndCode2010) > '' then trim(censusIndCode2010) end as censusIndCode2010
  , case when trim(censusIndCode19702000) > '' then trim(censusIndCode19702000) end as censusIndCode19702000
  , case when trim(censusOccCode2010) > '' then trim(censusOccCode2010) end as censusOccCode2010
  , case when trim(occupationSource) > '' then trim(occupationSource) end as occupationSource
  , case when trim(industrySource) > '' then trim(industrySource) end as industrySource
  , case when trim(censusOccIndSys7000) > '' then trim(censusOccIndSys7000) end as censusOccIndSys7000
  , case when trim(ruca2000) > '' then trim(ruca2000) end as ruca2000
  , case when trim(ruca2010) > '' then trim(ruca2010) end as ruca2010
  , case when trim(uric2000) > '' then trim(uric2000) end as uric2000
  , case when trim(uric2010) > '' then trim(uric2010) end as uric2010
  , case when trim(censusTrCert19708090) > '' then trim(censusTrCert19708090) end as censusTrCert19708090
  , case when trim(censusTrCertainty2000) > '' then trim(censusTrCertainty2000) end as censusTrCertainty2000
  , case when trim(gisCoordinateQuality) > '' then trim(gisCoordinateQuality) end as gisCoordinateQuality
  , case when trim(censusTrCertainty2010) > '' then trim(censusTrCertainty2010) end as censusTrCertainty2010
  , case when trim(censusTractCertainty2020) > '' then trim(censusTractCertainty2020) end as censusTractCertainty2020
  , case when trim(addrCurrentCountry) > '' then trim(addrCurrentCountry) end as addrCurrentCountry
  , case when trim(followupContactCountry) > '' then trim(followupContactCountry) end as followupContactCountry
  , case when trim(placeOfDeathState) > '' then trim(placeOfDeathState) end as placeOfDeathState
  , case when trim(placeOfDeathCountry) > '' then trim(placeOfDeathCountry) end as placeOfDeathCountry
  , case when trim(ruralurbanContinuum1993) > '' then trim(ruralurbanContinuum1993) end as ruralurbanContinuum1993
  , case when trim(ruralurbanContinuum2003) > '' then trim(ruralurbanContinuum2003) end as ruralurbanContinuum2003
  , case when trim(ruralurbanContinuum2013) > '' then trim(ruralurbanContinuum2013) end as ruralurbanContinuum2013
  , case when trim(siteIcdO1) > '' then trim(siteIcdO1) end as siteIcdO1
  , case when trim(histologyIcdO1) > '' then trim(histologyIcdO1) end as histologyIcdO1
  , case when trim(behaviorIcdO1) > '' then trim(behaviorIcdO1) end as behaviorIcdO1
  , case when trim(gradeIcdO1) > '' then trim(gradeIcdO1) end as gradeIcdO1
  , case when trim(icdO2ConversionFlag) > '' then trim(icdO2ConversionFlag) end as icdO2ConversionFlag
  , case when trim(overRideSsNodespos) > '' then trim(overRideSsNodespos) end as overRideSsNodespos
  , case when trim(overRideSsTnmN) > '' then trim(overRideSsTnmN) end as overRideSsTnmN
  , case when trim(overRideSsTnmM) > '' then trim(overRideSsTnmM) end as overRideSsTnmM
  , case when trim(overRideAcsnClassSeq) > '' then trim(overRideAcsnClassSeq) end as overRideAcsnClassSeq
  , case when trim(overRideHospseqDxconf) > '' then trim(overRideHospseqDxconf) end as overRideHospseqDxconf
  , case when trim(overRideCocSiteType) > '' then trim(overRideCocSiteType) end as overRideCocSiteType
  , case when trim(overRideHospseqSite) > '' then trim(overRideHospseqSite) end as overRideHospseqSite
  , case when trim(overRideSiteTnmStggrp) > '' then trim(overRideSiteTnmStggrp) end as overRideSiteTnmStggrp
  , case when trim(overRideAgeSiteMorph) > '' then trim(overRideAgeSiteMorph) end as overRideAgeSiteMorph
  , case when trim(overRideTnmStage) > '' then trim(overRideTnmStage) end as overRideTnmStage
  , case when trim(overRideTnmTis) > '' then trim(overRideTnmTis) end as overRideTnmTis
  , case when trim(overRideTnm3) > '' then trim(overRideTnm3) end as overRideTnm3
  , case when trim(overRideSeqnoDxconf) > '' then trim(overRideSeqnoDxconf) end as overRideSeqnoDxconf
  , case when trim(overRideSiteLatSeqno) > '' then trim(overRideSiteLatSeqno) end as overRideSiteLatSeqno
  , case when trim(overRideSurgDxconf) > '' then trim(overRideSurgDxconf) end as overRideSurgDxconf
  , case when trim(overRideSiteType) > '' then trim(overRideSiteType) end as overRideSiteType
  , case when trim(overRideHistology) > '' then trim(overRideHistology) end as overRideHistology
  , case when trim(overRideReportSource) > '' then trim(overRideReportSource) end as overRideReportSource
  , case when trim(overRideIllDefineSite) > '' then trim(overRideIllDefineSite) end as overRideIllDefineSite
  , case when trim(overRideLeukLymphoma) > '' then trim(overRideLeukLymphoma) end as overRideLeukLymphoma
  , case when trim(overRideSiteBehavior) > '' then trim(overRideSiteBehavior) end as overRideSiteBehavior
  , case when trim(overRideSiteEodDxDt) > '' then trim(overRideSiteEodDxDt) end as overRideSiteEodDxDt
  , case when trim(overRideSiteLatEod) > '' then trim(overRideSiteLatEod) end as overRideSiteLatEod
  , case when trim(overRideSiteLatMorph) > '' then trim(overRideSiteLatMorph) end as overRideSiteLatMorph
  , case when trim(overRideNameSex) > '' then trim(overRideNameSex) end as overRideNameSex
  , case when trim(dateCaseInitiated) > '' then date(substr(dateCaseInitiated, 1, 4) || '-' || coalesce(substr(dateCaseInitiated, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseInitiated, 7, 2), '01')) end as dateCaseInitiated 
  , case when trim(dateCaseCompleted) > '' then date(substr(dateCaseCompleted, 1, 4) || '-' || coalesce(substr(dateCaseCompleted, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseCompleted, 7, 2), '01')) end as dateCaseCompleted 
  , case when trim(dateCaseCompletedCoc) > '' then date(substr(dateCaseCompletedCoc, 1, 4) || '-' || coalesce(substr(dateCaseCompletedCoc, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseCompletedCoc, 7, 2), '01')) end as dateCaseCompletedCoc 
  , case when trim(dateCaseLastChanged) > '' then date(substr(dateCaseLastChanged, 1, 4) || '-' || coalesce(substr(dateCaseLastChanged, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseLastChanged, 7, 2), '01')) end as dateCaseLastChanged 
  , case when trim(dateCaseReportExported) > '' then date(substr(dateCaseReportExported, 1, 4) || '-' || coalesce(substr(dateCaseReportExported, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseReportExported, 7, 2), '01')) end as dateCaseReportExported 
  , case when trim(dateCaseReportReceived) > '' then date(substr(dateCaseReportReceived, 1, 4) || '-' || coalesce(substr(dateCaseReportReceived, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseReportReceived, 7, 2), '01')) end as dateCaseReportReceived 
  , case when trim(dateCaseReportLoaded) > '' then date(substr(dateCaseReportLoaded, 1, 4) || '-' || coalesce(substr(dateCaseReportLoaded, 5, 2), '01') || '-'
                     || coalesce(substr(dateCaseReportLoaded, 7, 2), '01')) end as dateCaseReportLoaded 
  , case when trim(dateTumorRecordAvailbl) > '' then date(substr(dateTumorRecordAvailbl, 1, 4) || '-' || coalesce(substr(dateTumorRecordAvailbl, 5, 2), '01') || '-'
                     || coalesce(substr(dateTumorRecordAvailbl, 7, 2), '01')) end as dateTumorRecordAvailbl 
  , case when trim(icdO3ConversionFlag) > '' then trim(icdO3ConversionFlag) end as icdO3ConversionFlag
  , case when trim(seerCodingSysCurrent) > '' then trim(seerCodingSysCurrent) end as seerCodingSysCurrent
  , case when trim(seerCodingSysOriginal) > '' then trim(seerCodingSysOriginal) end as seerCodingSysOriginal
  , case when trim(cocCodingSysCurrent) > '' then trim(cocCodingSysCurrent) end as cocCodingSysCurrent
  , case when trim(cocCodingSysOriginal) > '' then trim(cocCodingSysOriginal) end as cocCodingSysOriginal
  , case when trim(cocAccreditedFlag) > '' then trim(cocAccreditedFlag) end as cocAccreditedFlag
  , case when trim(rqrsNcdbSubmissionFlag) > '' then trim(rqrsNcdbSubmissionFlag) end as rqrsNcdbSubmissionFlag
  , case when trim(vendorName) > '' then trim(vendorName) end as vendorName
  , case when trim(seerTypeOfFollowUp) > '' then trim(seerTypeOfFollowUp) end as seerTypeOfFollowUp
  , case when trim(seerRecordNumber) > '' then trim(seerRecordNumber) end as seerRecordNumber
  , case when trim(overRideCs1) > '' then trim(overRideCs1) end as overRideCs1
  , case when trim(overRideCs2) > '' then trim(overRideCs2) end as overRideCs2
  , case when trim(overRideCs3) > '' then trim(overRideCs3) end as overRideCs3
  , case when trim(overRideCs4) > '' then trim(overRideCs4) end as overRideCs4
  , case when trim(overRideCs5) > '' then trim(overRideCs5) end as overRideCs5
  , case when trim(overRideCs6) > '' then trim(overRideCs6) end as overRideCs6
  , case when trim(overRideCs7) > '' then trim(overRideCs7) end as overRideCs7
  , case when trim(overRideCs8) > '' then trim(overRideCs8) end as overRideCs8
  , case when trim(overRideCs9) > '' then trim(overRideCs9) end as overRideCs9
  , case when trim(overRideCs10) > '' then trim(overRideCs10) end as overRideCs10
  , case when trim(overRideCs11) > '' then trim(overRideCs11) end as overRideCs11
  , case when trim(overRideCs12) > '' then trim(overRideCs12) end as overRideCs12
  , case when trim(overRideCs13) > '' then trim(overRideCs13) end as overRideCs13
  , case when trim(overRideCs14) > '' then trim(overRideCs14) end as overRideCs14
  , case when trim(overRideCs15) > '' then trim(overRideCs15) end as overRideCs15
  , case when trim(overRideCs16) > '' then trim(overRideCs16) end as overRideCs16
  , case when trim(overRideCs17) > '' then trim(overRideCs17) end as overRideCs17
  , case when trim(overRideCs18) > '' then trim(overRideCs18) end as overRideCs18
  , case when trim(overRideCs19) > '' then trim(overRideCs19) end as overRideCs19
  , case when trim(overRideCs20) > '' then trim(overRideCs20) end as overRideCs20
  , case when trim(dateOfLastContact) > '' then date(substr(dateOfLastContact, 1, 4) || '-' || coalesce(substr(dateOfLastContact, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfLastContact, 7, 2), '01')) end as dateOfLastContact 
  , case when trim(dateOfLastContactFlag) > '' then trim(dateOfLastContactFlag) end as dateOfLastContactFlag
  , case when trim(dateOfDeathCanada) > '' then date(substr(dateOfDeathCanada, 1, 4) || '-' || coalesce(substr(dateOfDeathCanada, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfDeathCanada, 7, 2), '01')) end as dateOfDeathCanada 
  , case when trim(dateOfDeathCanadaFlag) > '' then trim(dateOfDeathCanadaFlag) end as dateOfDeathCanadaFlag
  , case when trim(vitalStatus) > '' then trim(vitalStatus) end as vitalStatus
  , case when trim(vitalStatusRecode) > '' then trim(vitalStatusRecode) end as vitalStatusRecode
  , case when trim(cancerStatus) > '' then trim(cancerStatus) end as cancerStatus
  , case when trim(dateOfLastCancerStatus) > '' then date(substr(dateOfLastCancerStatus, 1, 4) || '-' || coalesce(substr(dateOfLastCancerStatus, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfLastCancerStatus, 7, 2), '01')) end as dateOfLastCancerStatus 
  , case when trim(dateOfLastCancerStatusFlag) > '' then trim(dateOfLastCancerStatusFlag) end as dateOfLastCancerStatusFlag
  , case when trim(recordNumberRecode) > '' then 0 + recordNumberRecode end as recordNumberRecode
  , case when trim(qualityOfSurvival) > '' then trim(qualityOfSurvival) end as qualityOfSurvival
  , case when trim(survDateActiveFollowup) > '' then date(substr(survDateActiveFollowup, 1, 4) || '-' || coalesce(substr(survDateActiveFollowup, 5, 2), '01') || '-'
                     || coalesce(substr(survDateActiveFollowup, 7, 2), '01')) end as survDateActiveFollowup 
  , case when trim(survFlagActiveFollowup) > '' then trim(survFlagActiveFollowup) end as survFlagActiveFollowup
  , case when trim(survMosActiveFollowup) > '' then 0 + survMosActiveFollowup end as survMosActiveFollowup
  , case when trim(survDatePresumedAlive) > '' then date(substr(survDatePresumedAlive, 1, 4) || '-' || coalesce(substr(survDatePresumedAlive, 5, 2), '01') || '-'
                     || coalesce(substr(survDatePresumedAlive, 7, 2), '01')) end as survDatePresumedAlive 
  , case when trim(survFlagPresumedAlive) > '' then trim(survFlagPresumedAlive) end as survFlagPresumedAlive
  , case when trim(survMosPresumedAlive) > '' then 0 + survMosPresumedAlive end as survMosPresumedAlive
  , case when trim(survDateDxRecode) > '' then date(substr(survDateDxRecode, 1, 4) || '-' || coalesce(substr(survDateDxRecode, 5, 2), '01') || '-'
                     || coalesce(substr(survDateDxRecode, 7, 2), '01')) end as survDateDxRecode 
  , case when trim(followUpSource) > '' then trim(followUpSource) end as followUpSource
  , case when trim(followUpSourceCentral) > '' then trim(followUpSourceCentral) end as followUpSourceCentral
  , case when trim(nextFollowUpSource) > '' then trim(nextFollowUpSource) end as nextFollowUpSource
  , case when trim(addrCurrentState) > '' then trim(addrCurrentState) end as addrCurrentState
  , case when trim(followUpContactState) > '' then trim(followUpContactState) end as followUpContactState
  , case when trim(recurrenceDate1st) > '' then date(substr(recurrenceDate1st, 1, 4) || '-' || coalesce(substr(recurrenceDate1st, 5, 2), '01') || '-'
                     || coalesce(substr(recurrenceDate1st, 7, 2), '01')) end as recurrenceDate1st 
  , case when trim(recurrenceDate1stFlag) > '' then trim(recurrenceDate1stFlag) end as recurrenceDate1stFlag
  , case when trim(recurrenceType1st) > '' then trim(recurrenceType1st) end as recurrenceType1st
  , case when trim(causeOfDeath) > '' then trim(causeOfDeath) end as causeOfDeath
  , case when trim(seerCauseSpecificCod) > '' then trim(seerCauseSpecificCod) end as seerCauseSpecificCod
  , case when trim(seerOtherCod) > '' then trim(seerOtherCod) end as seerOtherCod
  , case when trim(icdRevisionNumber) > '' then trim(icdRevisionNumber) end as icdRevisionNumber
  , case when trim(autopsy) > '' then trim(autopsy) end as autopsy
  , case when trim(placeOfDeath) > '' then trim(placeOfDeath) end as placeOfDeath
  , case when trim(sequenceNumberHospital) > '' then trim(sequenceNumberHospital) end as sequenceNumberHospital
  , case when trim(abstractedBy) > '' then trim(abstractedBy) end as abstractedBy
  , case when trim(dateOf1stContact) > '' then date(substr(dateOf1stContact, 1, 4) || '-' || coalesce(substr(dateOf1stContact, 5, 2), '01') || '-'
                     || coalesce(substr(dateOf1stContact, 7, 2), '01')) end as dateOf1stContact 
  , case when trim(dateOf1stContactFlag) > '' then trim(dateOf1stContactFlag) end as dateOf1stContactFlag
  , case when trim(dateOfInptAdm) > '' then date(substr(dateOfInptAdm, 1, 4) || '-' || coalesce(substr(dateOfInptAdm, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfInptAdm, 7, 2), '01')) end as dateOfInptAdm 
  , case when trim(dateOfInptAdmFlag) > '' then trim(dateOfInptAdmFlag) end as dateOfInptAdmFlag
  , case when trim(dateOfInptDisch) > '' then date(substr(dateOfInptDisch, 1, 4) || '-' || coalesce(substr(dateOfInptDisch, 5, 2), '01') || '-'
                     || coalesce(substr(dateOfInptDisch, 7, 2), '01')) end as dateOfInptDisch 
  , case when trim(dateOfInptDischFlag) > '' then trim(dateOfInptDischFlag) end as dateOfInptDischFlag
  , case when trim(inpatientStatus) > '' then trim(inpatientStatus) end as inpatientStatus
  , case when trim(classOfCase) > '' then trim(classOfCase) end as classOfCase
  , case when trim(primaryPayerAtDx) > '' then trim(primaryPayerAtDx) end as primaryPayerAtDx
  , case when trim(rxHospSurgApp2010) > '' then trim(rxHospSurgApp2010) end as rxHospSurgApp2010
  , case when trim(rxHospSurgPrimSite) > '' then trim(rxHospSurgPrimSite) end as rxHospSurgPrimSite
  , case when trim(rxHospScopeRegLnSur) > '' then trim(rxHospScopeRegLnSur) end as rxHospScopeRegLnSur
  , case when trim(rxHospSurgOthRegDis) > '' then trim(rxHospSurgOthRegDis) end as rxHospSurgOthRegDis
  , case when trim(rxHospRegLnRemoved) > '' then 0 + rxHospRegLnRemoved end as rxHospRegLnRemoved
  , case when trim(rxHospRadiation) > '' then trim(rxHospRadiation) end as rxHospRadiation
  , case when trim(rxHospChemo) > '' then trim(rxHospChemo) end as rxHospChemo
  , case when trim(rxHospHormone) > '' then trim(rxHospHormone) end as rxHospHormone
  , case when trim(rxHospBrm) > '' then trim(rxHospBrm) end as rxHospBrm
  , case when trim(rxHospOther) > '' then trim(rxHospOther) end as rxHospOther
  , case when trim(rxHospDxStgProc) > '' then trim(rxHospDxStgProc) end as rxHospDxStgProc
  , case when trim(rxHospSurgSite9802) > '' then trim(rxHospSurgSite9802) end as rxHospSurgSite9802
  , case when trim(rxHospScopeReg9802) > '' then trim(rxHospScopeReg9802) end as rxHospScopeReg9802
  , case when trim(rxHospSurgOth9802) > '' then trim(rxHospSurgOth9802) end as rxHospSurgOth9802
  , case when trim(rxHospPalliativeProc) > '' then trim(rxHospPalliativeProc) end as rxHospPalliativeProc
  , case when trim(recordType) > '' then trim(recordType) end as recordType
  , case when trim(registryType) > '' then trim(registryType) end as registryType
  , case when trim(registryId) > '' then trim(registryId) end as registryId
  , case when trim(npiRegistryId) > '' then trim(npiRegistryId) end as npiRegistryId
  , case when trim(naaccrRecordVersion) > '' then trim(naaccrRecordVersion) end as naaccrRecordVersion
  , case when trim(tumorRecordNumber) > '' then trim(tumorRecordNumber) end as tumorRecordNumber
  , case when trim(dateRegionalLNDissection) > '' then date(substr(dateRegionalLNDissection, 1, 4) || '-' || coalesce(substr(dateRegionalLNDissection, 5, 2), '01') || '-'
                     || coalesce(substr(dateRegionalLNDissection, 7, 2), '01')) end as dateRegionalLNDissection 
  , case when trim(dateRegionalLNDissectionFlag) > '' then trim(dateRegionalLNDissectionFlag) end as dateRegionalLNDissectionFlag
  , case when trim(tumorSizeClinical) > '' then trim(tumorSizeClinical) end as tumorSizeClinical
  , case when trim(tumorSizePathologic) > '' then trim(tumorSizePathologic) end as tumorSizePathologic
  , case when trim(tumorSizeSummary) > '' then trim(tumorSizeSummary) end as tumorSizeSummary
  , case when trim(seerSummaryStage2000) > '' then trim(seerSummaryStage2000) end as seerSummaryStage2000
  , case when trim(seerSummaryStage1977) > '' then trim(seerSummaryStage1977) end as seerSummaryStage1977
  , case when trim(derivedSummaryStage2018) > '' then trim(derivedSummaryStage2018) end as derivedSummaryStage2018
  , case when trim(summaryStage2018) > '' then trim(summaryStage2018) end as summaryStage2018
  , case when trim(eodPrimaryTumor) > '' then trim(eodPrimaryTumor) end as eodPrimaryTumor
  , case when trim(eodRegionalNodes) > '' then trim(eodRegionalNodes) end as eodRegionalNodes
  , case when trim(eodMets) > '' then trim(eodMets) end as eodMets
  , case when trim(eodTumorSize) > '' then 0 + eodTumorSize end as eodTumorSize
  , case when trim(eodExtension) > '' then trim(eodExtension) end as eodExtension
  , case when trim(eodExtensionProstPath) > '' then trim(eodExtensionProstPath) end as eodExtensionProstPath
  , case when trim(eodLymphNodeInvolv) > '' then trim(eodLymphNodeInvolv) end as eodLymphNodeInvolv
  , case when trim(regionalNodesPositive) > '' then 0 + regionalNodesPositive end as regionalNodesPositive
  , case when trim(regionalNodesExamined) > '' then 0 + regionalNodesExamined end as regionalNodesExamined
  , case when trim(dateSentinelLymphNodeBiopsy) > '' then date(substr(dateSentinelLymphNodeBiopsy, 1, 4) || '-' || coalesce(substr(dateSentinelLymphNodeBiopsy, 5, 2), '01') || '-'
                     || coalesce(substr(dateSentinelLymphNodeBiopsy, 7, 2), '01')) end as dateSentinelLymphNodeBiopsy 
  , case when trim(dateSentinelLymphNodeBiopsyFlag) > '' then trim(dateSentinelLymphNodeBiopsyFlag) end as dateSentinelLymphNodeBiopsyFlag
  , case when trim(sentinelLymphNodesExamined) > '' then trim(sentinelLymphNodesExamined) end as sentinelLymphNodesExamined
  , case when trim(sentinelLymphNodesPositive) > '' then trim(sentinelLymphNodesPositive) end as sentinelLymphNodesPositive
  , case when trim(eodOld2Digit) > '' then trim(eodOld2Digit) end as eodOld2Digit
  , case when trim(eodOld4Digit) > '' then trim(eodOld4Digit) end as eodOld4Digit
  , case when trim(codingSystemForEod) > '' then trim(codingSystemForEod) end as codingSystemForEod
  , case when trim(tnmPathT) > '' then trim(tnmPathT) end as tnmPathT
  , case when trim(tnmPathN) > '' then trim(tnmPathN) end as tnmPathN
  , case when trim(tnmPathM) > '' then trim(tnmPathM) end as tnmPathM
  , case when trim(tnmPathStageGroup) > '' then trim(tnmPathStageGroup) end as tnmPathStageGroup
  , case when trim(tnmPathDescriptor) > '' then trim(tnmPathDescriptor) end as tnmPathDescriptor
  , case when trim(tnmPathStagedBy) > '' then trim(tnmPathStagedBy) end as tnmPathStagedBy
  , case when trim(tnmClinT) > '' then trim(tnmClinT) end as tnmClinT
  , case when trim(tnmClinN) > '' then trim(tnmClinN) end as tnmClinN
  , case when trim(tnmClinM) > '' then trim(tnmClinM) end as tnmClinM
  , case when trim(tnmClinStageGroup) > '' then trim(tnmClinStageGroup) end as tnmClinStageGroup
  , case when trim(tnmClinDescriptor) > '' then trim(tnmClinDescriptor) end as tnmClinDescriptor
  , case when trim(tnmClinStagedBy) > '' then trim(tnmClinStagedBy) end as tnmClinStagedBy
  , case when trim(ajccId) > '' then trim(ajccId) end as ajccId
  , case when trim(ajccTnmClinTSuffix) > '' then trim(ajccTnmClinTSuffix) end as ajccTnmClinTSuffix
  , case when trim(ajccTnmPathTSuffix) > '' then trim(ajccTnmPathTSuffix) end as ajccTnmPathTSuffix
  , case when trim(ajccTnmPostTherapyTSuffix) > '' then trim(ajccTnmPostTherapyTSuffix) end as ajccTnmPostTherapyTSuffix
  , case when trim(ajccTnmClinNSuffix) > '' then trim(ajccTnmClinNSuffix) end as ajccTnmClinNSuffix
  , case when trim(ajccTnmPathNSuffix) > '' then trim(ajccTnmPathNSuffix) end as ajccTnmPathNSuffix
  , case when trim(ajccTnmPostTherapyNSuffix) > '' then trim(ajccTnmPostTherapyNSuffix) end as ajccTnmPostTherapyNSuffix
  , case when trim(tnmEditionNumber) > '' then trim(tnmEditionNumber) end as tnmEditionNumber
  , case when trim(metsAtDxBone) > '' then trim(metsAtDxBone) end as metsAtDxBone
  , case when trim(metsAtDxBrain) > '' then trim(metsAtDxBrain) end as metsAtDxBrain
  , case when trim(metsAtDxDistantLn) > '' then trim(metsAtDxDistantLn) end as metsAtDxDistantLn
  , case when trim(metsAtDxLiver) > '' then trim(metsAtDxLiver) end as metsAtDxLiver
  , case when trim(metsAtDxLung) > '' then trim(metsAtDxLung) end as metsAtDxLung
  , case when trim(metsAtDxOther) > '' then trim(metsAtDxOther) end as metsAtDxOther
  , case when trim(pediatricStage) > '' then trim(pediatricStage) end as pediatricStage
  , case when trim(pediatricStagingSystem) > '' then trim(pediatricStagingSystem) end as pediatricStagingSystem
  , case when trim(pediatricStagedBy) > '' then trim(pediatricStagedBy) end as pediatricStagedBy
  , case when trim(tumorMarker1) > '' then trim(tumorMarker1) end as tumorMarker1
  , case when trim(tumorMarker2) > '' then trim(tumorMarker2) end as tumorMarker2
  , case when trim(tumorMarker3) > '' then trim(tumorMarker3) end as tumorMarker3
  , case when trim(lymphVascularInvasion) > '' then trim(lymphVascularInvasion) end as lymphVascularInvasion
  , case when trim(csTumorSize) > '' then 0 + csTumorSize end as csTumorSize
  , case when trim(csExtension) > '' then trim(csExtension) end as csExtension
  , case when trim(csTumorSizeExtEval) > '' then trim(csTumorSizeExtEval) end as csTumorSizeExtEval
  , case when trim(csLymphNodes) > '' then trim(csLymphNodes) end as csLymphNodes
  , case when trim(csLymphNodesEval) > '' then trim(csLymphNodesEval) end as csLymphNodesEval
  , case when trim(csMetsAtDx) > '' then trim(csMetsAtDx) end as csMetsAtDx
  , case when trim(csMetsAtDxBone) > '' then trim(csMetsAtDxBone) end as csMetsAtDxBone
  , case when trim(csMetsAtDxBrain) > '' then trim(csMetsAtDxBrain) end as csMetsAtDxBrain
  , case when trim(csMetsAtDxLiver) > '' then trim(csMetsAtDxLiver) end as csMetsAtDxLiver
  , case when trim(csMetsAtDxLung) > '' then trim(csMetsAtDxLung) end as csMetsAtDxLung
  , case when trim(csMetsEval) > '' then trim(csMetsEval) end as csMetsEval
  , case when trim(csSiteSpecificFactor7) > '' then trim(csSiteSpecificFactor7) end as csSiteSpecificFactor7
  , case when trim(csSiteSpecificFactor8) > '' then trim(csSiteSpecificFactor8) end as csSiteSpecificFactor8
  , case when trim(csSiteSpecificFactor9) > '' then trim(csSiteSpecificFactor9) end as csSiteSpecificFactor9
  , case when trim(csSiteSpecificFactor10) > '' then trim(csSiteSpecificFactor10) end as csSiteSpecificFactor10
  , case when trim(csSiteSpecificFactor11) > '' then trim(csSiteSpecificFactor11) end as csSiteSpecificFactor11
  , case when trim(csSiteSpecificFactor12) > '' then trim(csSiteSpecificFactor12) end as csSiteSpecificFactor12
  , case when trim(csSiteSpecificFactor13) > '' then trim(csSiteSpecificFactor13) end as csSiteSpecificFactor13
  , case when trim(csSiteSpecificFactor14) > '' then trim(csSiteSpecificFactor14) end as csSiteSpecificFactor14
  , case when trim(csSiteSpecificFactor15) > '' then trim(csSiteSpecificFactor15) end as csSiteSpecificFactor15
  , case when trim(csSiteSpecificFactor16) > '' then trim(csSiteSpecificFactor16) end as csSiteSpecificFactor16
  , case when trim(csSiteSpecificFactor17) > '' then trim(csSiteSpecificFactor17) end as csSiteSpecificFactor17
  , case when trim(csSiteSpecificFactor18) > '' then trim(csSiteSpecificFactor18) end as csSiteSpecificFactor18
  , case when trim(csSiteSpecificFactor19) > '' then trim(csSiteSpecificFactor19) end as csSiteSpecificFactor19
  , case when trim(csSiteSpecificFactor20) > '' then trim(csSiteSpecificFactor20) end as csSiteSpecificFactor20
  , case when trim(csSiteSpecificFactor21) > '' then trim(csSiteSpecificFactor21) end as csSiteSpecificFactor21
  , case when trim(csSiteSpecificFactor22) > '' then trim(csSiteSpecificFactor22) end as csSiteSpecificFactor22
  , case when trim(csSiteSpecificFactor23) > '' then trim(csSiteSpecificFactor23) end as csSiteSpecificFactor23
  , case when trim(csSiteSpecificFactor24) > '' then trim(csSiteSpecificFactor24) end as csSiteSpecificFactor24
  , case when trim(csSiteSpecificFactor25) > '' then trim(csSiteSpecificFactor25) end as csSiteSpecificFactor25
  , case when trim(csSiteSpecificFactor1) > '' then trim(csSiteSpecificFactor1) end as csSiteSpecificFactor1
  , case when trim(csSiteSpecificFactor2) > '' then trim(csSiteSpecificFactor2) end as csSiteSpecificFactor2
  , case when trim(csSiteSpecificFactor3) > '' then trim(csSiteSpecificFactor3) end as csSiteSpecificFactor3
  , case when trim(csSiteSpecificFactor4) > '' then trim(csSiteSpecificFactor4) end as csSiteSpecificFactor4
  , case when trim(csSiteSpecificFactor5) > '' then trim(csSiteSpecificFactor5) end as csSiteSpecificFactor5
  , case when trim(csSiteSpecificFactor6) > '' then trim(csSiteSpecificFactor6) end as csSiteSpecificFactor6
  , case when trim(csVersionInputOriginal) > '' then trim(csVersionInputOriginal) end as csVersionInputOriginal
  , case when trim(csVersionDerived) > '' then trim(csVersionDerived) end as csVersionDerived
  , case when trim(csVersionInputCurrent) > '' then trim(csVersionInputCurrent) end as csVersionInputCurrent
  , case when trim(derivedAjcc6T) > '' then trim(derivedAjcc6T) end as derivedAjcc6T
  , case when trim(derivedAjcc6TDescript) > '' then trim(derivedAjcc6TDescript) end as derivedAjcc6TDescript
  , case when trim(derivedAjcc6N) > '' then trim(derivedAjcc6N) end as derivedAjcc6N
  , case when trim(derivedAjcc6NDescript) > '' then trim(derivedAjcc6NDescript) end as derivedAjcc6NDescript
  , case when trim(derivedAjcc6M) > '' then trim(derivedAjcc6M) end as derivedAjcc6M
  , case when trim(derivedAjcc6MDescript) > '' then trim(derivedAjcc6MDescript) end as derivedAjcc6MDescript
  , case when trim(derivedAjcc6StageGrp) > '' then trim(derivedAjcc6StageGrp) end as derivedAjcc6StageGrp
  , case when trim(derivedSs1977) > '' then trim(derivedSs1977) end as derivedSs1977
  , case when trim(derivedSs2000) > '' then trim(derivedSs2000) end as derivedSs2000
  , case when trim(derivedAjccFlag) > '' then trim(derivedAjccFlag) end as derivedAjccFlag
  , case when trim(derivedSs1977Flag) > '' then trim(derivedSs1977Flag) end as derivedSs1977Flag
  , case when trim(derivedSs2000Flag) > '' then trim(derivedSs2000Flag) end as derivedSs2000Flag
  , case when trim(comorbidComplication1) > '' then trim(comorbidComplication1) end as comorbidComplication1
  , case when trim(comorbidComplication2) > '' then trim(comorbidComplication2) end as comorbidComplication2
  , case when trim(comorbidComplication3) > '' then trim(comorbidComplication3) end as comorbidComplication3
  , case when trim(comorbidComplication4) > '' then trim(comorbidComplication4) end as comorbidComplication4
  , case when trim(comorbidComplication5) > '' then trim(comorbidComplication5) end as comorbidComplication5
  , case when trim(comorbidComplication6) > '' then trim(comorbidComplication6) end as comorbidComplication6
  , case when trim(comorbidComplication7) > '' then trim(comorbidComplication7) end as comorbidComplication7
  , case when trim(comorbidComplication8) > '' then trim(comorbidComplication8) end as comorbidComplication8
  , case when trim(comorbidComplication9) > '' then trim(comorbidComplication9) end as comorbidComplication9
  , case when trim(comorbidComplication10) > '' then trim(comorbidComplication10) end as comorbidComplication10
  , case when trim(icdRevisionComorbid) > '' then trim(icdRevisionComorbid) end as icdRevisionComorbid
  , case when trim(derivedAjcc7T) > '' then trim(derivedAjcc7T) end as derivedAjcc7T
  , case when trim(derivedAjcc7TDescript) > '' then trim(derivedAjcc7TDescript) end as derivedAjcc7TDescript
  , case when trim(derivedAjcc7N) > '' then trim(derivedAjcc7N) end as derivedAjcc7N
  , case when trim(derivedAjcc7NDescript) > '' then trim(derivedAjcc7NDescript) end as derivedAjcc7NDescript
  , case when trim(derivedAjcc7M) > '' then trim(derivedAjcc7M) end as derivedAjcc7M
  , case when trim(derivedAjcc7MDescript) > '' then trim(derivedAjcc7MDescript) end as derivedAjcc7MDescript
  , case when trim(derivedAjcc7StageGrp) > '' then trim(derivedAjcc7StageGrp) end as derivedAjcc7StageGrp
  , case when trim(derivedPrerx7T) > '' then trim(derivedPrerx7T) end as derivedPrerx7T
  , case when trim(derivedPrerx7TDescrip) > '' then trim(derivedPrerx7TDescrip) end as derivedPrerx7TDescrip
  , case when trim(derivedPrerx7N) > '' then trim(derivedPrerx7N) end as derivedPrerx7N
  , case when trim(derivedPrerx7NDescrip) > '' then trim(derivedPrerx7NDescrip) end as derivedPrerx7NDescrip
  , case when trim(derivedPrerx7M) > '' then trim(derivedPrerx7M) end as derivedPrerx7M
  , case when trim(derivedPrerx7MDescrip) > '' then trim(derivedPrerx7MDescrip) end as derivedPrerx7MDescrip
  , case when trim(derivedPrerx7StageGrp) > '' then trim(derivedPrerx7StageGrp) end as derivedPrerx7StageGrp
  , case when trim(derivedPostrx7T) > '' then trim(derivedPostrx7T) end as derivedPostrx7T
  , case when trim(derivedPostrx7N) > '' then trim(derivedPostrx7N) end as derivedPostrx7N
  , case when trim(derivedPostrx7M) > '' then trim(derivedPostrx7M) end as derivedPostrx7M
  , case when trim(derivedPostrx7StgeGrp) > '' then trim(derivedPostrx7StgeGrp) end as derivedPostrx7StgeGrp
  , case when trim(derivedNeoadjuvRxFlag) > '' then trim(derivedNeoadjuvRxFlag) end as derivedNeoadjuvRxFlag
  , case when trim(derivedSeerPathStgGrp) > '' then trim(derivedSeerPathStgGrp) end as derivedSeerPathStgGrp
  , case when trim(derivedSeerClinStgGrp) > '' then trim(derivedSeerClinStgGrp) end as derivedSeerClinStgGrp
  , case when trim(derivedSeerCmbStgGrp) > '' then trim(derivedSeerCmbStgGrp) end as derivedSeerCmbStgGrp
  , case when trim(derivedSeerCombinedT) > '' then trim(derivedSeerCombinedT) end as derivedSeerCombinedT
  , case when trim(derivedSeerCombinedN) > '' then trim(derivedSeerCombinedN) end as derivedSeerCombinedN
  , case when trim(derivedSeerCombinedM) > '' then trim(derivedSeerCombinedM) end as derivedSeerCombinedM
  , case when trim(derivedSeerCmbTSrc) > '' then trim(derivedSeerCmbTSrc) end as derivedSeerCmbTSrc
  , case when trim(derivedSeerCmbNSrc) > '' then trim(derivedSeerCmbNSrc) end as derivedSeerCmbNSrc
  , case when trim(derivedSeerCmbMSrc) > '' then trim(derivedSeerCmbMSrc) end as derivedSeerCmbMSrc
  , case when trim(npcrDerivedClinStgGrp) > '' then trim(npcrDerivedClinStgGrp) end as npcrDerivedClinStgGrp
  , case when trim(npcrDerivedPathStgGrp) > '' then trim(npcrDerivedPathStgGrp) end as npcrDerivedPathStgGrp
  , case when trim(seerSiteSpecificFact1) > '' then trim(seerSiteSpecificFact1) end as seerSiteSpecificFact1
  , case when trim(seerSiteSpecificFact2) > '' then trim(seerSiteSpecificFact2) end as seerSiteSpecificFact2
  , case when trim(seerSiteSpecificFact3) > '' then trim(seerSiteSpecificFact3) end as seerSiteSpecificFact3
  , case when trim(seerSiteSpecificFact4) > '' then trim(seerSiteSpecificFact4) end as seerSiteSpecificFact4
  , case when trim(seerSiteSpecificFact5) > '' then trim(seerSiteSpecificFact5) end as seerSiteSpecificFact5
  , case when trim(seerSiteSpecificFact6) > '' then trim(seerSiteSpecificFact6) end as seerSiteSpecificFact6
  , case when trim(secondaryDiagnosis1) > '' then trim(secondaryDiagnosis1) end as secondaryDiagnosis1
  , case when trim(secondaryDiagnosis2) > '' then trim(secondaryDiagnosis2) end as secondaryDiagnosis2
  , case when trim(secondaryDiagnosis3) > '' then trim(secondaryDiagnosis3) end as secondaryDiagnosis3
  , case when trim(secondaryDiagnosis4) > '' then trim(secondaryDiagnosis4) end as secondaryDiagnosis4
  , case when trim(secondaryDiagnosis5) > '' then trim(secondaryDiagnosis5) end as secondaryDiagnosis5
  , case when trim(secondaryDiagnosis6) > '' then trim(secondaryDiagnosis6) end as secondaryDiagnosis6
  , case when trim(secondaryDiagnosis7) > '' then trim(secondaryDiagnosis7) end as secondaryDiagnosis7
  , case when trim(secondaryDiagnosis8) > '' then trim(secondaryDiagnosis8) end as secondaryDiagnosis8
  , case when trim(secondaryDiagnosis9) > '' then trim(secondaryDiagnosis9) end as secondaryDiagnosis9
  , case when trim(secondaryDiagnosis10) > '' then trim(secondaryDiagnosis10) end as secondaryDiagnosis10
  , case when trim(schemaId) > '' then trim(schemaId) end as schemaId
  , case when trim(chromosome1pLossHeterozygosity) > '' then trim(chromosome1pLossHeterozygosity) end as chromosome1pLossHeterozygosity
  , case when trim(chromosome19qLossHeterozygosity) > '' then trim(chromosome19qLossHeterozygosity) end as chromosome19qLossHeterozygosity
  , case when trim(adenoidCysticBasaloidPattern) > '' then trim(adenoidCysticBasaloidPattern) end as adenoidCysticBasaloidPattern
  , case when trim(adenopathy) > '' then trim(adenopathy) end as adenopathy
  , case when trim(afpPostOrchiectomyLabValue) > '' then 0 + afpPostOrchiectomyLabValue end as afpPostOrchiectomyLabValue
  , case when trim(afpPostOrchiectomyRange) > '' then trim(afpPostOrchiectomyRange) end as afpPostOrchiectomyRange
  , case when trim(afpPreOrchiectomyLabValue) > '' then 0 + afpPreOrchiectomyLabValue end as afpPreOrchiectomyLabValue
  , case when trim(afpPreOrchiectomyRange) > '' then trim(afpPreOrchiectomyRange) end as afpPreOrchiectomyRange
  , case when trim(afpPretreatmentInterpretation) > '' then trim(afpPretreatmentInterpretation) end as afpPretreatmentInterpretation
  , case when trim(afpPretreatmentLabValue) > '' then 0 + afpPretreatmentLabValue end as afpPretreatmentLabValue
  , case when trim(anemia) > '' then trim(anemia) end as anemia
  , case when trim(bSymptoms) > '' then trim(bSymptoms) end as bSymptoms
  , case when trim(bilirubinPretxTotalLabValue) > '' then trim(bilirubinPretxTotalLabValue) end as bilirubinPretxTotalLabValue
  , case when trim(bilirubinPretxUnitOfMeasure) > '' then trim(bilirubinPretxUnitOfMeasure) end as bilirubinPretxUnitOfMeasure
  , case when trim(boneInvasion) > '' then trim(boneInvasion) end as boneInvasion
  , case when trim(brainMolecularMarkers) > '' then trim(brainMolecularMarkers) end as brainMolecularMarkers
  , case when trim(breslowTumorThickness) > '' then trim(breslowTumorThickness) end as breslowTumorThickness
  , case when trim(ca125PretreatmentInterpretation) > '' then trim(ca125PretreatmentInterpretation) end as ca125PretreatmentInterpretation
  , case when trim(ceaPretreatmentInterpretation) > '' then trim(ceaPretreatmentInterpretation) end as ceaPretreatmentInterpretation
  , case when trim(ceaPretreatmentLabValue) > '' then 0 + ceaPretreatmentLabValue end as ceaPretreatmentLabValue
  , case when trim(chromosome3Status) > '' then trim(chromosome3Status) end as chromosome3Status
  , case when trim(chromosome8qStatus) > '' then trim(chromosome8qStatus) end as chromosome8qStatus
  , case when trim(circumferentialResectionMargin) > '' then trim(circumferentialResectionMargin) end as circumferentialResectionMargin
  , case when trim(creatininePretreatmentLabValue) > '' then trim(creatininePretreatmentLabValue) end as creatininePretreatmentLabValue
  , case when trim(creatininePretxUnitOfMeasure) > '' then trim(creatininePretxUnitOfMeasure) end as creatininePretxUnitOfMeasure
  , case when trim(estrogenReceptorPercntPosOrRange) > '' then trim(estrogenReceptorPercntPosOrRange) end as estrogenReceptorPercntPosOrRange
  , case when trim(estrogenReceptorSummary) > '' then trim(estrogenReceptorSummary) end as estrogenReceptorSummary
  , case when trim(estrogenReceptorTotalAllredScore) > '' then trim(estrogenReceptorTotalAllredScore) end as estrogenReceptorTotalAllredScore
  , case when trim(esophagusAndEgjTumorEpicenter) > '' then trim(esophagusAndEgjTumorEpicenter) end as esophagusAndEgjTumorEpicenter
  , case when trim(extranodalExtensionClin) > '' then trim(extranodalExtensionClin) end as extranodalExtensionClin
  , case when trim(extranodalExtensionHeadNeckClin) > '' then trim(extranodalExtensionHeadNeckClin) end as extranodalExtensionHeadNeckClin
  , case when trim(extranodalExtensionHeadNeckPath) > '' then trim(extranodalExtensionHeadNeckPath) end as extranodalExtensionHeadNeckPath
  , case when trim(extranodalExtensionPath) > '' then trim(extranodalExtensionPath) end as extranodalExtensionPath
  , case when trim(extravascularMatrixPatterns) > '' then trim(extravascularMatrixPatterns) end as extravascularMatrixPatterns
  , case when trim(fibrosisScore) > '' then trim(fibrosisScore) end as fibrosisScore
  , case when trim(figoStage) > '' then trim(figoStage) end as figoStage
  , case when trim(gestationalTrophoblasticPxIndex) > '' then trim(gestationalTrophoblasticPxIndex) end as gestationalTrophoblasticPxIndex
  , case when trim(gleasonPatternsClinical) > '' then trim(gleasonPatternsClinical) end as gleasonPatternsClinical
  , case when trim(gleasonPatternsPathological) > '' then trim(gleasonPatternsPathological) end as gleasonPatternsPathological
  , case when trim(gleasonScoreClinical) > '' then trim(gleasonScoreClinical) end as gleasonScoreClinical
  , case when trim(gleasonScorePathological) > '' then trim(gleasonScorePathological) end as gleasonScorePathological
  , case when trim(gleasonTertiaryPattern) > '' then trim(gleasonTertiaryPattern) end as gleasonTertiaryPattern
  , case when trim(gradeClinical) > '' then trim(gradeClinical) end as gradeClinical
  , case when trim(gradePathological) > '' then trim(gradePathological) end as gradePathological
  , case when trim(gradePostTherapy) > '' then trim(gradePostTherapy) end as gradePostTherapy
  , case when trim(hcgPostOrchiectomyLabValue) > '' then 0 + hcgPostOrchiectomyLabValue end as hcgPostOrchiectomyLabValue
  , case when trim(hcgPostOrchiectomyRange) > '' then trim(hcgPostOrchiectomyRange) end as hcgPostOrchiectomyRange
  , case when trim(hcgPreOrchiectomyLabValue) > '' then 0 + hcgPreOrchiectomyLabValue end as hcgPreOrchiectomyLabValue
  , case when trim(hcgPreOrchiectomyRange) > '' then trim(hcgPreOrchiectomyRange) end as hcgPreOrchiectomyRange
  , case when trim(her2IhcSummary) > '' then trim(her2IhcSummary) end as her2IhcSummary
  , case when trim(her2IshDualProbeCopyNumber) > '' then trim(her2IshDualProbeCopyNumber) end as her2IshDualProbeCopyNumber
  , case when trim(her2IshDualProbeRatio) > '' then trim(her2IshDualProbeRatio) end as her2IshDualProbeRatio
  , case when trim(her2IshSingleProbeCopyNumber) > '' then trim(her2IshSingleProbeCopyNumber) end as her2IshSingleProbeCopyNumber
  , case when trim(her2IshSummary) > '' then trim(her2IshSummary) end as her2IshSummary
  , case when trim(her2OverallSummary) > '' then trim(her2OverallSummary) end as her2OverallSummary
  , case when trim(heritableTrait) > '' then trim(heritableTrait) end as heritableTrait
  , case when trim(highRiskCytogenetics) > '' then trim(highRiskCytogenetics) end as highRiskCytogenetics
  , case when trim(highRiskHistologicFeatures) > '' then trim(highRiskHistologicFeatures) end as highRiskHistologicFeatures
  , case when trim(hivStatus) > '' then trim(hivStatus) end as hivStatus
  , case when trim(iNRProthrombinTime) > '' then trim(iNRProthrombinTime) end as iNRProthrombinTime
  , case when trim(ipsilateralAdrenalGlandInvolve) > '' then trim(ipsilateralAdrenalGlandInvolve) end as ipsilateralAdrenalGlandInvolve
  , case when trim(jak2) > '' then trim(jak2) end as jak2
  , case when trim(ki67) > '' then trim(ki67) end as ki67
  , case when trim(invasionBeyondCapsule) > '' then trim(invasionBeyondCapsule) end as invasionBeyondCapsule
  , case when trim(kitGeneImmunohistochemistry) > '' then trim(kitGeneImmunohistochemistry) end as kitGeneImmunohistochemistry
  , case when trim(kras) > '' then trim(kras) end as kras
  , case when trim(ldhPostOrchiectomyRange) > '' then trim(ldhPostOrchiectomyRange) end as ldhPostOrchiectomyRange
  , case when trim(ldhPreOrchiectomyRange) > '' then trim(ldhPreOrchiectomyRange) end as ldhPreOrchiectomyRange
  , case when trim(ldhPretreatmentLevel) > '' then trim(ldhPretreatmentLevel) end as ldhPretreatmentLevel
  , case when trim(ldhUpperLimitsOfNormal) > '' then trim(ldhUpperLimitsOfNormal) end as ldhUpperLimitsOfNormal
  , case when trim(lnAssessMethodFemoralInguinal) > '' then trim(lnAssessMethodFemoralInguinal) end as lnAssessMethodFemoralInguinal
  , case when trim(lnAssessMethodParaaortic) > '' then trim(lnAssessMethodParaaortic) end as lnAssessMethodParaaortic
  , case when trim(lnAssessMethodPelvic) > '' then trim(lnAssessMethodPelvic) end as lnAssessMethodPelvic
  , case when trim(lnDistantAssessMethod) > '' then trim(lnDistantAssessMethod) end as lnDistantAssessMethod
  , case when trim(lnDistantMediastinalScalene) > '' then trim(lnDistantMediastinalScalene) end as lnDistantMediastinalScalene
  , case when trim(lnHeadAndNeckLevels1To3) > '' then trim(lnHeadAndNeckLevels1To3) end as lnHeadAndNeckLevels1To3
  , case when trim(lnHeadAndNeckLevels4To5) > '' then trim(lnHeadAndNeckLevels4To5) end as lnHeadAndNeckLevels4To5
  , case when trim(lnHeadAndNeckLevels6To7) > '' then trim(lnHeadAndNeckLevels6To7) end as lnHeadAndNeckLevels6To7
  , case when trim(lnHeadAndNeckOther) > '' then trim(lnHeadAndNeckOther) end as lnHeadAndNeckOther
  , case when trim(lnIsolatedTumorCells) > '' then trim(lnIsolatedTumorCells) end as lnIsolatedTumorCells
  , case when trim(lnLaterality) > '' then trim(lnLaterality) end as lnLaterality
  , case when trim(lnPositiveAxillaryLevel1To2) > '' then trim(lnPositiveAxillaryLevel1To2) end as lnPositiveAxillaryLevel1To2
  , case when trim(lnSize) > '' then trim(lnSize) end as lnSize
  , case when trim(lnStatusFemorInguinParaaortPelv) > '' then trim(lnStatusFemorInguinParaaortPelv) end as lnStatusFemorInguinParaaortPelv
  , case when trim(lymphocytosis) > '' then trim(lymphocytosis) end as lymphocytosis
  , case when trim(majorVeinInvolvement) > '' then trim(majorVeinInvolvement) end as majorVeinInvolvement
  , case when trim(measuredBasalDiameter) > '' then trim(measuredBasalDiameter) end as measuredBasalDiameter
  , case when trim(measuredThickness) > '' then trim(measuredThickness) end as measuredThickness
  , case when trim(methylationOfO6MGMT) > '' then trim(methylationOfO6MGMT) end as methylationOfO6MGMT
  , case when trim(microsatelliteInstability) > '' then trim(microsatelliteInstability) end as microsatelliteInstability
  , case when trim(microvascularDensity) > '' then trim(microvascularDensity) end as microvascularDensity
  , case when trim(mitoticCountUvealMelanoma) > '' then trim(mitoticCountUvealMelanoma) end as mitoticCountUvealMelanoma
  , case when trim(mitoticRateMelanoma) > '' then trim(mitoticRateMelanoma) end as mitoticRateMelanoma
  , case when trim(multigeneSignatureMethod) > '' then trim(multigeneSignatureMethod) end as multigeneSignatureMethod
  , case when trim(multigeneSignatureResults) > '' then trim(multigeneSignatureResults) end as multigeneSignatureResults
  , case when trim(nccnInternationalPrognosticIndex) > '' then trim(nccnInternationalPrognosticIndex) end as nccnInternationalPrognosticIndex
  , case when trim(numberOfCoresExamined) > '' then trim(numberOfCoresExamined) end as numberOfCoresExamined
  , case when trim(numberOfCoresPositive) > '' then trim(numberOfCoresPositive) end as numberOfCoresPositive
  , case when trim(numberOfExaminedParaAorticNodes) > '' then trim(numberOfExaminedParaAorticNodes) end as numberOfExaminedParaAorticNodes
  , case when trim(numberOfExaminedPelvicNodes) > '' then trim(numberOfExaminedPelvicNodes) end as numberOfExaminedPelvicNodes
  , case when trim(numberOfPositiveParaAorticNodes) > '' then trim(numberOfPositiveParaAorticNodes) end as numberOfPositiveParaAorticNodes
  , case when trim(numberOfPositivePelvicNodes) > '' then trim(numberOfPositivePelvicNodes) end as numberOfPositivePelvicNodes
  , case when trim(oncotypeDxRecurrenceScoreDcis) > '' then trim(oncotypeDxRecurrenceScoreDcis) end as oncotypeDxRecurrenceScoreDcis
  , case when trim(oncotypeDxRecurrenceScoreInvasiv) > '' then trim(oncotypeDxRecurrenceScoreInvasiv) end as oncotypeDxRecurrenceScoreInvasiv
  , case when trim(oncotypeDxRiskLevelDcis) > '' then trim(oncotypeDxRiskLevelDcis) end as oncotypeDxRiskLevelDcis
  , case when trim(oncotypeDxRiskLevelInvasive) > '' then trim(oncotypeDxRiskLevelInvasive) end as oncotypeDxRiskLevelInvasive
  , case when trim(organomegaly) > '' then trim(organomegaly) end as organomegaly
  , case when trim(percentNecrosisPostNeoadjuvant) > '' then trim(percentNecrosisPostNeoadjuvant) end as percentNecrosisPostNeoadjuvant
  , case when trim(perineuralInvasion) > '' then trim(perineuralInvasion) end as perineuralInvasion
  , case when trim(peripheralBloodInvolvement) > '' then trim(peripheralBloodInvolvement) end as peripheralBloodInvolvement
  , case when trim(peritonealCytology) > '' then trim(peritonealCytology) end as peritonealCytology
  , case when trim(pleuralEffusion) > '' then trim(pleuralEffusion) end as pleuralEffusion
  , case when trim(progesteroneRecepPrcntPosOrRange) > '' then trim(progesteroneRecepPrcntPosOrRange) end as progesteroneRecepPrcntPosOrRange
  , case when trim(progesteroneRecepSummary) > '' then trim(progesteroneRecepSummary) end as progesteroneRecepSummary
  , case when trim(progesteroneRecepTotalAllredScor) > '' then trim(progesteroneRecepTotalAllredScor) end as progesteroneRecepTotalAllredScor
  , case when trim(primarySclerosingCholangitis) > '' then trim(primarySclerosingCholangitis) end as primarySclerosingCholangitis
  , case when trim(profoundImmuneSuppression) > '' then trim(profoundImmuneSuppression) end as profoundImmuneSuppression
  , case when trim(prostatePathologicalExtension) > '' then trim(prostatePathologicalExtension) end as prostatePathologicalExtension
  , case when trim(psaLabValue) > '' then trim(psaLabValue) end as psaLabValue
  , case when trim(residualTumVolPostCytoreduction) > '' then trim(residualTumVolPostCytoreduction) end as residualTumVolPostCytoreduction
  , case when trim(responseToNeoadjuvantTherapy) > '' then trim(responseToNeoadjuvantTherapy) end as responseToNeoadjuvantTherapy
  , case when trim(sCategoryClinical) > '' then trim(sCategoryClinical) end as sCategoryClinical
  , case when trim(sCategoryPathological) > '' then trim(sCategoryPathological) end as sCategoryPathological
  , case when trim(sarcomatoidFeatures) > '' then trim(sarcomatoidFeatures) end as sarcomatoidFeatures
  , case when trim(schemaDiscriminator1) > '' then trim(schemaDiscriminator1) end as schemaDiscriminator1
  , case when trim(schemaDiscriminator2) > '' then trim(schemaDiscriminator2) end as schemaDiscriminator2
  , case when trim(schemaDiscriminator3) > '' then trim(schemaDiscriminator3) end as schemaDiscriminator3
  , case when trim(separateTumorNodules) > '' then trim(separateTumorNodules) end as separateTumorNodules
  , case when trim(serumAlbuminPretreatmentLevel) > '' then trim(serumAlbuminPretreatmentLevel) end as serumAlbuminPretreatmentLevel
  , case when trim(serumBeta2MicroglobulinPretxLvl) > '' then trim(serumBeta2MicroglobulinPretxLvl) end as serumBeta2MicroglobulinPretxLvl
  , case when trim(ldhPretreatmentLabValue) > '' then 0 + ldhPretreatmentLabValue end as ldhPretreatmentLabValue
  , case when trim(thrombocytopenia) > '' then trim(thrombocytopenia) end as thrombocytopenia
  , case when trim(tumorDeposits) > '' then trim(tumorDeposits) end as tumorDeposits
  , case when trim(tumorGrowthPattern) > '' then trim(tumorGrowthPattern) end as tumorGrowthPattern
  , case when trim(ulceration) > '' then trim(ulceration) end as ulceration
  , case when trim(visceralParietalPleuralInvasion) > '' then trim(visceralParietalPleuralInvasion) end as visceralParietalPleuralInvasion
  , case when trim(rxDateSurgery) > '' then date(substr(rxDateSurgery, 1, 4) || '-' || coalesce(substr(rxDateSurgery, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateSurgery, 7, 2), '01')) end as rxDateSurgery 
  , case when trim(rxDateSurgeryFlag) > '' then trim(rxDateSurgeryFlag) end as rxDateSurgeryFlag
  , case when trim(rxDateRadiation) > '' then date(substr(rxDateRadiation, 1, 4) || '-' || coalesce(substr(rxDateRadiation, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateRadiation, 7, 2), '01')) end as rxDateRadiation 
  , case when trim(rxDateRadiationFlag) > '' then trim(rxDateRadiationFlag) end as rxDateRadiationFlag
  , case when trim(rxDateChemo) > '' then date(substr(rxDateChemo, 1, 4) || '-' || coalesce(substr(rxDateChemo, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateChemo, 7, 2), '01')) end as rxDateChemo 
  , case when trim(rxDateChemoFlag) > '' then trim(rxDateChemoFlag) end as rxDateChemoFlag
  , case when trim(rxDateHormone) > '' then date(substr(rxDateHormone, 1, 4) || '-' || coalesce(substr(rxDateHormone, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateHormone, 7, 2), '01')) end as rxDateHormone 
  , case when trim(rxDateHormoneFlag) > '' then trim(rxDateHormoneFlag) end as rxDateHormoneFlag
  , case when trim(rxDateBrm) > '' then date(substr(rxDateBrm, 1, 4) || '-' || coalesce(substr(rxDateBrm, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateBrm, 7, 2), '01')) end as rxDateBrm 
  , case when trim(rxDateBrmFlag) > '' then trim(rxDateBrmFlag) end as rxDateBrmFlag
  , case when trim(rxDateOther) > '' then date(substr(rxDateOther, 1, 4) || '-' || coalesce(substr(rxDateOther, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateOther, 7, 2), '01')) end as rxDateOther 
  , case when trim(rxDateOtherFlag) > '' then trim(rxDateOtherFlag) end as rxDateOtherFlag
  , case when trim(dateInitialRxSeer) > '' then date(substr(dateInitialRxSeer, 1, 4) || '-' || coalesce(substr(dateInitialRxSeer, 5, 2), '01') || '-'
                     || coalesce(substr(dateInitialRxSeer, 7, 2), '01')) end as dateInitialRxSeer 
  , case when trim(dateInitialRxSeerFlag) > '' then trim(dateInitialRxSeerFlag) end as dateInitialRxSeerFlag
  , case when trim(date1stCrsRxCoc) > '' then date(substr(date1stCrsRxCoc, 1, 4) || '-' || coalesce(substr(date1stCrsRxCoc, 5, 2), '01') || '-'
                     || coalesce(substr(date1stCrsRxCoc, 7, 2), '01')) end as date1stCrsRxCoc 
  , case when trim(date1stCrsRxCocFlag) > '' then trim(date1stCrsRxCocFlag) end as date1stCrsRxCocFlag
  , case when trim(rxDateDxStgProc) > '' then date(substr(rxDateDxStgProc, 1, 4) || '-' || coalesce(substr(rxDateDxStgProc, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateDxStgProc, 7, 2), '01')) end as rxDateDxStgProc 
  , case when trim(rxDateDxStgProcFlag) > '' then trim(rxDateDxStgProcFlag) end as rxDateDxStgProcFlag
  , case when trim(rxSummTreatmentStatus) > '' then trim(rxSummTreatmentStatus) end as rxSummTreatmentStatus
  , case when trim(rxSummSurgPrimSite) > '' then trim(rxSummSurgPrimSite) end as rxSummSurgPrimSite
  , case when trim(rxSummScopeRegLnSur) > '' then trim(rxSummScopeRegLnSur) end as rxSummScopeRegLnSur
  , case when trim(rxSummSurgOthRegDis) > '' then trim(rxSummSurgOthRegDis) end as rxSummSurgOthRegDis
  , case when trim(rxSummRegLnExamined) > '' then 0 + rxSummRegLnExamined end as rxSummRegLnExamined
  , case when trim(rxSummSurgicalApproch) > '' then trim(rxSummSurgicalApproch) end as rxSummSurgicalApproch
  , case when trim(rxSummSurgicalMargins) > '' then trim(rxSummSurgicalMargins) end as rxSummSurgicalMargins
  , case when trim(rxSummReconstruct1st) > '' then trim(rxSummReconstruct1st) end as rxSummReconstruct1st
  , case when trim(reasonForNoSurgery) > '' then trim(reasonForNoSurgery) end as reasonForNoSurgery
  , case when trim(rxSummDxStgProc) > '' then trim(rxSummDxStgProc) end as rxSummDxStgProc
  , case when trim(rxSummRadiation) > '' then trim(rxSummRadiation) end as rxSummRadiation
  , case when trim(rxSummRadToCns) > '' then trim(rxSummRadToCns) end as rxSummRadToCns
  , case when trim(rxSummSurgRadSeq) > '' then trim(rxSummSurgRadSeq) end as rxSummSurgRadSeq
  , case when trim(rxSummChemo) > '' then trim(rxSummChemo) end as rxSummChemo
  , case when trim(rxSummHormone) > '' then trim(rxSummHormone) end as rxSummHormone
  , case when trim(rxSummBrm) > '' then trim(rxSummBrm) end as rxSummBrm
  , case when trim(rxSummOther) > '' then trim(rxSummOther) end as rxSummOther
  , case when trim(reasonForNoRadiation) > '' then trim(reasonForNoRadiation) end as reasonForNoRadiation
  , case when trim(rxCodingSystemCurrent) > '' then trim(rxCodingSystemCurrent) end as rxCodingSystemCurrent
  , case when trim(phase1DosePerFraction) > '' then 0 + phase1DosePerFraction end as phase1DosePerFraction
  , case when trim(phase1RadiationExternalBeamTech) > '' then trim(phase1RadiationExternalBeamTech) end as phase1RadiationExternalBeamTech
  , case when trim(phase1NumberOfFractions) > '' then 0 + phase1NumberOfFractions end as phase1NumberOfFractions
  , case when trim(phase1RadiationPrimaryTxVolume) > '' then trim(phase1RadiationPrimaryTxVolume) end as phase1RadiationPrimaryTxVolume
  , case when trim(phase1RadiationToDrainingLN) > '' then trim(phase1RadiationToDrainingLN) end as phase1RadiationToDrainingLN
  , case when trim(phase1RadiationTreatmentModality) > '' then trim(phase1RadiationTreatmentModality) end as phase1RadiationTreatmentModality
  , case when trim(phase1TotalDose) > '' then 0 + phase1TotalDose end as phase1TotalDose
  , case when trim(radRegionalDoseCgy) > '' then 0 + radRegionalDoseCgy end as radRegionalDoseCgy
  , case when trim(phase2DosePerFraction) > '' then 0 + phase2DosePerFraction end as phase2DosePerFraction
  , case when trim(phase2RadiationExternalBeamTech) > '' then trim(phase2RadiationExternalBeamTech) end as phase2RadiationExternalBeamTech
  , case when trim(phase2NumberOfFractions) > '' then 0 + phase2NumberOfFractions end as phase2NumberOfFractions
  , case when trim(phase2RadiationPrimaryTxVolume) > '' then trim(phase2RadiationPrimaryTxVolume) end as phase2RadiationPrimaryTxVolume
  , case when trim(phase2RadiationToDrainingLN) > '' then trim(phase2RadiationToDrainingLN) end as phase2RadiationToDrainingLN
  , case when trim(phase2RadiationTreatmentModality) > '' then trim(phase2RadiationTreatmentModality) end as phase2RadiationTreatmentModality
  , case when trim(phase2TotalDose) > '' then 0 + phase2TotalDose end as phase2TotalDose
  , case when trim(radNoOfTreatmentVol) > '' then 0 + radNoOfTreatmentVol end as radNoOfTreatmentVol
  , case when trim(phase3DosePerFraction) > '' then 0 + phase3DosePerFraction end as phase3DosePerFraction
  , case when trim(phase3RadiationExternalBeamTech) > '' then trim(phase3RadiationExternalBeamTech) end as phase3RadiationExternalBeamTech
  , case when trim(phase3NumberOfFractions) > '' then 0 + phase3NumberOfFractions end as phase3NumberOfFractions
  , case when trim(phase3RadiationPrimaryTxVolume) > '' then trim(phase3RadiationPrimaryTxVolume) end as phase3RadiationPrimaryTxVolume
  , case when trim(phase3RadiationToDrainingLN) > '' then trim(phase3RadiationToDrainingLN) end as phase3RadiationToDrainingLN
  , case when trim(phase3RadiationTreatmentModality) > '' then trim(phase3RadiationTreatmentModality) end as phase3RadiationTreatmentModality
  , case when trim(phase3TotalDose) > '' then 0 + phase3TotalDose end as phase3TotalDose
  , case when trim(radiationTxDiscontinuedEarly) > '' then trim(radiationTxDiscontinuedEarly) end as radiationTxDiscontinuedEarly
  , case when trim(numberPhasesOfRadTxToVolume) > '' then 0 + numberPhasesOfRadTxToVolume end as numberPhasesOfRadTxToVolume
  , case when trim(totalDose) > '' then 0 + totalDose end as totalDose
  , case when trim(radTreatmentVolume) > '' then trim(radTreatmentVolume) end as radTreatmentVolume
  , case when trim(radLocationOfRx) > '' then trim(radLocationOfRx) end as radLocationOfRx
  , case when trim(radRegionalRxModality) > '' then trim(radRegionalRxModality) end as radRegionalRxModality
  , case when trim(rxSummSystemicSurSeq) > '' then trim(rxSummSystemicSurSeq) end as rxSummSystemicSurSeq
  , case when trim(rxSummSurgeryType) > '' then trim(rxSummSurgeryType) end as rxSummSurgeryType
  , case when trim(rxSummSurgSite9802) > '' then trim(rxSummSurgSite9802) end as rxSummSurgSite9802
  , case when trim(rxSummScopeReg9802) > '' then trim(rxSummScopeReg9802) end as rxSummScopeReg9802
  , case when trim(rxSummSurgOth9802) > '' then trim(rxSummSurgOth9802) end as rxSummSurgOth9802
  , case when trim(rxDateMostDefinSurg) > '' then date(substr(rxDateMostDefinSurg, 1, 4) || '-' || coalesce(substr(rxDateMostDefinSurg, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateMostDefinSurg, 7, 2), '01')) end as rxDateMostDefinSurg 
  , case when trim(rxDateMostDefinSurgFlag) > '' then trim(rxDateMostDefinSurgFlag) end as rxDateMostDefinSurgFlag
  , case when trim(rxDateSurgicalDisch) > '' then date(substr(rxDateSurgicalDisch, 1, 4) || '-' || coalesce(substr(rxDateSurgicalDisch, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateSurgicalDisch, 7, 2), '01')) end as rxDateSurgicalDisch 
  , case when trim(rxDateSurgicalDischFlag) > '' then trim(rxDateSurgicalDischFlag) end as rxDateSurgicalDischFlag
  , case when trim(readmSameHosp30Days) > '' then trim(readmSameHosp30Days) end as readmSameHosp30Days
  , case when trim(radBoostRxModality) > '' then trim(radBoostRxModality) end as radBoostRxModality
  , case when trim(radBoostDoseCgy) > '' then 0 + radBoostDoseCgy end as radBoostDoseCgy
  , case when trim(rxDateRadiationEnded) > '' then date(substr(rxDateRadiationEnded, 1, 4) || '-' || coalesce(substr(rxDateRadiationEnded, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateRadiationEnded, 7, 2), '01')) end as rxDateRadiationEnded 
  , case when trim(rxDateRadiationEndedFlag) > '' then trim(rxDateRadiationEndedFlag) end as rxDateRadiationEndedFlag
  , case when trim(rxDateSystemic) > '' then date(substr(rxDateSystemic, 1, 4) || '-' || coalesce(substr(rxDateSystemic, 5, 2), '01') || '-'
                     || coalesce(substr(rxDateSystemic, 7, 2), '01')) end as rxDateSystemic 
  , case when trim(rxDateSystemicFlag) > '' then trim(rxDateSystemicFlag) end as rxDateSystemicFlag
  , case when trim(rxSummTransplntEndocr) > '' then trim(rxSummTransplntEndocr) end as rxSummTransplntEndocr
  , case when trim(rxSummPalliativeProc) > '' then trim(rxSummPalliativeProc) end as rxSummPalliativeProc
  , case when trim(subsqRx2ndCourseDate) > '' then date(substr(subsqRx2ndCourseDate, 1, 4) || '-' || coalesce(substr(subsqRx2ndCourseDate, 5, 2), '01') || '-'
                     || coalesce(substr(subsqRx2ndCourseDate, 7, 2), '01')) end as subsqRx2ndCourseDate 
  , case when trim(subsqRx2ndcrsDateFlag) > '' then trim(subsqRx2ndcrsDateFlag) end as subsqRx2ndcrsDateFlag
  , case when trim(subsqRx2ndCourseSurg) > '' then trim(subsqRx2ndCourseSurg) end as subsqRx2ndCourseSurg
  , case when trim(subsqRx2ndCourseRad) > '' then trim(subsqRx2ndCourseRad) end as subsqRx2ndCourseRad
  , case when trim(subsqRx2ndCourseChemo) > '' then trim(subsqRx2ndCourseChemo) end as subsqRx2ndCourseChemo
  , case when trim(subsqRx2ndCourseHorm) > '' then trim(subsqRx2ndCourseHorm) end as subsqRx2ndCourseHorm
  , case when trim(subsqRx2ndCourseBrm) > '' then trim(subsqRx2ndCourseBrm) end as subsqRx2ndCourseBrm
  , case when trim(subsqRx2ndCourseOth) > '' then trim(subsqRx2ndCourseOth) end as subsqRx2ndCourseOth
  , case when trim(subsqRx2ndScopeLnSu) > '' then trim(subsqRx2ndScopeLnSu) end as subsqRx2ndScopeLnSu
  , case when trim(subsqRx2ndSurgOth) > '' then trim(subsqRx2ndSurgOth) end as subsqRx2ndSurgOth
  , case when trim(subsqRx2ndRegLnRem) > '' then trim(subsqRx2ndRegLnRem) end as subsqRx2ndRegLnRem
  , case when trim(subsqRx3rdCourseDate) > '' then date(substr(subsqRx3rdCourseDate, 1, 4) || '-' || coalesce(substr(subsqRx3rdCourseDate, 5, 2), '01') || '-'
                     || coalesce(substr(subsqRx3rdCourseDate, 7, 2), '01')) end as subsqRx3rdCourseDate 
  , case when trim(subsqRx3rdcrsDateFlag) > '' then trim(subsqRx3rdcrsDateFlag) end as subsqRx3rdcrsDateFlag
  , case when trim(subsqRx3rdCourseSurg) > '' then trim(subsqRx3rdCourseSurg) end as subsqRx3rdCourseSurg
  , case when trim(subsqRx3rdCourseRad) > '' then trim(subsqRx3rdCourseRad) end as subsqRx3rdCourseRad
  , case when trim(subsqRx3rdCourseChemo) > '' then trim(subsqRx3rdCourseChemo) end as subsqRx3rdCourseChemo
  , case when trim(subsqRx3rdCourseHorm) > '' then trim(subsqRx3rdCourseHorm) end as subsqRx3rdCourseHorm
  , case when trim(subsqRx3rdCourseBrm) > '' then trim(subsqRx3rdCourseBrm) end as subsqRx3rdCourseBrm
  , case when trim(subsqRx3rdCourseOth) > '' then trim(subsqRx3rdCourseOth) end as subsqRx3rdCourseOth
  , case when trim(subsqRx3rdScopeLnSu) > '' then trim(subsqRx3rdScopeLnSu) end as subsqRx3rdScopeLnSu
  , case when trim(subsqRx3rdSurgOth) > '' then trim(subsqRx3rdSurgOth) end as subsqRx3rdSurgOth
  , case when trim(subsqRx3rdRegLnRem) > '' then trim(subsqRx3rdRegLnRem) end as subsqRx3rdRegLnRem
  , case when trim(subsqRx4thCourseDate) > '' then date(substr(subsqRx4thCourseDate, 1, 4) || '-' || coalesce(substr(subsqRx4thCourseDate, 5, 2), '01') || '-'
                     || coalesce(substr(subsqRx4thCourseDate, 7, 2), '01')) end as subsqRx4thCourseDate 
  , case when trim(subsqRx4thcrsDateFlag) > '' then trim(subsqRx4thcrsDateFlag) end as subsqRx4thcrsDateFlag
  , case when trim(subsqRx4thCourseSurg) > '' then trim(subsqRx4thCourseSurg) end as subsqRx4thCourseSurg
  , case when trim(subsqRx4thCourseRad) > '' then trim(subsqRx4thCourseRad) end as subsqRx4thCourseRad
  , case when trim(subsqRx4thCourseChemo) > '' then trim(subsqRx4thCourseChemo) end as subsqRx4thCourseChemo
  , case when trim(subsqRx4thCourseHorm) > '' then trim(subsqRx4thCourseHorm) end as subsqRx4thCourseHorm
  , case when trim(subsqRx4thCourseBrm) > '' then trim(subsqRx4thCourseBrm) end as subsqRx4thCourseBrm
  , case when trim(subsqRx4thCourseOth) > '' then trim(subsqRx4thCourseOth) end as subsqRx4thCourseOth
  , case when trim(subsqRx4thScopeLnSu) > '' then trim(subsqRx4thScopeLnSu) end as subsqRx4thScopeLnSu
  , case when trim(subsqRx4thSurgOth) > '' then trim(subsqRx4thSurgOth) end as subsqRx4thSurgOth
  , case when trim(subsqRx4thRegLnRem) > '' then trim(subsqRx4thRegLnRem) end as subsqRx4thRegLnRem
  , case when trim(subsqRxReconstructDel) > '' then trim(subsqRxReconstructDel) end as subsqRxReconstructDel
  , case when trim(pathDateSpecCollect1) > '' then date(substr(pathDateSpecCollect1, 1, 4) || '-' || coalesce(substr(pathDateSpecCollect1, 5, 2), '01') || '-'
                     || coalesce(substr(pathDateSpecCollect1, 7, 2), '01')) end as pathDateSpecCollect1 
  , case when trim(pathDateSpecCollect2) > '' then date(substr(pathDateSpecCollect2, 1, 4) || '-' || coalesce(substr(pathDateSpecCollect2, 5, 2), '01') || '-'
                     || coalesce(substr(pathDateSpecCollect2, 7, 2), '01')) end as pathDateSpecCollect2 
  , case when trim(pathDateSpecCollect3) > '' then date(substr(pathDateSpecCollect3, 1, 4) || '-' || coalesce(substr(pathDateSpecCollect3, 5, 2), '01') || '-'
                     || coalesce(substr(pathDateSpecCollect3, 7, 2), '01')) end as pathDateSpecCollect3 
  , case when trim(pathDateSpecCollect4) > '' then date(substr(pathDateSpecCollect4, 1, 4) || '-' || coalesce(substr(pathDateSpecCollect4, 5, 2), '01') || '-'
                     || coalesce(substr(pathDateSpecCollect4, 7, 2), '01')) end as pathDateSpecCollect4 
  , case when trim(pathDateSpecCollect5) > '' then date(substr(pathDateSpecCollect5, 1, 4) || '-' || coalesce(substr(pathDateSpecCollect5, 5, 2), '01') || '-'
                     || coalesce(substr(pathDateSpecCollect5, 7, 2), '01')) end as pathDateSpecCollect5 
  , case when trim(pathReportType1) > '' then trim(pathReportType1) end as pathReportType1
  , case when trim(pathReportType2) > '' then trim(pathReportType2) end as pathReportType2
  , case when trim(pathReportType3) > '' then trim(pathReportType3) end as pathReportType3
  , case when trim(pathReportType4) > '' then trim(pathReportType4) end as pathReportType4
  , case when trim(pathReportType5) > '' then trim(pathReportType5) end as pathReportType5
        from naaccr_fields_raw;


drop view if exists naaccr_tumor;
create view naaccr_tumor as
select tumor_id
     , patientIdNumber -- pat_ids    
     , dateOfBirth, dateOfLastContact, sex, vitalStatus -- pat_attrs
--    tmr_ids = ['tumorRecordNumber']
     , dateOfDiagnosis  -- tmr_attrs
     , sequenceNumberCentral, sequenceNumberHospital, primarySite
     -- PHI? , ageAtDiagnosis
     , dateOfInptAdm, dateOfInptDisch, classOfCase
     , dateCaseInitiated, dateCaseCompleted, dateCaseLastChanged
     , naaccrRecordVersion, npiRegistryId -- report_ids
     , dateCaseReportExported  -- report_attrs
from naaccr_fields;

drop view if exists naaccr_patient;
create view naaccr_patient as
select distinct patientIdNumber, dateOfBirth, dateOfLastContact, sex, vitalStatus
     , naaccrRecordVersion, npiRegistryId -- report_ids
     , dateCaseReportExported 
from naaccr_tumor;

/* dups?
select count(*), patientIdNumber
from naaccr_patient
group by patientIdNumber
having count(*) > 1;
*/


select * from section_1 order by tumor_id, naaccrNum;

-- 1: Cancer Identification
drop table if exists section_1;
create table section_1 as
-- 1: Cancer Identification

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sequenceNumberCentral' as naaccrId, 380 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sequenceNumberCentral as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sequenceNumberCentral is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfDiagnosis' as naaccrId, 390 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfDiagnosis as date_value , null as text_value , tumor_id from naaccr_fields where dateOfDiagnosis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfDiagnosisFlag' as naaccrId, 391 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfDiagnosisFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfDiagnosisFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'primarySite' as naaccrId, 400 as naaccrNum , 0 as identified_only , '@' as valtype_cd , primarySite as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where primarySite is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'laterality' as naaccrId, 410 as naaccrNum , 0 as identified_only , '@' as valtype_cd , laterality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where laterality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'histologyIcdO2' as naaccrId, 420 as naaccrNum , 0 as identified_only , '@' as valtype_cd , histologyIcdO2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where histologyIcdO2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'behaviorIcdO2' as naaccrId, 430 as naaccrNum , 0 as identified_only , '@' as valtype_cd , behaviorIcdO2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where behaviorIcdO2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfMultTumorsFlag' as naaccrId, 439 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfMultTumorsFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfMultTumorsFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'grade' as naaccrId, 440 as naaccrNum , 0 as identified_only , '@' as valtype_cd , grade as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where grade is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradePathValue' as naaccrId, 441 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradePathValue as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradePathValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ambiguousTerminologyDx' as naaccrId, 442 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ambiguousTerminologyDx as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ambiguousTerminologyDx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateConclusiveDx' as naaccrId, 443 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateConclusiveDx as date_value , null as text_value , tumor_id from naaccr_fields where dateConclusiveDx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'multTumRptAsOnePrim' as naaccrId, 444 as naaccrNum , 0 as identified_only , '@' as valtype_cd , multTumRptAsOnePrim as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where multTumRptAsOnePrim is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfMultTumors' as naaccrId, 445 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfMultTumors as date_value , null as text_value , tumor_id from naaccr_fields where dateOfMultTumors is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'multiplicityCounter' as naaccrId, 446 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , multiplicityCounter as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where multiplicityCounter is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateConclusiveDxFlag' as naaccrId, 448 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateConclusiveDxFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateConclusiveDxFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradePathSystem' as naaccrId, 449 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradePathSystem as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradePathSystem is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'siteCodingSysCurrent' as naaccrId, 450 as naaccrNum , 0 as identified_only , '@' as valtype_cd , siteCodingSysCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where siteCodingSysCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'siteCodingSysOriginal' as naaccrId, 460 as naaccrNum , 0 as identified_only , '@' as valtype_cd , siteCodingSysOriginal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where siteCodingSysOriginal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'morphCodingSysCurrent' as naaccrId, 470 as naaccrNum , 0 as identified_only , '@' as valtype_cd , morphCodingSysCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where morphCodingSysCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'morphCodingSysOriginl' as naaccrId, 480 as naaccrNum , 0 as identified_only , '@' as valtype_cd , morphCodingSysOriginl as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where morphCodingSysOriginl is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'diagnosticConfirmation' as naaccrId, 490 as naaccrNum , 0 as identified_only , '@' as valtype_cd , diagnosticConfirmation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where diagnosticConfirmation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'typeOfReportingSource' as naaccrId, 500 as naaccrNum , 0 as identified_only , '@' as valtype_cd , typeOfReportingSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where typeOfReportingSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'casefindingSource' as naaccrId, 501 as naaccrNum , 0 as identified_only , '@' as valtype_cd , casefindingSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where casefindingSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'histologicTypeIcdO3' as naaccrId, 522 as naaccrNum , 0 as identified_only , '@' as valtype_cd , histologicTypeIcdO3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where histologicTypeIcdO3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'behaviorCodeIcdO3' as naaccrId, 523 as naaccrNum , 0 as identified_only , '@' as valtype_cd , behaviorCodeIcdO3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where behaviorCodeIcdO3 is not null;

-- 2: Demographic
drop table if exists section_2;
create table section_2 as
-- 2: Demographic

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'addrAtDxState' as naaccrId, 80 as naaccrNum , 0 as identified_only , '@' as valtype_cd , addrAtDxState as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where addrAtDxState is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'stateAtDxGeocode19708090' as naaccrId, 81 as naaccrNum , 0 as identified_only , '@' as valtype_cd , stateAtDxGeocode19708090 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where stateAtDxGeocode19708090 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'stateAtDxGeocode2000' as naaccrId, 82 as naaccrNum , 0 as identified_only , '@' as valtype_cd , stateAtDxGeocode2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where stateAtDxGeocode2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'stateAtDxGeocode2010' as naaccrId, 83 as naaccrNum , 0 as identified_only , '@' as valtype_cd , stateAtDxGeocode2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where stateAtDxGeocode2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'stateAtDxGeocode2020' as naaccrId, 84 as naaccrNum , 0 as identified_only , '@' as valtype_cd , stateAtDxGeocode2020 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where stateAtDxGeocode2020 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'addrAtDxCountry' as naaccrId, 102 as naaccrNum , 0 as identified_only , '@' as valtype_cd , addrAtDxCountry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where addrAtDxCountry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusCodSys19708090' as naaccrId, 120 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusCodSys19708090 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusCodSys19708090 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusTrPovertyIndictr' as naaccrId, 145 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusTrPovertyIndictr as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusTrPovertyIndictr is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'maritalStatusAtDx' as naaccrId, 150 as naaccrNum , 0 as identified_only , '@' as valtype_cd , maritalStatusAtDx as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where maritalStatusAtDx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'race1' as naaccrId, 160 as naaccrNum , 0 as identified_only , '@' as valtype_cd , race1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where race1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'race2' as naaccrId, 161 as naaccrNum , 0 as identified_only , '@' as valtype_cd , race2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where race2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'race3' as naaccrId, 162 as naaccrNum , 0 as identified_only , '@' as valtype_cd , race3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where race3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'race4' as naaccrId, 163 as naaccrNum , 0 as identified_only , '@' as valtype_cd , race4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where race4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'race5' as naaccrId, 164 as naaccrNum , 0 as identified_only , '@' as valtype_cd , race5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where race5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'raceCodingSysCurrent' as naaccrId, 170 as naaccrNum , 0 as identified_only , '@' as valtype_cd , raceCodingSysCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where raceCodingSysCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'raceCodingSysOriginal' as naaccrId, 180 as naaccrNum , 0 as identified_only , '@' as valtype_cd , raceCodingSysOriginal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where raceCodingSysOriginal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'spanishHispanicOrigin' as naaccrId, 190 as naaccrNum , 0 as identified_only , '@' as valtype_cd , spanishHispanicOrigin as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where spanishHispanicOrigin is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'nhiaDerivedHispOrigin' as naaccrId, 191 as naaccrNum , 0 as identified_only , '@' as valtype_cd , nhiaDerivedHispOrigin as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where nhiaDerivedHispOrigin is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ihsLink' as naaccrId, 192 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ihsLink as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ihsLink is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'raceNapiia' as naaccrId, 193 as naaccrNum , 0 as identified_only , '@' as valtype_cd , raceNapiia as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where raceNapiia is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'computedEthnicity' as naaccrId, 200 as naaccrNum , 0 as identified_only , '@' as valtype_cd , computedEthnicity as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where computedEthnicity is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'computedEthnicitySource' as naaccrId, 210 as naaccrNum , 0 as identified_only , '@' as valtype_cd , computedEthnicitySource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where computedEthnicitySource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sex' as naaccrId, 220 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sex as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sex is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfBirth' as naaccrId, 240 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfBirth as date_value , null as text_value , tumor_id from naaccr_fields where dateOfBirth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfBirthFlag' as naaccrId, 241 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfBirthFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfBirthFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'birthplace' as naaccrId, 250 as naaccrNum , 0 as identified_only , '@' as valtype_cd , birthplace as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where birthplace is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'birthplaceState' as naaccrId, 252 as naaccrNum , 0 as identified_only , '@' as valtype_cd , birthplaceState as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where birthplaceState is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'birthplaceCountry' as naaccrId, 254 as naaccrNum , 0 as identified_only , '@' as valtype_cd , birthplaceCountry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where birthplaceCountry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusOccCode19702000' as naaccrId, 270 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusOccCode19702000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusOccCode19702000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusIndCode2010' as naaccrId, 272 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusIndCode2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusIndCode2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusIndCode19702000' as naaccrId, 280 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusIndCode19702000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusIndCode19702000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusOccCode2010' as naaccrId, 282 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusOccCode2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusOccCode2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'occupationSource' as naaccrId, 290 as naaccrNum , 0 as identified_only , '@' as valtype_cd , occupationSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where occupationSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'industrySource' as naaccrId, 300 as naaccrNum , 0 as identified_only , '@' as valtype_cd , industrySource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where industrySource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusOccIndSys7000' as naaccrId, 330 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusOccIndSys7000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusOccIndSys7000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ruca2000' as naaccrId, 339 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ruca2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ruca2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ruca2010' as naaccrId, 341 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ruca2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ruca2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'uric2000' as naaccrId, 345 as naaccrNum , 0 as identified_only , '@' as valtype_cd , uric2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where uric2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'uric2010' as naaccrId, 346 as naaccrNum , 0 as identified_only , '@' as valtype_cd , uric2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where uric2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusTrCert19708090' as naaccrId, 364 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusTrCert19708090 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusTrCert19708090 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusTrCertainty2000' as naaccrId, 365 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusTrCertainty2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusTrCertainty2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gisCoordinateQuality' as naaccrId, 366 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gisCoordinateQuality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gisCoordinateQuality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusTrCertainty2010' as naaccrId, 367 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusTrCertainty2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusTrCertainty2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'censusTractCertainty2020' as naaccrId, 369 as naaccrNum , 0 as identified_only , '@' as valtype_cd , censusTractCertainty2020 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where censusTractCertainty2020 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'addrCurrentCountry' as naaccrId, 1832 as naaccrNum , 0 as identified_only , '@' as valtype_cd , addrCurrentCountry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where addrCurrentCountry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'followupContactCountry' as naaccrId, 1847 as naaccrNum , 0 as identified_only , '@' as valtype_cd , followupContactCountry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where followupContactCountry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'placeOfDeathState' as naaccrId, 1942 as naaccrNum , 0 as identified_only , '@' as valtype_cd , placeOfDeathState as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where placeOfDeathState is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'placeOfDeathCountry' as naaccrId, 1944 as naaccrNum , 0 as identified_only , '@' as valtype_cd , placeOfDeathCountry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where placeOfDeathCountry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ruralurbanContinuum1993' as naaccrId, 3300 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ruralurbanContinuum1993 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ruralurbanContinuum1993 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ruralurbanContinuum2003' as naaccrId, 3310 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ruralurbanContinuum2003 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ruralurbanContinuum2003 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ruralurbanContinuum2013' as naaccrId, 3312 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ruralurbanContinuum2013 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ruralurbanContinuum2013 is not null;

-- 3: Edit Overrides/Conversion History/System Admin
drop table if exists section_3;
create table section_3 as
-- 3: Edit Overrides/Conversion History/System Admin

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'siteIcdO1' as naaccrId, 1960 as naaccrNum , 0 as identified_only , '@' as valtype_cd , siteIcdO1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where siteIcdO1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'histologyIcdO1' as naaccrId, 1971 as naaccrNum , 0 as identified_only , '@' as valtype_cd , histologyIcdO1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where histologyIcdO1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'behaviorIcdO1' as naaccrId, 1972 as naaccrNum , 0 as identified_only , '@' as valtype_cd , behaviorIcdO1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where behaviorIcdO1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradeIcdO1' as naaccrId, 1973 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradeIcdO1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradeIcdO1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'icdO2ConversionFlag' as naaccrId, 1980 as naaccrNum , 0 as identified_only , '@' as valtype_cd , icdO2ConversionFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where icdO2ConversionFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSsNodespos' as naaccrId, 1981 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSsNodespos as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSsNodespos is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSsTnmN' as naaccrId, 1982 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSsTnmN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSsTnmN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSsTnmM' as naaccrId, 1983 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSsTnmM as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSsTnmM is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideAcsnClassSeq' as naaccrId, 1985 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideAcsnClassSeq as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideAcsnClassSeq is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideHospseqDxconf' as naaccrId, 1986 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideHospseqDxconf as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideHospseqDxconf is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCocSiteType' as naaccrId, 1987 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCocSiteType as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCocSiteType is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideHospseqSite' as naaccrId, 1988 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideHospseqSite as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideHospseqSite is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteTnmStggrp' as naaccrId, 1989 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteTnmStggrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteTnmStggrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideAgeSiteMorph' as naaccrId, 1990 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideAgeSiteMorph as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideAgeSiteMorph is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideTnmStage' as naaccrId, 1992 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideTnmStage as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideTnmStage is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideTnmTis' as naaccrId, 1993 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideTnmTis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideTnmTis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideTnm3' as naaccrId, 1994 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideTnm3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideTnm3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSeqnoDxconf' as naaccrId, 2000 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSeqnoDxconf as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSeqnoDxconf is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteLatSeqno' as naaccrId, 2010 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteLatSeqno as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteLatSeqno is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSurgDxconf' as naaccrId, 2020 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSurgDxconf as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSurgDxconf is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteType' as naaccrId, 2030 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteType as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteType is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideHistology' as naaccrId, 2040 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideHistology as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideHistology is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideReportSource' as naaccrId, 2050 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideReportSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideReportSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideIllDefineSite' as naaccrId, 2060 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideIllDefineSite as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideIllDefineSite is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideLeukLymphoma' as naaccrId, 2070 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideLeukLymphoma as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideLeukLymphoma is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteBehavior' as naaccrId, 2071 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteBehavior as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteBehavior is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteEodDxDt' as naaccrId, 2072 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteEodDxDt as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteEodDxDt is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteLatEod' as naaccrId, 2073 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteLatEod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteLatEod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideSiteLatMorph' as naaccrId, 2074 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideSiteLatMorph as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideSiteLatMorph is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideNameSex' as naaccrId, 2078 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideNameSex as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideNameSex is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseInitiated' as naaccrId, 2085 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseInitiated as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseInitiated is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseCompleted' as naaccrId, 2090 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseCompleted as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseCompleted is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseCompletedCoc' as naaccrId, 2092 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseCompletedCoc as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseCompletedCoc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseLastChanged' as naaccrId, 2100 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseLastChanged as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseLastChanged is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseReportExported' as naaccrId, 2110 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseReportExported as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseReportExported is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseReportReceived' as naaccrId, 2111 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseReportReceived as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseReportReceived is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateCaseReportLoaded' as naaccrId, 2112 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateCaseReportLoaded as date_value , null as text_value , tumor_id from naaccr_fields where dateCaseReportLoaded is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateTumorRecordAvailbl' as naaccrId, 2113 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateTumorRecordAvailbl as date_value , null as text_value , tumor_id from naaccr_fields where dateTumorRecordAvailbl is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'icdO3ConversionFlag' as naaccrId, 2116 as naaccrNum , 0 as identified_only , '@' as valtype_cd , icdO3ConversionFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where icdO3ConversionFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerCodingSysCurrent' as naaccrId, 2120 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerCodingSysCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerCodingSysCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerCodingSysOriginal' as naaccrId, 2130 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerCodingSysOriginal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerCodingSysOriginal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'cocCodingSysCurrent' as naaccrId, 2140 as naaccrNum , 0 as identified_only , '@' as valtype_cd , cocCodingSysCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where cocCodingSysCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'cocCodingSysOriginal' as naaccrId, 2150 as naaccrNum , 0 as identified_only , '@' as valtype_cd , cocCodingSysOriginal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where cocCodingSysOriginal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'cocAccreditedFlag' as naaccrId, 2152 as naaccrNum , 0 as identified_only , '@' as valtype_cd , cocAccreditedFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where cocAccreditedFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rqrsNcdbSubmissionFlag' as naaccrId, 2155 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rqrsNcdbSubmissionFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rqrsNcdbSubmissionFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'vendorName' as naaccrId, 2170 as naaccrNum , 0 as identified_only , 'T' as valtype_cd , null as code_value , null as numeric_value , null as date_value , vendorName as text_value , tumor_id from naaccr_fields where vendorName is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerTypeOfFollowUp' as naaccrId, 2180 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerTypeOfFollowUp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerTypeOfFollowUp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerRecordNumber' as naaccrId, 2190 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerRecordNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerRecordNumber is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs1' as naaccrId, 3750 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs2' as naaccrId, 3751 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs3' as naaccrId, 3752 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs4' as naaccrId, 3753 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs5' as naaccrId, 3754 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs6' as naaccrId, 3755 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs6 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs6 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs7' as naaccrId, 3756 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs7 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs7 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs8' as naaccrId, 3757 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs8 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs8 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs9' as naaccrId, 3758 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs9 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs9 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs10' as naaccrId, 3759 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs10 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs10 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs11' as naaccrId, 3760 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs11 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs11 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs12' as naaccrId, 3761 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs12 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs12 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs13' as naaccrId, 3762 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs13 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs13 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs14' as naaccrId, 3763 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs14 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs14 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs15' as naaccrId, 3764 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs15 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs15 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs16' as naaccrId, 3765 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs16 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs16 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs17' as naaccrId, 3766 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs17 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs17 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs18' as naaccrId, 3767 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs18 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs18 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs19' as naaccrId, 3768 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs19 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs19 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'overRideCs20' as naaccrId, 3769 as naaccrNum , 0 as identified_only , '@' as valtype_cd , overRideCs20 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where overRideCs20 is not null;

-- 4: Follow-up/Recurrence/Death
drop table if exists section_4;
create table section_4 as
-- 4: Follow-up/Recurrence/Death

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfLastContact' as naaccrId, 1750 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfLastContact as date_value , null as text_value , tumor_id from naaccr_fields where dateOfLastContact is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfLastContactFlag' as naaccrId, 1751 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfLastContactFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfLastContactFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfDeathCanada' as naaccrId, 1755 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfDeathCanada as date_value , null as text_value , tumor_id from naaccr_fields where dateOfDeathCanada is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfDeathCanadaFlag' as naaccrId, 1756 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfDeathCanadaFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfDeathCanadaFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'vitalStatus' as naaccrId, 1760 as naaccrNum , 0 as identified_only , '@' as valtype_cd , vitalStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where vitalStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'vitalStatusRecode' as naaccrId, 1762 as naaccrNum , 0 as identified_only , '@' as valtype_cd , vitalStatusRecode as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where vitalStatusRecode is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'cancerStatus' as naaccrId, 1770 as naaccrNum , 0 as identified_only , '@' as valtype_cd , cancerStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where cancerStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfLastCancerStatus' as naaccrId, 1772 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfLastCancerStatus as date_value , null as text_value , tumor_id from naaccr_fields where dateOfLastCancerStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfLastCancerStatusFlag' as naaccrId, 1773 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfLastCancerStatusFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfLastCancerStatusFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'recordNumberRecode' as naaccrId, 1775 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , recordNumberRecode as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where recordNumberRecode is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'qualityOfSurvival' as naaccrId, 1780 as naaccrNum , 0 as identified_only , '@' as valtype_cd , qualityOfSurvival as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where qualityOfSurvival is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survDateActiveFollowup' as naaccrId, 1782 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , survDateActiveFollowup as date_value , null as text_value , tumor_id from naaccr_fields where survDateActiveFollowup is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survFlagActiveFollowup' as naaccrId, 1783 as naaccrNum , 0 as identified_only , '@' as valtype_cd , survFlagActiveFollowup as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where survFlagActiveFollowup is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survMosActiveFollowup' as naaccrId, 1784 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , survMosActiveFollowup as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where survMosActiveFollowup is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survDatePresumedAlive' as naaccrId, 1785 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , survDatePresumedAlive as date_value , null as text_value , tumor_id from naaccr_fields where survDatePresumedAlive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survFlagPresumedAlive' as naaccrId, 1786 as naaccrNum , 0 as identified_only , '@' as valtype_cd , survFlagPresumedAlive as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where survFlagPresumedAlive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survMosPresumedAlive' as naaccrId, 1787 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , survMosPresumedAlive as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where survMosPresumedAlive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'survDateDxRecode' as naaccrId, 1788 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , survDateDxRecode as date_value , null as text_value , tumor_id from naaccr_fields where survDateDxRecode is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'followUpSource' as naaccrId, 1790 as naaccrNum , 0 as identified_only , '@' as valtype_cd , followUpSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where followUpSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'followUpSourceCentral' as naaccrId, 1791 as naaccrNum , 0 as identified_only , '@' as valtype_cd , followUpSourceCentral as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where followUpSourceCentral is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'nextFollowUpSource' as naaccrId, 1800 as naaccrNum , 0 as identified_only , '@' as valtype_cd , nextFollowUpSource as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where nextFollowUpSource is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'addrCurrentState' as naaccrId, 1820 as naaccrNum , 0 as identified_only , '@' as valtype_cd , addrCurrentState as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where addrCurrentState is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'followUpContactState' as naaccrId, 1844 as naaccrNum , 0 as identified_only , '@' as valtype_cd , followUpContactState as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where followUpContactState is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'recurrenceDate1st' as naaccrId, 1860 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , recurrenceDate1st as date_value , null as text_value , tumor_id from naaccr_fields where recurrenceDate1st is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'recurrenceDate1stFlag' as naaccrId, 1861 as naaccrNum , 0 as identified_only , '@' as valtype_cd , recurrenceDate1stFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where recurrenceDate1stFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'recurrenceType1st' as naaccrId, 1880 as naaccrNum , 0 as identified_only , '@' as valtype_cd , recurrenceType1st as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where recurrenceType1st is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'causeOfDeath' as naaccrId, 1910 as naaccrNum , 0 as identified_only , '@' as valtype_cd , causeOfDeath as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where causeOfDeath is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerCauseSpecificCod' as naaccrId, 1914 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerCauseSpecificCod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerCauseSpecificCod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerOtherCod' as naaccrId, 1915 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerOtherCod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerOtherCod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'icdRevisionNumber' as naaccrId, 1920 as naaccrNum , 0 as identified_only , '@' as valtype_cd , icdRevisionNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where icdRevisionNumber is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'autopsy' as naaccrId, 1930 as naaccrNum , 0 as identified_only , '@' as valtype_cd , autopsy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where autopsy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'placeOfDeath' as naaccrId, 1940 as naaccrNum , 0 as identified_only , '@' as valtype_cd , placeOfDeath as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where placeOfDeath is not null;

-- 6: Hospital-Specific
drop table if exists section_6;
create table section_6 as
-- 6: Hospital-Specific

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sequenceNumberHospital' as naaccrId, 560 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sequenceNumberHospital as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sequenceNumberHospital is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'abstractedBy' as naaccrId, 570 as naaccrNum , 0 as identified_only , '@' as valtype_cd , abstractedBy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where abstractedBy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOf1stContact' as naaccrId, 580 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOf1stContact as date_value , null as text_value , tumor_id from naaccr_fields where dateOf1stContact is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOf1stContactFlag' as naaccrId, 581 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOf1stContactFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOf1stContactFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfInptAdm' as naaccrId, 590 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfInptAdm as date_value , null as text_value , tumor_id from naaccr_fields where dateOfInptAdm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfInptAdmFlag' as naaccrId, 591 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfInptAdmFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfInptAdmFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfInptDisch' as naaccrId, 600 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateOfInptDisch as date_value , null as text_value , tumor_id from naaccr_fields where dateOfInptDisch is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateOfInptDischFlag' as naaccrId, 601 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateOfInptDischFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateOfInptDischFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'inpatientStatus' as naaccrId, 605 as naaccrNum , 0 as identified_only , '@' as valtype_cd , inpatientStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where inpatientStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'classOfCase' as naaccrId, 610 as naaccrNum , 0 as identified_only , '@' as valtype_cd , classOfCase as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where classOfCase is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'primaryPayerAtDx' as naaccrId, 630 as naaccrNum , 0 as identified_only , '@' as valtype_cd , primaryPayerAtDx as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where primaryPayerAtDx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospSurgApp2010' as naaccrId, 668 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospSurgApp2010 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospSurgApp2010 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospSurgPrimSite' as naaccrId, 670 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospSurgPrimSite as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospSurgPrimSite is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospScopeRegLnSur' as naaccrId, 672 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospScopeRegLnSur as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospScopeRegLnSur is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospSurgOthRegDis' as naaccrId, 674 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospSurgOthRegDis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospSurgOthRegDis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospRegLnRemoved' as naaccrId, 676 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , rxHospRegLnRemoved as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospRegLnRemoved is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospRadiation' as naaccrId, 690 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospRadiation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospRadiation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospChemo' as naaccrId, 700 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospChemo as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospHormone' as naaccrId, 710 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospHormone as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospHormone is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospBrm' as naaccrId, 720 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospBrm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospOther' as naaccrId, 730 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospOther as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospOther is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospDxStgProc' as naaccrId, 740 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospDxStgProc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospDxStgProc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospSurgSite9802' as naaccrId, 746 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospSurgSite9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospSurgSite9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospScopeReg9802' as naaccrId, 747 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospScopeReg9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospScopeReg9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospSurgOth9802' as naaccrId, 748 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospSurgOth9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospSurgOth9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxHospPalliativeProc' as naaccrId, 3280 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxHospPalliativeProc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxHospPalliativeProc is not null;

-- 9: Record ID
drop table if exists section_9;
create table section_9 as
-- 9: Record ID

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'recordType' as naaccrId, 10 as naaccrNum , 0 as identified_only , '@' as valtype_cd , recordType as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where recordType is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'registryType' as naaccrId, 30 as naaccrNum , 0 as identified_only , '@' as valtype_cd , registryType as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where registryType is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'registryId' as naaccrId, 40 as naaccrNum , 0 as identified_only , 'T' as valtype_cd , null as code_value , null as numeric_value , null as date_value , registryId as text_value , tumor_id from naaccr_fields where registryId is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'npiRegistryId' as naaccrId, 45 as naaccrNum , 0 as identified_only , 'T' as valtype_cd , null as code_value , null as numeric_value , null as date_value , npiRegistryId as text_value , tumor_id from naaccr_fields where npiRegistryId is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'naaccrRecordVersion' as naaccrId, 50 as naaccrNum , 0 as identified_only , '@' as valtype_cd , naaccrRecordVersion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where naaccrRecordVersion is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorRecordNumber' as naaccrId, 60 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorRecordNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorRecordNumber is not null;

-- 11: Stage/Prognostic Factors
drop table if exists section_11;
create table section_11 as
-- 11: Stage/Prognostic Factors

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateRegionalLNDissection' as naaccrId, 682 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateRegionalLNDissection as date_value , null as text_value , tumor_id from naaccr_fields where dateRegionalLNDissection is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateRegionalLNDissectionFlag' as naaccrId, 683 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateRegionalLNDissectionFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateRegionalLNDissectionFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorSizeClinical' as naaccrId, 752 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorSizeClinical as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorSizeClinical is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorSizePathologic' as naaccrId, 754 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorSizePathologic as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorSizePathologic is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorSizeSummary' as naaccrId, 756 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorSizeSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorSizeSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSummaryStage2000' as naaccrId, 759 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSummaryStage2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSummaryStage2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSummaryStage1977' as naaccrId, 760 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSummaryStage1977 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSummaryStage1977 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSummaryStage2018' as naaccrId, 762 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSummaryStage2018 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSummaryStage2018 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'summaryStage2018' as naaccrId, 764 as naaccrNum , 0 as identified_only , '@' as valtype_cd , summaryStage2018 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where summaryStage2018 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodPrimaryTumor' as naaccrId, 772 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodPrimaryTumor as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodPrimaryTumor is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodRegionalNodes' as naaccrId, 774 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodRegionalNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodRegionalNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodMets' as naaccrId, 776 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodMets as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodMets is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodTumorSize' as naaccrId, 780 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , eodTumorSize as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodTumorSize is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodExtension' as naaccrId, 790 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodExtension as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodExtension is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodExtensionProstPath' as naaccrId, 800 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodExtensionProstPath as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodExtensionProstPath is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodLymphNodeInvolv' as naaccrId, 810 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodLymphNodeInvolv as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodLymphNodeInvolv is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'regionalNodesPositive' as naaccrId, 820 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , regionalNodesPositive as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where regionalNodesPositive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'regionalNodesExamined' as naaccrId, 830 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , regionalNodesExamined as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where regionalNodesExamined is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateSentinelLymphNodeBiopsy' as naaccrId, 832 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateSentinelLymphNodeBiopsy as date_value , null as text_value , tumor_id from naaccr_fields where dateSentinelLymphNodeBiopsy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateSentinelLymphNodeBiopsyFlag' as naaccrId, 833 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateSentinelLymphNodeBiopsyFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateSentinelLymphNodeBiopsyFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sentinelLymphNodesExamined' as naaccrId, 834 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sentinelLymphNodesExamined as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sentinelLymphNodesExamined is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sentinelLymphNodesPositive' as naaccrId, 835 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sentinelLymphNodesPositive as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sentinelLymphNodesPositive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodOld2Digit' as naaccrId, 850 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodOld2Digit as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodOld2Digit is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'eodOld4Digit' as naaccrId, 860 as naaccrNum , 0 as identified_only , '@' as valtype_cd , eodOld4Digit as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where eodOld4Digit is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'codingSystemForEod' as naaccrId, 870 as naaccrNum , 0 as identified_only , '@' as valtype_cd , codingSystemForEod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where codingSystemForEod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathT' as naaccrId, 880 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathT as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathT is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathN' as naaccrId, 890 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathM' as naaccrId, 900 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathM as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathM is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathStageGroup' as naaccrId, 910 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathStageGroup as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathStageGroup is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathDescriptor' as naaccrId, 920 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathDescriptor as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathDescriptor is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmPathStagedBy' as naaccrId, 930 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmPathStagedBy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmPathStagedBy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinT' as naaccrId, 940 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinT as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinT is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinN' as naaccrId, 950 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinM' as naaccrId, 960 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinM as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinM is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinStageGroup' as naaccrId, 970 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinStageGroup as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinStageGroup is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinDescriptor' as naaccrId, 980 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinDescriptor as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinDescriptor is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmClinStagedBy' as naaccrId, 990 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmClinStagedBy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmClinStagedBy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccId' as naaccrId, 995 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccId as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccId is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmClinTSuffix' as naaccrId, 1031 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmClinTSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmClinTSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmPathTSuffix' as naaccrId, 1032 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmPathTSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmPathTSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmPostTherapyTSuffix' as naaccrId, 1033 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmPostTherapyTSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmPostTherapyTSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmClinNSuffix' as naaccrId, 1034 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmClinNSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmClinNSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmPathNSuffix' as naaccrId, 1035 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmPathNSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmPathNSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ajccTnmPostTherapyNSuffix' as naaccrId, 1036 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ajccTnmPostTherapyNSuffix as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ajccTnmPostTherapyNSuffix is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tnmEditionNumber' as naaccrId, 1060 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tnmEditionNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tnmEditionNumber is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxBone' as naaccrId, 1112 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxBone as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxBone is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxBrain' as naaccrId, 1113 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxBrain as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxBrain is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxDistantLn' as naaccrId, 1114 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxDistantLn as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxDistantLn is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxLiver' as naaccrId, 1115 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxLiver as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxLiver is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxLung' as naaccrId, 1116 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxLung as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxLung is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'metsAtDxOther' as naaccrId, 1117 as naaccrNum , 0 as identified_only , '@' as valtype_cd , metsAtDxOther as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where metsAtDxOther is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pediatricStage' as naaccrId, 1120 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pediatricStage as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pediatricStage is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pediatricStagingSystem' as naaccrId, 1130 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pediatricStagingSystem as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pediatricStagingSystem is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pediatricStagedBy' as naaccrId, 1140 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pediatricStagedBy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pediatricStagedBy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorMarker1' as naaccrId, 1150 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorMarker1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorMarker1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorMarker2' as naaccrId, 1160 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorMarker2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorMarker2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorMarker3' as naaccrId, 1170 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorMarker3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorMarker3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lymphVascularInvasion' as naaccrId, 1182 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lymphVascularInvasion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lymphVascularInvasion is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csTumorSize' as naaccrId, 2800 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , csTumorSize as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csTumorSize is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csExtension' as naaccrId, 2810 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csExtension as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csExtension is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csTumorSizeExtEval' as naaccrId, 2820 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csTumorSizeExtEval as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csTumorSizeExtEval is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csLymphNodes' as naaccrId, 2830 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csLymphNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csLymphNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csLymphNodesEval' as naaccrId, 2840 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csLymphNodesEval as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csLymphNodesEval is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsAtDx' as naaccrId, 2850 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsAtDx as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsAtDx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsAtDxBone' as naaccrId, 2851 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsAtDxBone as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsAtDxBone is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsAtDxBrain' as naaccrId, 2852 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsAtDxBrain as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsAtDxBrain is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsAtDxLiver' as naaccrId, 2853 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsAtDxLiver as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsAtDxLiver is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsAtDxLung' as naaccrId, 2854 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsAtDxLung as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsAtDxLung is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csMetsEval' as naaccrId, 2860 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csMetsEval as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csMetsEval is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor7' as naaccrId, 2861 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor7 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor7 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor8' as naaccrId, 2862 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor8 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor8 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor9' as naaccrId, 2863 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor9 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor9 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor10' as naaccrId, 2864 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor10 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor10 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor11' as naaccrId, 2865 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor11 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor11 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor12' as naaccrId, 2866 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor12 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor12 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor13' as naaccrId, 2867 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor13 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor13 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor14' as naaccrId, 2868 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor14 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor14 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor15' as naaccrId, 2869 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor15 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor15 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor16' as naaccrId, 2870 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor16 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor16 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor17' as naaccrId, 2871 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor17 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor17 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor18' as naaccrId, 2872 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor18 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor18 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor19' as naaccrId, 2873 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor19 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor19 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor20' as naaccrId, 2874 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor20 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor20 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor21' as naaccrId, 2875 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor21 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor21 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor22' as naaccrId, 2876 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor22 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor22 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor23' as naaccrId, 2877 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor23 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor23 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor24' as naaccrId, 2878 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor24 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor24 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor25' as naaccrId, 2879 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor25 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor25 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor1' as naaccrId, 2880 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor2' as naaccrId, 2890 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor3' as naaccrId, 2900 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor4' as naaccrId, 2910 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor5' as naaccrId, 2920 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csSiteSpecificFactor6' as naaccrId, 2930 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csSiteSpecificFactor6 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csSiteSpecificFactor6 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csVersionInputOriginal' as naaccrId, 2935 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csVersionInputOriginal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csVersionInputOriginal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csVersionDerived' as naaccrId, 2936 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csVersionDerived as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csVersionDerived is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'csVersionInputCurrent' as naaccrId, 2937 as naaccrNum , 0 as identified_only , '@' as valtype_cd , csVersionInputCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where csVersionInputCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6T' as naaccrId, 2940 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6T as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6T is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6TDescript' as naaccrId, 2950 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6TDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6TDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6N' as naaccrId, 2960 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6N as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6N is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6NDescript' as naaccrId, 2970 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6NDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6NDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6M' as naaccrId, 2980 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6M as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6M is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6MDescript' as naaccrId, 2990 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6MDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6MDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc6StageGrp' as naaccrId, 3000 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc6StageGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc6StageGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSs1977' as naaccrId, 3010 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSs1977 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSs1977 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSs2000' as naaccrId, 3020 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSs2000 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSs2000 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjccFlag' as naaccrId, 3030 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjccFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjccFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSs1977Flag' as naaccrId, 3040 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSs1977Flag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSs1977Flag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSs2000Flag' as naaccrId, 3050 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSs2000Flag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSs2000Flag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication1' as naaccrId, 3110 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication2' as naaccrId, 3120 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication3' as naaccrId, 3130 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication4' as naaccrId, 3140 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication5' as naaccrId, 3150 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication6' as naaccrId, 3160 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication6 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication6 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication7' as naaccrId, 3161 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication7 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication7 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication8' as naaccrId, 3162 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication8 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication8 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication9' as naaccrId, 3163 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication9 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication9 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'comorbidComplication10' as naaccrId, 3164 as naaccrNum , 0 as identified_only , '@' as valtype_cd , comorbidComplication10 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where comorbidComplication10 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'icdRevisionComorbid' as naaccrId, 3165 as naaccrNum , 0 as identified_only , '@' as valtype_cd , icdRevisionComorbid as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where icdRevisionComorbid is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7T' as naaccrId, 3400 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7T as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7T is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7TDescript' as naaccrId, 3402 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7TDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7TDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7N' as naaccrId, 3410 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7N as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7N is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7NDescript' as naaccrId, 3412 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7NDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7NDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7M' as naaccrId, 3420 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7M as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7M is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7MDescript' as naaccrId, 3422 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7MDescript as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7MDescript is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedAjcc7StageGrp' as naaccrId, 3430 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedAjcc7StageGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedAjcc7StageGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7T' as naaccrId, 3440 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7T as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7T is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7TDescrip' as naaccrId, 3442 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7TDescrip as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7TDescrip is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7N' as naaccrId, 3450 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7N as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7N is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7NDescrip' as naaccrId, 3452 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7NDescrip as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7NDescrip is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7M' as naaccrId, 3460 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7M as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7M is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7MDescrip' as naaccrId, 3462 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7MDescrip as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7MDescrip is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPrerx7StageGrp' as naaccrId, 3470 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPrerx7StageGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPrerx7StageGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPostrx7T' as naaccrId, 3480 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPostrx7T as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPostrx7T is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPostrx7N' as naaccrId, 3482 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPostrx7N as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPostrx7N is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPostrx7M' as naaccrId, 3490 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPostrx7M as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPostrx7M is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedPostrx7StgeGrp' as naaccrId, 3492 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedPostrx7StgeGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedPostrx7StgeGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedNeoadjuvRxFlag' as naaccrId, 3600 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedNeoadjuvRxFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedNeoadjuvRxFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerPathStgGrp' as naaccrId, 3605 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerPathStgGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerPathStgGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerClinStgGrp' as naaccrId, 3610 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerClinStgGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerClinStgGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCmbStgGrp' as naaccrId, 3614 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCmbStgGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCmbStgGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCombinedT' as naaccrId, 3616 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCombinedT as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCombinedT is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCombinedN' as naaccrId, 3618 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCombinedN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCombinedN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCombinedM' as naaccrId, 3620 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCombinedM as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCombinedM is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCmbTSrc' as naaccrId, 3622 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCmbTSrc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCmbTSrc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCmbNSrc' as naaccrId, 3624 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCmbNSrc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCmbNSrc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'derivedSeerCmbMSrc' as naaccrId, 3626 as naaccrNum , 0 as identified_only , '@' as valtype_cd , derivedSeerCmbMSrc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where derivedSeerCmbMSrc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'npcrDerivedClinStgGrp' as naaccrId, 3650 as naaccrNum , 0 as identified_only , '@' as valtype_cd , npcrDerivedClinStgGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where npcrDerivedClinStgGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'npcrDerivedPathStgGrp' as naaccrId, 3655 as naaccrNum , 0 as identified_only , '@' as valtype_cd , npcrDerivedPathStgGrp as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where npcrDerivedPathStgGrp is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact1' as naaccrId, 3700 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact2' as naaccrId, 3702 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact3' as naaccrId, 3704 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact4' as naaccrId, 3706 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact5' as naaccrId, 3708 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'seerSiteSpecificFact6' as naaccrId, 3710 as naaccrNum , 0 as identified_only , '@' as valtype_cd , seerSiteSpecificFact6 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where seerSiteSpecificFact6 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis1' as naaccrId, 3780 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis2' as naaccrId, 3782 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis3' as naaccrId, 3784 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis4' as naaccrId, 3786 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis5' as naaccrId, 3788 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis6' as naaccrId, 3790 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis6 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis6 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis7' as naaccrId, 3792 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis7 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis7 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis8' as naaccrId, 3794 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis8 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis8 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis9' as naaccrId, 3796 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis9 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis9 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'secondaryDiagnosis10' as naaccrId, 3798 as naaccrNum , 0 as identified_only , '@' as valtype_cd , secondaryDiagnosis10 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where secondaryDiagnosis10 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'schemaId' as naaccrId, 3800 as naaccrNum , 0 as identified_only , '@' as valtype_cd , schemaId as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where schemaId is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'chromosome1pLossHeterozygosity' as naaccrId, 3801 as naaccrNum , 0 as identified_only , '@' as valtype_cd , chromosome1pLossHeterozygosity as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where chromosome1pLossHeterozygosity is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'chromosome19qLossHeterozygosity' as naaccrId, 3802 as naaccrNum , 0 as identified_only , '@' as valtype_cd , chromosome19qLossHeterozygosity as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where chromosome19qLossHeterozygosity is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'adenoidCysticBasaloidPattern' as naaccrId, 3803 as naaccrNum , 0 as identified_only , '@' as valtype_cd , adenoidCysticBasaloidPattern as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where adenoidCysticBasaloidPattern is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'adenopathy' as naaccrId, 3804 as naaccrNum , 0 as identified_only , '@' as valtype_cd , adenopathy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where adenopathy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPostOrchiectomyLabValue' as naaccrId, 3805 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , afpPostOrchiectomyLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPostOrchiectomyLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPostOrchiectomyRange' as naaccrId, 3806 as naaccrNum , 0 as identified_only , '@' as valtype_cd , afpPostOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPostOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPreOrchiectomyLabValue' as naaccrId, 3807 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , afpPreOrchiectomyLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPreOrchiectomyLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPreOrchiectomyRange' as naaccrId, 3808 as naaccrNum , 0 as identified_only , '@' as valtype_cd , afpPreOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPreOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPretreatmentInterpretation' as naaccrId, 3809 as naaccrNum , 0 as identified_only , '@' as valtype_cd , afpPretreatmentInterpretation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPretreatmentInterpretation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'afpPretreatmentLabValue' as naaccrId, 3810 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , afpPretreatmentLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where afpPretreatmentLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'anemia' as naaccrId, 3811 as naaccrNum , 0 as identified_only , '@' as valtype_cd , anemia as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where anemia is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'bSymptoms' as naaccrId, 3812 as naaccrNum , 0 as identified_only , '@' as valtype_cd , bSymptoms as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where bSymptoms is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'bilirubinPretxTotalLabValue' as naaccrId, 3813 as naaccrNum , 0 as identified_only , '@' as valtype_cd , bilirubinPretxTotalLabValue as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where bilirubinPretxTotalLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'bilirubinPretxUnitOfMeasure' as naaccrId, 3814 as naaccrNum , 0 as identified_only , '@' as valtype_cd , bilirubinPretxUnitOfMeasure as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where bilirubinPretxUnitOfMeasure is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'boneInvasion' as naaccrId, 3815 as naaccrNum , 0 as identified_only , '@' as valtype_cd , boneInvasion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where boneInvasion is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'brainMolecularMarkers' as naaccrId, 3816 as naaccrNum , 0 as identified_only , '@' as valtype_cd , brainMolecularMarkers as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where brainMolecularMarkers is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'breslowTumorThickness' as naaccrId, 3817 as naaccrNum , 0 as identified_only , '@' as valtype_cd , breslowTumorThickness as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where breslowTumorThickness is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ca125PretreatmentInterpretation' as naaccrId, 3818 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ca125PretreatmentInterpretation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ca125PretreatmentInterpretation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ceaPretreatmentInterpretation' as naaccrId, 3819 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ceaPretreatmentInterpretation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ceaPretreatmentInterpretation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ceaPretreatmentLabValue' as naaccrId, 3820 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , ceaPretreatmentLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ceaPretreatmentLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'chromosome3Status' as naaccrId, 3821 as naaccrNum , 0 as identified_only , '@' as valtype_cd , chromosome3Status as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where chromosome3Status is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'chromosome8qStatus' as naaccrId, 3822 as naaccrNum , 0 as identified_only , '@' as valtype_cd , chromosome8qStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where chromosome8qStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'circumferentialResectionMargin' as naaccrId, 3823 as naaccrNum , 0 as identified_only , '@' as valtype_cd , circumferentialResectionMargin as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where circumferentialResectionMargin is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'creatininePretreatmentLabValue' as naaccrId, 3824 as naaccrNum , 0 as identified_only , '@' as valtype_cd , creatininePretreatmentLabValue as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where creatininePretreatmentLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'creatininePretxUnitOfMeasure' as naaccrId, 3825 as naaccrNum , 0 as identified_only , '@' as valtype_cd , creatininePretxUnitOfMeasure as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where creatininePretxUnitOfMeasure is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'estrogenReceptorPercntPosOrRange' as naaccrId, 3826 as naaccrNum , 0 as identified_only , '@' as valtype_cd , estrogenReceptorPercntPosOrRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where estrogenReceptorPercntPosOrRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'estrogenReceptorSummary' as naaccrId, 3827 as naaccrNum , 0 as identified_only , '@' as valtype_cd , estrogenReceptorSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where estrogenReceptorSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'estrogenReceptorTotalAllredScore' as naaccrId, 3828 as naaccrNum , 0 as identified_only , '@' as valtype_cd , estrogenReceptorTotalAllredScore as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where estrogenReceptorTotalAllredScore is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'esophagusAndEgjTumorEpicenter' as naaccrId, 3829 as naaccrNum , 0 as identified_only , '@' as valtype_cd , esophagusAndEgjTumorEpicenter as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where esophagusAndEgjTumorEpicenter is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'extranodalExtensionClin' as naaccrId, 3830 as naaccrNum , 0 as identified_only , '@' as valtype_cd , extranodalExtensionClin as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where extranodalExtensionClin is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'extranodalExtensionHeadNeckClin' as naaccrId, 3831 as naaccrNum , 0 as identified_only , '@' as valtype_cd , extranodalExtensionHeadNeckClin as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where extranodalExtensionHeadNeckClin is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'extranodalExtensionHeadNeckPath' as naaccrId, 3832 as naaccrNum , 0 as identified_only , '@' as valtype_cd , extranodalExtensionHeadNeckPath as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where extranodalExtensionHeadNeckPath is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'extranodalExtensionPath' as naaccrId, 3833 as naaccrNum , 0 as identified_only , '@' as valtype_cd , extranodalExtensionPath as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where extranodalExtensionPath is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'extravascularMatrixPatterns' as naaccrId, 3834 as naaccrNum , 0 as identified_only , '@' as valtype_cd , extravascularMatrixPatterns as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where extravascularMatrixPatterns is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'fibrosisScore' as naaccrId, 3835 as naaccrNum , 0 as identified_only , '@' as valtype_cd , fibrosisScore as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where fibrosisScore is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'figoStage' as naaccrId, 3836 as naaccrNum , 0 as identified_only , '@' as valtype_cd , figoStage as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where figoStage is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gestationalTrophoblasticPxIndex' as naaccrId, 3837 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gestationalTrophoblasticPxIndex as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gestationalTrophoblasticPxIndex is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gleasonPatternsClinical' as naaccrId, 3838 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gleasonPatternsClinical as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gleasonPatternsClinical is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gleasonPatternsPathological' as naaccrId, 3839 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gleasonPatternsPathological as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gleasonPatternsPathological is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gleasonScoreClinical' as naaccrId, 3840 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gleasonScoreClinical as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gleasonScoreClinical is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gleasonScorePathological' as naaccrId, 3841 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gleasonScorePathological as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gleasonScorePathological is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gleasonTertiaryPattern' as naaccrId, 3842 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gleasonTertiaryPattern as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gleasonTertiaryPattern is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradeClinical' as naaccrId, 3843 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradeClinical as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradeClinical is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradePathological' as naaccrId, 3844 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradePathological as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradePathological is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'gradePostTherapy' as naaccrId, 3845 as naaccrNum , 0 as identified_only , '@' as valtype_cd , gradePostTherapy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where gradePostTherapy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'hcgPostOrchiectomyLabValue' as naaccrId, 3846 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , hcgPostOrchiectomyLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where hcgPostOrchiectomyLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'hcgPostOrchiectomyRange' as naaccrId, 3847 as naaccrNum , 0 as identified_only , '@' as valtype_cd , hcgPostOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where hcgPostOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'hcgPreOrchiectomyLabValue' as naaccrId, 3848 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , hcgPreOrchiectomyLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where hcgPreOrchiectomyLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'hcgPreOrchiectomyRange' as naaccrId, 3849 as naaccrNum , 0 as identified_only , '@' as valtype_cd , hcgPreOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where hcgPreOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2IhcSummary' as naaccrId, 3850 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2IhcSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2IhcSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2IshDualProbeCopyNumber' as naaccrId, 3851 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2IshDualProbeCopyNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2IshDualProbeCopyNumber is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2IshDualProbeRatio' as naaccrId, 3852 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2IshDualProbeRatio as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2IshDualProbeRatio is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2IshSingleProbeCopyNumber' as naaccrId, 3853 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2IshSingleProbeCopyNumber as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2IshSingleProbeCopyNumber is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2IshSummary' as naaccrId, 3854 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2IshSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2IshSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'her2OverallSummary' as naaccrId, 3855 as naaccrNum , 0 as identified_only , '@' as valtype_cd , her2OverallSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where her2OverallSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'heritableTrait' as naaccrId, 3856 as naaccrNum , 0 as identified_only , '@' as valtype_cd , heritableTrait as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where heritableTrait is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'highRiskCytogenetics' as naaccrId, 3857 as naaccrNum , 0 as identified_only , '@' as valtype_cd , highRiskCytogenetics as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where highRiskCytogenetics is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'highRiskHistologicFeatures' as naaccrId, 3858 as naaccrNum , 0 as identified_only , '@' as valtype_cd , highRiskHistologicFeatures as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where highRiskHistologicFeatures is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'hivStatus' as naaccrId, 3859 as naaccrNum , 0 as identified_only , '@' as valtype_cd , hivStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where hivStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'iNRProthrombinTime' as naaccrId, 3860 as naaccrNum , 0 as identified_only , '@' as valtype_cd , iNRProthrombinTime as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where iNRProthrombinTime is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ipsilateralAdrenalGlandInvolve' as naaccrId, 3861 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ipsilateralAdrenalGlandInvolve as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ipsilateralAdrenalGlandInvolve is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'jak2' as naaccrId, 3862 as naaccrNum , 0 as identified_only , '@' as valtype_cd , jak2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where jak2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ki67' as naaccrId, 3863 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ki67 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ki67 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'invasionBeyondCapsule' as naaccrId, 3864 as naaccrNum , 0 as identified_only , '@' as valtype_cd , invasionBeyondCapsule as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where invasionBeyondCapsule is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'kitGeneImmunohistochemistry' as naaccrId, 3865 as naaccrNum , 0 as identified_only , '@' as valtype_cd , kitGeneImmunohistochemistry as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where kitGeneImmunohistochemistry is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'kras' as naaccrId, 3866 as naaccrNum , 0 as identified_only , '@' as valtype_cd , kras as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where kras is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ldhPostOrchiectomyRange' as naaccrId, 3867 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ldhPostOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ldhPostOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ldhPreOrchiectomyRange' as naaccrId, 3868 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ldhPreOrchiectomyRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ldhPreOrchiectomyRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ldhPretreatmentLevel' as naaccrId, 3869 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ldhPretreatmentLevel as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ldhPretreatmentLevel is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ldhUpperLimitsOfNormal' as naaccrId, 3870 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ldhUpperLimitsOfNormal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ldhUpperLimitsOfNormal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnAssessMethodFemoralInguinal' as naaccrId, 3871 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnAssessMethodFemoralInguinal as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnAssessMethodFemoralInguinal is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnAssessMethodParaaortic' as naaccrId, 3872 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnAssessMethodParaaortic as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnAssessMethodParaaortic is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnAssessMethodPelvic' as naaccrId, 3873 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnAssessMethodPelvic as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnAssessMethodPelvic is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnDistantAssessMethod' as naaccrId, 3874 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnDistantAssessMethod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnDistantAssessMethod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnDistantMediastinalScalene' as naaccrId, 3875 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnDistantMediastinalScalene as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnDistantMediastinalScalene is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnHeadAndNeckLevels1To3' as naaccrId, 3876 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnHeadAndNeckLevels1To3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnHeadAndNeckLevels1To3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnHeadAndNeckLevels4To5' as naaccrId, 3877 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnHeadAndNeckLevels4To5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnHeadAndNeckLevels4To5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnHeadAndNeckLevels6To7' as naaccrId, 3878 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnHeadAndNeckLevels6To7 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnHeadAndNeckLevels6To7 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnHeadAndNeckOther' as naaccrId, 3879 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnHeadAndNeckOther as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnHeadAndNeckOther is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnIsolatedTumorCells' as naaccrId, 3880 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnIsolatedTumorCells as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnIsolatedTumorCells is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnLaterality' as naaccrId, 3881 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnLaterality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnLaterality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnPositiveAxillaryLevel1To2' as naaccrId, 3882 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnPositiveAxillaryLevel1To2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnPositiveAxillaryLevel1To2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnSize' as naaccrId, 3883 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnSize as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnSize is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lnStatusFemorInguinParaaortPelv' as naaccrId, 3884 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lnStatusFemorInguinParaaortPelv as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lnStatusFemorInguinParaaortPelv is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'lymphocytosis' as naaccrId, 3885 as naaccrNum , 0 as identified_only , '@' as valtype_cd , lymphocytosis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where lymphocytosis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'majorVeinInvolvement' as naaccrId, 3886 as naaccrNum , 0 as identified_only , '@' as valtype_cd , majorVeinInvolvement as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where majorVeinInvolvement is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'measuredBasalDiameter' as naaccrId, 3887 as naaccrNum , 0 as identified_only , '@' as valtype_cd , measuredBasalDiameter as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where measuredBasalDiameter is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'measuredThickness' as naaccrId, 3888 as naaccrNum , 0 as identified_only , '@' as valtype_cd , measuredThickness as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where measuredThickness is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'methylationOfO6MGMT' as naaccrId, 3889 as naaccrNum , 0 as identified_only , '@' as valtype_cd , methylationOfO6MGMT as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where methylationOfO6MGMT is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'microsatelliteInstability' as naaccrId, 3890 as naaccrNum , 0 as identified_only , '@' as valtype_cd , microsatelliteInstability as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where microsatelliteInstability is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'microvascularDensity' as naaccrId, 3891 as naaccrNum , 0 as identified_only , '@' as valtype_cd , microvascularDensity as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where microvascularDensity is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'mitoticCountUvealMelanoma' as naaccrId, 3892 as naaccrNum , 0 as identified_only , '@' as valtype_cd , mitoticCountUvealMelanoma as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where mitoticCountUvealMelanoma is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'mitoticRateMelanoma' as naaccrId, 3893 as naaccrNum , 0 as identified_only , '@' as valtype_cd , mitoticRateMelanoma as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where mitoticRateMelanoma is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'multigeneSignatureMethod' as naaccrId, 3894 as naaccrNum , 0 as identified_only , '@' as valtype_cd , multigeneSignatureMethod as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where multigeneSignatureMethod is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'multigeneSignatureResults' as naaccrId, 3895 as naaccrNum , 0 as identified_only , '@' as valtype_cd , multigeneSignatureResults as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where multigeneSignatureResults is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'nccnInternationalPrognosticIndex' as naaccrId, 3896 as naaccrNum , 0 as identified_only , '@' as valtype_cd , nccnInternationalPrognosticIndex as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where nccnInternationalPrognosticIndex is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfCoresExamined' as naaccrId, 3897 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfCoresExamined as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfCoresExamined is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfCoresPositive' as naaccrId, 3898 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfCoresPositive as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfCoresPositive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfExaminedParaAorticNodes' as naaccrId, 3899 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfExaminedParaAorticNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfExaminedParaAorticNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfExaminedPelvicNodes' as naaccrId, 3900 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfExaminedPelvicNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfExaminedPelvicNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfPositiveParaAorticNodes' as naaccrId, 3901 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfPositiveParaAorticNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfPositiveParaAorticNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberOfPositivePelvicNodes' as naaccrId, 3902 as naaccrNum , 0 as identified_only , '@' as valtype_cd , numberOfPositivePelvicNodes as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberOfPositivePelvicNodes is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'oncotypeDxRecurrenceScoreDcis' as naaccrId, 3903 as naaccrNum , 0 as identified_only , '@' as valtype_cd , oncotypeDxRecurrenceScoreDcis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where oncotypeDxRecurrenceScoreDcis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'oncotypeDxRecurrenceScoreInvasiv' as naaccrId, 3904 as naaccrNum , 0 as identified_only , '@' as valtype_cd , oncotypeDxRecurrenceScoreInvasiv as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where oncotypeDxRecurrenceScoreInvasiv is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'oncotypeDxRiskLevelDcis' as naaccrId, 3905 as naaccrNum , 0 as identified_only , '@' as valtype_cd , oncotypeDxRiskLevelDcis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where oncotypeDxRiskLevelDcis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'oncotypeDxRiskLevelInvasive' as naaccrId, 3906 as naaccrNum , 0 as identified_only , '@' as valtype_cd , oncotypeDxRiskLevelInvasive as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where oncotypeDxRiskLevelInvasive is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'organomegaly' as naaccrId, 3907 as naaccrNum , 0 as identified_only , '@' as valtype_cd , organomegaly as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where organomegaly is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'percentNecrosisPostNeoadjuvant' as naaccrId, 3908 as naaccrNum , 0 as identified_only , '@' as valtype_cd , percentNecrosisPostNeoadjuvant as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where percentNecrosisPostNeoadjuvant is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'perineuralInvasion' as naaccrId, 3909 as naaccrNum , 0 as identified_only , '@' as valtype_cd , perineuralInvasion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where perineuralInvasion is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'peripheralBloodInvolvement' as naaccrId, 3910 as naaccrNum , 0 as identified_only , '@' as valtype_cd , peripheralBloodInvolvement as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where peripheralBloodInvolvement is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'peritonealCytology' as naaccrId, 3911 as naaccrNum , 0 as identified_only , '@' as valtype_cd , peritonealCytology as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where peritonealCytology is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pleuralEffusion' as naaccrId, 3913 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pleuralEffusion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pleuralEffusion is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'progesteroneRecepPrcntPosOrRange' as naaccrId, 3914 as naaccrNum , 0 as identified_only , '@' as valtype_cd , progesteroneRecepPrcntPosOrRange as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where progesteroneRecepPrcntPosOrRange is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'progesteroneRecepSummary' as naaccrId, 3915 as naaccrNum , 0 as identified_only , '@' as valtype_cd , progesteroneRecepSummary as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where progesteroneRecepSummary is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'progesteroneRecepTotalAllredScor' as naaccrId, 3916 as naaccrNum , 0 as identified_only , '@' as valtype_cd , progesteroneRecepTotalAllredScor as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where progesteroneRecepTotalAllredScor is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'primarySclerosingCholangitis' as naaccrId, 3917 as naaccrNum , 0 as identified_only , '@' as valtype_cd , primarySclerosingCholangitis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where primarySclerosingCholangitis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'profoundImmuneSuppression' as naaccrId, 3918 as naaccrNum , 0 as identified_only , '@' as valtype_cd , profoundImmuneSuppression as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where profoundImmuneSuppression is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'prostatePathologicalExtension' as naaccrId, 3919 as naaccrNum , 0 as identified_only , '@' as valtype_cd , prostatePathologicalExtension as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where prostatePathologicalExtension is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'psaLabValue' as naaccrId, 3920 as naaccrNum , 0 as identified_only , '@' as valtype_cd , psaLabValue as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where psaLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'residualTumVolPostCytoreduction' as naaccrId, 3921 as naaccrNum , 0 as identified_only , '@' as valtype_cd , residualTumVolPostCytoreduction as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where residualTumVolPostCytoreduction is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'responseToNeoadjuvantTherapy' as naaccrId, 3922 as naaccrNum , 0 as identified_only , '@' as valtype_cd , responseToNeoadjuvantTherapy as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where responseToNeoadjuvantTherapy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sCategoryClinical' as naaccrId, 3923 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sCategoryClinical as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sCategoryClinical is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sCategoryPathological' as naaccrId, 3924 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sCategoryPathological as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sCategoryPathological is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'sarcomatoidFeatures' as naaccrId, 3925 as naaccrNum , 0 as identified_only , '@' as valtype_cd , sarcomatoidFeatures as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where sarcomatoidFeatures is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'schemaDiscriminator1' as naaccrId, 3926 as naaccrNum , 0 as identified_only , '@' as valtype_cd , schemaDiscriminator1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where schemaDiscriminator1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'schemaDiscriminator2' as naaccrId, 3927 as naaccrNum , 0 as identified_only , '@' as valtype_cd , schemaDiscriminator2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where schemaDiscriminator2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'schemaDiscriminator3' as naaccrId, 3928 as naaccrNum , 0 as identified_only , '@' as valtype_cd , schemaDiscriminator3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where schemaDiscriminator3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'separateTumorNodules' as naaccrId, 3929 as naaccrNum , 0 as identified_only , '@' as valtype_cd , separateTumorNodules as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where separateTumorNodules is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'serumAlbuminPretreatmentLevel' as naaccrId, 3930 as naaccrNum , 0 as identified_only , '@' as valtype_cd , serumAlbuminPretreatmentLevel as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where serumAlbuminPretreatmentLevel is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'serumBeta2MicroglobulinPretxLvl' as naaccrId, 3931 as naaccrNum , 0 as identified_only , '@' as valtype_cd , serumBeta2MicroglobulinPretxLvl as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where serumBeta2MicroglobulinPretxLvl is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ldhPretreatmentLabValue' as naaccrId, 3932 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , ldhPretreatmentLabValue as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ldhPretreatmentLabValue is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'thrombocytopenia' as naaccrId, 3933 as naaccrNum , 0 as identified_only , '@' as valtype_cd , thrombocytopenia as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where thrombocytopenia is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorDeposits' as naaccrId, 3934 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorDeposits as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorDeposits is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'tumorGrowthPattern' as naaccrId, 3935 as naaccrNum , 0 as identified_only , '@' as valtype_cd , tumorGrowthPattern as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where tumorGrowthPattern is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'ulceration' as naaccrId, 3936 as naaccrNum , 0 as identified_only , '@' as valtype_cd , ulceration as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where ulceration is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'visceralParietalPleuralInvasion' as naaccrId, 3937 as naaccrNum , 0 as identified_only , '@' as valtype_cd , visceralParietalPleuralInvasion as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where visceralParietalPleuralInvasion is not null;

-- 12: Text-Diagnosis
drop table if exists section_12;
create table section_12 as select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcPe' as naaccrId, 2520 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcPe as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcXRayScan' as naaccrId, 2530 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcXRayScan as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcScopes' as naaccrId, 2540 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcScopes as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcLabTests' as naaccrId, 2550 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcLabTests as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcOp' as naaccrId, 2560 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcOp as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textDxProcPath' as naaccrId, 2570 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textDxProcPath as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textPrimarySiteTitle' as naaccrId, 2580 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textPrimarySiteTitle as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textHistologyTitle' as naaccrId, 2590 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textHistologyTitle as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textStaging' as naaccrId, 2600 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textStaging as text_value from naaccr_fields;

-- 13: Text-Miscellaneous
drop table if exists section_13;
create table section_13 as select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textRemarks' as naaccrId, 2680 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textRemarks as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'textPlaceOfDiagnosis' as naaccrId, 2690 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , textPlaceOfDiagnosis as text_value from naaccr_fields;

-- 14: Text-Treatment
drop table if exists section_14;
create table section_14 as select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextSurgery' as naaccrId, 2610 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextSurgery as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextRadiation' as naaccrId, 2620 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextRadiation as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextRadiationOther' as naaccrId, 2630 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextRadiationOther as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextChemo' as naaccrId, 2640 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextChemo as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextHormone' as naaccrId, 2650 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextHormone as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextBrm' as naaccrId, 2660 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextBrm as text_value from naaccr_fields
union all
select tumor_id, patientIdNumber, dateCaseLastChanged, dateOfLastContact, dateCaseCompleted, dateOfDiagnosis , 'rxTextOther' as naaccrId, 2670 as naaccrNum , 1 as identified_only , null as code_value , null as numeric_value , null as date_value , rxTextOther as text_value from naaccr_fields;

-- 15: Treatment-1st Course
drop table if exists section_15;
create table section_15 as
-- 15: Treatment-1st Course

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSurgery' as naaccrId, 1200 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateSurgery as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSurgery is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSurgeryFlag' as naaccrId, 1201 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateSurgeryFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSurgeryFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateRadiation' as naaccrId, 1210 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateRadiation as date_value , null as text_value , tumor_id from naaccr_fields where rxDateRadiation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateRadiationFlag' as naaccrId, 1211 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateRadiationFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateRadiationFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateChemo' as naaccrId, 1220 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateChemo as date_value , null as text_value , tumor_id from naaccr_fields where rxDateChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateChemoFlag' as naaccrId, 1221 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateChemoFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateChemoFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateHormone' as naaccrId, 1230 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateHormone as date_value , null as text_value , tumor_id from naaccr_fields where rxDateHormone is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateHormoneFlag' as naaccrId, 1231 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateHormoneFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateHormoneFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateBrm' as naaccrId, 1240 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateBrm as date_value , null as text_value , tumor_id from naaccr_fields where rxDateBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateBrmFlag' as naaccrId, 1241 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateBrmFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateBrmFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateOther' as naaccrId, 1250 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateOther as date_value , null as text_value , tumor_id from naaccr_fields where rxDateOther is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateOtherFlag' as naaccrId, 1251 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateOtherFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateOtherFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateInitialRxSeer' as naaccrId, 1260 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , dateInitialRxSeer as date_value , null as text_value , tumor_id from naaccr_fields where dateInitialRxSeer is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'dateInitialRxSeerFlag' as naaccrId, 1261 as naaccrNum , 0 as identified_only , '@' as valtype_cd , dateInitialRxSeerFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where dateInitialRxSeerFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'date1stCrsRxCoc' as naaccrId, 1270 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , date1stCrsRxCoc as date_value , null as text_value , tumor_id from naaccr_fields where date1stCrsRxCoc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'date1stCrsRxCocFlag' as naaccrId, 1271 as naaccrNum , 0 as identified_only , '@' as valtype_cd , date1stCrsRxCocFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where date1stCrsRxCocFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateDxStgProc' as naaccrId, 1280 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateDxStgProc as date_value , null as text_value , tumor_id from naaccr_fields where rxDateDxStgProc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateDxStgProcFlag' as naaccrId, 1281 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateDxStgProcFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateDxStgProcFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummTreatmentStatus' as naaccrId, 1285 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummTreatmentStatus as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummTreatmentStatus is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgPrimSite' as naaccrId, 1290 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgPrimSite as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgPrimSite is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummScopeRegLnSur' as naaccrId, 1292 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummScopeRegLnSur as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummScopeRegLnSur is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgOthRegDis' as naaccrId, 1294 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgOthRegDis as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgOthRegDis is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummRegLnExamined' as naaccrId, 1296 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , rxSummRegLnExamined as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummRegLnExamined is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgicalApproch' as naaccrId, 1310 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgicalApproch as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgicalApproch is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgicalMargins' as naaccrId, 1320 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgicalMargins as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgicalMargins is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummReconstruct1st' as naaccrId, 1330 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummReconstruct1st as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummReconstruct1st is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'reasonForNoSurgery' as naaccrId, 1340 as naaccrNum , 0 as identified_only , '@' as valtype_cd , reasonForNoSurgery as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where reasonForNoSurgery is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummDxStgProc' as naaccrId, 1350 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummDxStgProc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummDxStgProc is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummRadiation' as naaccrId, 1360 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummRadiation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummRadiation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummRadToCns' as naaccrId, 1370 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummRadToCns as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummRadToCns is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgRadSeq' as naaccrId, 1380 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgRadSeq as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgRadSeq is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummChemo' as naaccrId, 1390 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummChemo as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummHormone' as naaccrId, 1400 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummHormone as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummHormone is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummBrm' as naaccrId, 1410 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummBrm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummOther' as naaccrId, 1420 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummOther as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummOther is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'reasonForNoRadiation' as naaccrId, 1430 as naaccrNum , 0 as identified_only , '@' as valtype_cd , reasonForNoRadiation as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where reasonForNoRadiation is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxCodingSystemCurrent' as naaccrId, 1460 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxCodingSystemCurrent as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxCodingSystemCurrent is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1DosePerFraction' as naaccrId, 1501 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase1DosePerFraction as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1DosePerFraction is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1RadiationExternalBeamTech' as naaccrId, 1502 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase1RadiationExternalBeamTech as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1RadiationExternalBeamTech is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1NumberOfFractions' as naaccrId, 1503 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase1NumberOfFractions as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1NumberOfFractions is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1RadiationPrimaryTxVolume' as naaccrId, 1504 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase1RadiationPrimaryTxVolume as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1RadiationPrimaryTxVolume is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1RadiationToDrainingLN' as naaccrId, 1505 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase1RadiationToDrainingLN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1RadiationToDrainingLN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1RadiationTreatmentModality' as naaccrId, 1506 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase1RadiationTreatmentModality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1RadiationTreatmentModality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase1TotalDose' as naaccrId, 1507 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase1TotalDose as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase1TotalDose is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radRegionalDoseCgy' as naaccrId, 1510 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , radRegionalDoseCgy as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radRegionalDoseCgy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2DosePerFraction' as naaccrId, 1511 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase2DosePerFraction as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2DosePerFraction is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2RadiationExternalBeamTech' as naaccrId, 1512 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase2RadiationExternalBeamTech as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2RadiationExternalBeamTech is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2NumberOfFractions' as naaccrId, 1513 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase2NumberOfFractions as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2NumberOfFractions is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2RadiationPrimaryTxVolume' as naaccrId, 1514 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase2RadiationPrimaryTxVolume as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2RadiationPrimaryTxVolume is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2RadiationToDrainingLN' as naaccrId, 1515 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase2RadiationToDrainingLN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2RadiationToDrainingLN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2RadiationTreatmentModality' as naaccrId, 1516 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase2RadiationTreatmentModality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2RadiationTreatmentModality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase2TotalDose' as naaccrId, 1517 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase2TotalDose as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase2TotalDose is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radNoOfTreatmentVol' as naaccrId, 1520 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , radNoOfTreatmentVol as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radNoOfTreatmentVol is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3DosePerFraction' as naaccrId, 1521 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase3DosePerFraction as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3DosePerFraction is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3RadiationExternalBeamTech' as naaccrId, 1522 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase3RadiationExternalBeamTech as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3RadiationExternalBeamTech is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3NumberOfFractions' as naaccrId, 1523 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase3NumberOfFractions as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3NumberOfFractions is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3RadiationPrimaryTxVolume' as naaccrId, 1524 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase3RadiationPrimaryTxVolume as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3RadiationPrimaryTxVolume is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3RadiationToDrainingLN' as naaccrId, 1525 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase3RadiationToDrainingLN as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3RadiationToDrainingLN is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3RadiationTreatmentModality' as naaccrId, 1526 as naaccrNum , 0 as identified_only , '@' as valtype_cd , phase3RadiationTreatmentModality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3RadiationTreatmentModality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'phase3TotalDose' as naaccrId, 1527 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , phase3TotalDose as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where phase3TotalDose is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radiationTxDiscontinuedEarly' as naaccrId, 1531 as naaccrNum , 0 as identified_only , '@' as valtype_cd , radiationTxDiscontinuedEarly as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radiationTxDiscontinuedEarly is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'numberPhasesOfRadTxToVolume' as naaccrId, 1532 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , numberPhasesOfRadTxToVolume as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where numberPhasesOfRadTxToVolume is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'totalDose' as naaccrId, 1533 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , totalDose as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where totalDose is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radTreatmentVolume' as naaccrId, 1540 as naaccrNum , 0 as identified_only , '@' as valtype_cd , radTreatmentVolume as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radTreatmentVolume is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radLocationOfRx' as naaccrId, 1550 as naaccrNum , 0 as identified_only , '@' as valtype_cd , radLocationOfRx as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radLocationOfRx is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radRegionalRxModality' as naaccrId, 1570 as naaccrNum , 0 as identified_only , '@' as valtype_cd , radRegionalRxModality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radRegionalRxModality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSystemicSurSeq' as naaccrId, 1639 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSystemicSurSeq as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSystemicSurSeq is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgeryType' as naaccrId, 1640 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgeryType as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgeryType is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgSite9802' as naaccrId, 1646 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgSite9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgSite9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummScopeReg9802' as naaccrId, 1647 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummScopeReg9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummScopeReg9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummSurgOth9802' as naaccrId, 1648 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummSurgOth9802 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummSurgOth9802 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateMostDefinSurg' as naaccrId, 3170 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateMostDefinSurg as date_value , null as text_value , tumor_id from naaccr_fields where rxDateMostDefinSurg is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateMostDefinSurgFlag' as naaccrId, 3171 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateMostDefinSurgFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateMostDefinSurgFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSurgicalDisch' as naaccrId, 3180 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateSurgicalDisch as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSurgicalDisch is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSurgicalDischFlag' as naaccrId, 3181 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateSurgicalDischFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSurgicalDischFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'readmSameHosp30Days' as naaccrId, 3190 as naaccrNum , 0 as identified_only , '@' as valtype_cd , readmSameHosp30Days as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where readmSameHosp30Days is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radBoostRxModality' as naaccrId, 3200 as naaccrNum , 0 as identified_only , '@' as valtype_cd , radBoostRxModality as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radBoostRxModality is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'radBoostDoseCgy' as naaccrId, 3210 as naaccrNum , 0 as identified_only , 'N' as valtype_cd , null as code_value , radBoostDoseCgy as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where radBoostDoseCgy is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateRadiationEnded' as naaccrId, 3220 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateRadiationEnded as date_value , null as text_value , tumor_id from naaccr_fields where rxDateRadiationEnded is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateRadiationEndedFlag' as naaccrId, 3221 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateRadiationEndedFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateRadiationEndedFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSystemic' as naaccrId, 3230 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , rxDateSystemic as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSystemic is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxDateSystemicFlag' as naaccrId, 3231 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxDateSystemicFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxDateSystemicFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummTransplntEndocr' as naaccrId, 3250 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummTransplntEndocr as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummTransplntEndocr is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'rxSummPalliativeProc' as naaccrId, 3270 as naaccrNum , 0 as identified_only , '@' as valtype_cd , rxSummPalliativeProc as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where rxSummPalliativeProc is not null;

-- 16: Treatment-Subsequent & Other
drop table if exists section_16;
create table section_16 as
-- 16: Treatment-Subsequent & Other

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseDate' as naaccrId, 1660 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , subsqRx2ndCourseDate as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseDate is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndcrsDateFlag' as naaccrId, 1661 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndcrsDateFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndcrsDateFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseSurg' as naaccrId, 1671 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseSurg as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseSurg is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseRad' as naaccrId, 1672 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseRad as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseRad is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseChemo' as naaccrId, 1673 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseChemo as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseHorm' as naaccrId, 1674 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseHorm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseHorm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseBrm' as naaccrId, 1675 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseBrm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndCourseOth' as naaccrId, 1676 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndCourseOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndCourseOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndScopeLnSu' as naaccrId, 1677 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndScopeLnSu as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndScopeLnSu is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndSurgOth' as naaccrId, 1678 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndSurgOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndSurgOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx2ndRegLnRem' as naaccrId, 1679 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx2ndRegLnRem as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx2ndRegLnRem is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseDate' as naaccrId, 1680 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , subsqRx3rdCourseDate as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseDate is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdcrsDateFlag' as naaccrId, 1681 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdcrsDateFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdcrsDateFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseSurg' as naaccrId, 1691 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseSurg as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseSurg is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseRad' as naaccrId, 1692 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseRad as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseRad is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseChemo' as naaccrId, 1693 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseChemo as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseHorm' as naaccrId, 1694 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseHorm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseHorm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseBrm' as naaccrId, 1695 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseBrm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdCourseOth' as naaccrId, 1696 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdCourseOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdCourseOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdScopeLnSu' as naaccrId, 1697 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdScopeLnSu as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdScopeLnSu is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdSurgOth' as naaccrId, 1698 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdSurgOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdSurgOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx3rdRegLnRem' as naaccrId, 1699 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx3rdRegLnRem as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx3rdRegLnRem is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseDate' as naaccrId, 1700 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , subsqRx4thCourseDate as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseDate is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thcrsDateFlag' as naaccrId, 1701 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thcrsDateFlag as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thcrsDateFlag is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseSurg' as naaccrId, 1711 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseSurg as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseSurg is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseRad' as naaccrId, 1712 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseRad as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseRad is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseChemo' as naaccrId, 1713 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseChemo as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseChemo is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseHorm' as naaccrId, 1714 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseHorm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseHorm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseBrm' as naaccrId, 1715 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseBrm as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseBrm is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thCourseOth' as naaccrId, 1716 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thCourseOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thCourseOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thScopeLnSu' as naaccrId, 1717 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thScopeLnSu as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thScopeLnSu is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thSurgOth' as naaccrId, 1718 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thSurgOth as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thSurgOth is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRx4thRegLnRem' as naaccrId, 1719 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRx4thRegLnRem as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRx4thRegLnRem is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'subsqRxReconstructDel' as naaccrId, 1741 as naaccrNum , 0 as identified_only , '@' as valtype_cd , subsqRxReconstructDel as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where subsqRxReconstructDel is not null;

-- 17: Pathology
drop table if exists section_17;
create table section_17 as
-- 17: Pathology

                select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathDateSpecCollect1' as naaccrId, 7320 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , pathDateSpecCollect1 as date_value , null as text_value , tumor_id from naaccr_fields where pathDateSpecCollect1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathDateSpecCollect2' as naaccrId, 7321 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , pathDateSpecCollect2 as date_value , null as text_value , tumor_id from naaccr_fields where pathDateSpecCollect2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathDateSpecCollect3' as naaccrId, 7322 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , pathDateSpecCollect3 as date_value , null as text_value , tumor_id from naaccr_fields where pathDateSpecCollect3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathDateSpecCollect4' as naaccrId, 7323 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , pathDateSpecCollect4 as date_value , null as text_value , tumor_id from naaccr_fields where pathDateSpecCollect4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathDateSpecCollect5' as naaccrId, 7324 as naaccrNum , 0 as identified_only , 'D' as valtype_cd , null as code_value , null as numeric_value , pathDateSpecCollect5 as date_value , null as text_value , tumor_id from naaccr_fields where pathDateSpecCollect5 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathReportType1' as naaccrId, 7480 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pathReportType1 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pathReportType1 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathReportType2' as naaccrId, 7481 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pathReportType2 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pathReportType2 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathReportType3' as naaccrId, 7482 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pathReportType3 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pathReportType3 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathReportType4' as naaccrId, 7483 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pathReportType4 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pathReportType4 is not null
union all
select patientIdNumber, dateOfBirth, dateOfDiagnosis, dateOfLastContact, dateCaseCompleted, dateCaseLastChanged , 'pathReportType5' as naaccrId, 7484 as naaccrNum , 0 as identified_only , '@' as valtype_cd , pathReportType5 as code_value , null as numeric_value , null as date_value , null as text_value , tumor_id from naaccr_fields where pathReportType5 is not null;

create table tumor_item_value as
select * from section_1
union all
select * from section_2
union all
select * from section_3
union all
select * from section_4
union all
select * from section_6
union all
select * from section_9
union all
select * from section_11
union all
select * from section_15
union all
select * from section_16
union all
select * from section_17;
