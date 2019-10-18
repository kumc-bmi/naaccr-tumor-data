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


drop view if exists naaccr_fields_raw_id;
create view naaccr_fields_raw_id as
select row_number() over (order by dateOfDiagnosis, dateOfBirth, dateOfLastContact, dateCaseLastChanged, dateCaseCompleted, dateCaseReportExported)
       as tumor_id
     , f.*
from naaccr_fields_raw f;


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
  , case when trim(multiplicityCounter) > '' then (0 + multiplicityCounter) end as multiplicityCounter
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
  , case when trim(recordNumberRecode) > '' then (0 + recordNumberRecode) end as recordNumberRecode
  , case when trim(qualityOfSurvival) > '' then trim(qualityOfSurvival) end as qualityOfSurvival
  , case when trim(survDateActiveFollowup) > '' then date(substr(survDateActiveFollowup, 1, 4) || '-' || coalesce(substr(survDateActiveFollowup, 5, 2), '01') || '-'
                 || coalesce(substr(survDateActiveFollowup, 7, 2), '01')) end as survDateActiveFollowup
  , case when trim(survFlagActiveFollowup) > '' then trim(survFlagActiveFollowup) end as survFlagActiveFollowup
  , case when trim(survMosActiveFollowup) > '' then (0 + survMosActiveFollowup) end as survMosActiveFollowup
  , case when trim(survDatePresumedAlive) > '' then date(substr(survDatePresumedAlive, 1, 4) || '-' || coalesce(substr(survDatePresumedAlive, 5, 2), '01') || '-'
                 || coalesce(substr(survDatePresumedAlive, 7, 2), '01')) end as survDatePresumedAlive
  , case when trim(survFlagPresumedAlive) > '' then trim(survFlagPresumedAlive) end as survFlagPresumedAlive
  , case when trim(survMosPresumedAlive) > '' then (0 + survMosPresumedAlive) end as survMosPresumedAlive
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
  , case when trim(rxHospRegLnRemoved) > '' then (0 + rxHospRegLnRemoved) end as rxHospRegLnRemoved
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
  , case when trim(eodTumorSize) > '' then (0 + eodTumorSize) end as eodTumorSize
  , case when trim(eodExtension) > '' then trim(eodExtension) end as eodExtension
  , case when trim(eodExtensionProstPath) > '' then trim(eodExtensionProstPath) end as eodExtensionProstPath
  , case when trim(eodLymphNodeInvolv) > '' then trim(eodLymphNodeInvolv) end as eodLymphNodeInvolv
  , case when trim(regionalNodesPositive) > '' then (0 + regionalNodesPositive) end as regionalNodesPositive
  , case when trim(regionalNodesExamined) > '' then (0 + regionalNodesExamined) end as regionalNodesExamined
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
  , case when trim(csTumorSize) > '' then (0 + csTumorSize) end as csTumorSize
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
  , case when trim(afpPostOrchiectomyLabValue) > '' then (0 + afpPostOrchiectomyLabValue) end as afpPostOrchiectomyLabValue
  , case when trim(afpPostOrchiectomyRange) > '' then trim(afpPostOrchiectomyRange) end as afpPostOrchiectomyRange
  , case when trim(afpPreOrchiectomyLabValue) > '' then (0 + afpPreOrchiectomyLabValue) end as afpPreOrchiectomyLabValue
  , case when trim(afpPreOrchiectomyRange) > '' then trim(afpPreOrchiectomyRange) end as afpPreOrchiectomyRange
  , case when trim(afpPretreatmentInterpretation) > '' then trim(afpPretreatmentInterpretation) end as afpPretreatmentInterpretation
  , case when trim(afpPretreatmentLabValue) > '' then (0 + afpPretreatmentLabValue) end as afpPretreatmentLabValue
  , case when trim(anemia) > '' then trim(anemia) end as anemia
  , case when trim(bSymptoms) > '' then trim(bSymptoms) end as bSymptoms
  , case when trim(bilirubinPretxTotalLabValue) > '' then trim(bilirubinPretxTotalLabValue) end as bilirubinPretxTotalLabValue
  , case when trim(bilirubinPretxUnitOfMeasure) > '' then trim(bilirubinPretxUnitOfMeasure) end as bilirubinPretxUnitOfMeasure
  , case when trim(boneInvasion) > '' then trim(boneInvasion) end as boneInvasion
  , case when trim(brainMolecularMarkers) > '' then trim(brainMolecularMarkers) end as brainMolecularMarkers
  , case when trim(breslowTumorThickness) > '' then trim(breslowTumorThickness) end as breslowTumorThickness
  , case when trim(ca125PretreatmentInterpretation) > '' then trim(ca125PretreatmentInterpretation) end as ca125PretreatmentInterpretation
  , case when trim(ceaPretreatmentInterpretation) > '' then trim(ceaPretreatmentInterpretation) end as ceaPretreatmentInterpretation
  , case when trim(ceaPretreatmentLabValue) > '' then (0 + ceaPretreatmentLabValue) end as ceaPretreatmentLabValue
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
  , case when trim(hcgPostOrchiectomyLabValue) > '' then (0 + hcgPostOrchiectomyLabValue) end as hcgPostOrchiectomyLabValue
  , case when trim(hcgPostOrchiectomyRange) > '' then trim(hcgPostOrchiectomyRange) end as hcgPostOrchiectomyRange
  , case when trim(hcgPreOrchiectomyLabValue) > '' then (0 + hcgPreOrchiectomyLabValue) end as hcgPreOrchiectomyLabValue
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
  , case when trim(ldhPretreatmentLabValue) > '' then (0 + ldhPretreatmentLabValue) end as ldhPretreatmentLabValue
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
  , case when trim(rxSummRegLnExamined) > '' then (0 + rxSummRegLnExamined) end as rxSummRegLnExamined
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
  , case when trim(phase1DosePerFraction) > '' then (0 + phase1DosePerFraction) end as phase1DosePerFraction
  , case when trim(phase1RadiationExternalBeamTech) > '' then trim(phase1RadiationExternalBeamTech) end as phase1RadiationExternalBeamTech
  , case when trim(phase1NumberOfFractions) > '' then (0 + phase1NumberOfFractions) end as phase1NumberOfFractions
  , case when trim(phase1RadiationPrimaryTxVolume) > '' then trim(phase1RadiationPrimaryTxVolume) end as phase1RadiationPrimaryTxVolume
  , case when trim(phase1RadiationToDrainingLN) > '' then trim(phase1RadiationToDrainingLN) end as phase1RadiationToDrainingLN
  , case when trim(phase1RadiationTreatmentModality) > '' then trim(phase1RadiationTreatmentModality) end as phase1RadiationTreatmentModality
  , case when trim(phase1TotalDose) > '' then (0 + phase1TotalDose) end as phase1TotalDose
  , case when trim(radRegionalDoseCgy) > '' then (0 + radRegionalDoseCgy) end as radRegionalDoseCgy
  , case when trim(phase2DosePerFraction) > '' then (0 + phase2DosePerFraction) end as phase2DosePerFraction
  , case when trim(phase2RadiationExternalBeamTech) > '' then trim(phase2RadiationExternalBeamTech) end as phase2RadiationExternalBeamTech
  , case when trim(phase2NumberOfFractions) > '' then (0 + phase2NumberOfFractions) end as phase2NumberOfFractions
  , case when trim(phase2RadiationPrimaryTxVolume) > '' then trim(phase2RadiationPrimaryTxVolume) end as phase2RadiationPrimaryTxVolume
  , case when trim(phase2RadiationToDrainingLN) > '' then trim(phase2RadiationToDrainingLN) end as phase2RadiationToDrainingLN
  , case when trim(phase2RadiationTreatmentModality) > '' then trim(phase2RadiationTreatmentModality) end as phase2RadiationTreatmentModality
  , case when trim(phase2TotalDose) > '' then (0 + phase2TotalDose) end as phase2TotalDose
  , case when trim(radNoOfTreatmentVol) > '' then (0 + radNoOfTreatmentVol) end as radNoOfTreatmentVol
  , case when trim(phase3DosePerFraction) > '' then (0 + phase3DosePerFraction) end as phase3DosePerFraction
  , case when trim(phase3RadiationExternalBeamTech) > '' then trim(phase3RadiationExternalBeamTech) end as phase3RadiationExternalBeamTech
  , case when trim(phase3NumberOfFractions) > '' then (0 + phase3NumberOfFractions) end as phase3NumberOfFractions
  , case when trim(phase3RadiationPrimaryTxVolume) > '' then trim(phase3RadiationPrimaryTxVolume) end as phase3RadiationPrimaryTxVolume
  , case when trim(phase3RadiationToDrainingLN) > '' then trim(phase3RadiationToDrainingLN) end as phase3RadiationToDrainingLN
  , case when trim(phase3RadiationTreatmentModality) > '' then trim(phase3RadiationTreatmentModality) end as phase3RadiationTreatmentModality
  , case when trim(phase3TotalDose) > '' then (0 + phase3TotalDose) end as phase3TotalDose
  , case when trim(radiationTxDiscontinuedEarly) > '' then trim(radiationTxDiscontinuedEarly) end as radiationTxDiscontinuedEarly
  , case when trim(numberPhasesOfRadTxToVolume) > '' then (0 + numberPhasesOfRadTxToVolume) end as numberPhasesOfRadTxToVolume
  , case when trim(totalDose) > '' then (0 + totalDose) end as totalDose
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
  , case when trim(radBoostDoseCgy) > '' then (0 + radBoostDoseCgy) end as radBoostDoseCgy
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
