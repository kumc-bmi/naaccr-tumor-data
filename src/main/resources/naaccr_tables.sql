drop table if exists naaccr_tumors;
drop table if exists naaccr_pmap;
drop table if exists naaccr_patients;
drop table if exists naaccr_observations;
drop table if exists naaccr_export_stats;

--------------------------------------------------------
--  DDL for Table NAACCR_TUMORS
--------------------------------------------------------

create table naaccr_tumors (
  tumor_id integer primary key,
  naaccrRecordVersion varchar(3),
  npiRegistryId varchar(10),
  tumorRecordNumber integer,
  patientIdNumber varchar(8),
  patientSystemIdHosp varchar(8),
  sex varchar(1),
  ageAtDiagnosis integer,
  dateOfBirth date,
  sequenceNumberCentral varchar(2),
  dateOfDiagnosis date not null,
  primarySite varchar(4) not null,
  accessionNumberHosp varchar(9),
  sequenceNumberHospital varchar(2),
  dateOfInptAdm date,
  dateOfInptDisch date,
  classOfCase varchar(2),
  dateCaseInitiated date,
  dateCaseCompleted date,
  dateCaseLastchanged date,
  dateDaseReportExported date,
  dateOfLastContact date,
  vitalStatus varchar(1),
  encounter_num integer,
  task_id varchar(255)
);


--------------------------------------------------------
--  DDL for Table NAACCR_PMAP
--------------------------------------------------------

create table naaccr_pmap (
  patientIdNumber varchar(32) not null
);


--------------------------------------------------------
--  DDL for Table NAACCR_PATIENTS
--------------------------------------------------------

create table naaccr_patients (
  naaccrRecordVersion varchar(3),
  npiRegistryId varchar(10),
  patientIdNumber varchar(8) not null,
  patientSystemIdHosp varchar(8),
  sex varchar(1) not null,
  dateOfBirth date not null,
  dateCaseReportExported date,
  dateOfLastContact date,
  vitalStatus varchar(1),
  patient_num integer,
  task_id varchar(255)
);

--------------------------------------------------------
--  DDL for Index NAACCR_PATIENTS_PK
--------------------------------------------------------

create index naaccr_patients_pk on naaccr_patients (patientIdNumber);


--------------------------------------------------------
--  DDL for Table NAACCR_OBSERVATIONS
--------------------------------------------------------

create table naaccr_observations (
  tumroId integer not null,
  patientIdNumber varchar(8) not null,
  naaccrId varchar(64) not null,
  concept_cd varchar(50) not null,
  provider_id varchar(50),
  start_date date not null,
  modifier_cd varchar(50),
  instance_num integer,
  valtype_cd varchar(50) not null,
  tval_char varchar(255),
  nval_num numeric(19,4),
  valueflag_cd varchar(32),
  units_cd varchar(32),
  end_date DATE,
  location_cd varchar(50),
  update_date date not null,
  task_id varchar(255)
);


--------------------------------------------------------
--  DDL for Table NAACCR_EXPORT_STATS
--------------------------------------------------------

create table naaccr_export_stats (
  sectionId integer not null,
  section varchar(255),
  dx_yr integer not null,
  naaccrId varchar(64) not null,
  naaccrNum integer not null,
  valtype_cd varchar(50) not null,
  mean numeric(19, 4),
  sd numeric(19, 4),
  value varchar(255),
  freq integer,
  present integer not null,
  tumor_qty integer not null,
  task_id varchar(255)
);
