create table section (
  sectionid int,
  section text
);

create table record_layout (
  start int,
  end int,
  length int,
  item int,
  name text,
  xmlId text,
  parentTag text,
  section text,
  note text
);

create table data_descriptor (
  item int,
  name text,
  format text,
  allow_value text,
  length int,
  note text
);

create table item_description (
  item text,
  xmlId text,
  parentTag text,
  description text
);
