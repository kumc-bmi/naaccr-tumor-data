# install Relma from loinc.org; then look in Users\Public
RELMA_DB=~/Downloads/WIN10/RELMA.MDB
# from mdb-tools (debian / ubuntu package)
MDB_EXPORT=mdb-export
MDB_SCHEMA=mdb-schema
# from sqlite3 (debian / ubuntu package)
SQLITE=sqlite3

all: loinc_naaccr.csv loinc_naaccr_answer.csv

loinc_naaccr.csv: crosswalk.sql relma.db
	$(SQLITE) relma.db <crosswalk.sql >$@

loinc_naaccr_answer.csv: answers.sql relma.db
	$(SQLITE) relma.db <answers.sql >$@

relma.db: schema.sql load_tables.sql \
		LOINC.csv LOINC_RELATED_CODE_LK.csv RELATED_CODE.csv \
		ANSWER.csv ANSWER_STRING.csv LoincAnswerListLinks.csv
	rm -f relma.db
	$(SQLITE) $@ <schema.sql
	$(SQLITE) $@ <load_tables.sql

schema.sql: $(RELMA_DB)
	$(MDB_SCHEMA) $(RELMA_DB) sqlite | grep -v 'ALTER TABLE' >$@

LOINC.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< LOINC >$@

LOINC_RELATED_CODE_LK.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< LOINC_RELATED_CODE_LK >$@

RELATED_CODE.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< RELATED_CODE >$@

ANSWER.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< ANSWER >$@

ANSWER_STRING.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< ANSWER_STRING >$@

LoincAnswerListLinks.csv: $(RELMA_DB)
	$(MDB_EXPORT) $< LoincAnswerListLinks >$@

clean:
	rm -f relma.db schema.sql *~ [A-Z]*.csv loinc_naaccr.csv


