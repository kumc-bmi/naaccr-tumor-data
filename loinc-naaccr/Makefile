# install Relma from loinc.org; then look in Users\Public
RELMA_DB=~/Downloads/WIN10/RELMA.MDB
# from mdb-tools (debian / ubuntu package)
MDB_EXPORT=mdb-export
MDB_SCHEMA=mdb-schema
# from sqlite3 (debian / ubuntu package)
SQLITE=sqlite3

loinc_naaccr.csv: crosswalk.sql relma.db
	$(SQLITE) relma.db <crosswalk.sql >$@

relma.db: schema.sql load_tables.sql \
		LOINC.csv LOINC_RELATED_CODE_LK.csv RELATED_CODE.csv
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

clean:
	rm -f relma.db schema.sql *~ LOINC*.csv RELATED*.csv loinc_naaccr.csv


