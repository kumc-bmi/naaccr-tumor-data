It works something like this:

```
naaccr-tumor-data/loinc-naaccr$ make
mdb-schema ~/Downloads/WIN10/RELMA.MDB sqlite | grep -v 'ALTER TABLE' >schema.sql
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB LOINC >LOINC.csv
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB LOINC_RELATED_CODE_LK >LOINC_RELATED_CODE_LK.csv
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB RELATED_CODE >RELATED_CODE.csv
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB ANSWER >ANSWER.csv
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB ANSWER_STRING >ANSWER_STRING.csv
mdb-export /home/dconnolly/Downloads/WIN10/RELMA.MDB LoincAnswerListLinks >LoincAnswerListLinks.csv
rm -f relma.db
sqlite3 relma.db <schema.sql
sqlite3 relma.db <load_tables.sql
sqlite3 relma.db <crosswalk.sql >loinc_naaccr.csv
sqlite3 relma.db <answers.sql >loinc_naaccr_answer.csv
```
