''' import_csv -- import CSV data (e.g. from MS Access) into, e.g. Oracle

Usage::

  > python import_csv.py SCHEMA TABLE INPUT.csv \
     'oracle://USER:PASSWORD@HOST:PORT/' 2>,debug_log


'''
import csv
import sys
import logging

import sqlalchemy
from   sqlalchemy.dialects import oracle

def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    sl = logging.getLogger('sqlalchemy')
    sl.setLevel(logging.DEBUG)
    
    schemaname, tablename, infn, dbaddr = sys.argv[1:5]

    rd = csv.DictReader(open(infn))
    e = sqlalchemy.create_engine(dbaddr)

    # print e.execute('select 1+1 from dual').fetchall()

    md = sqlalchemy.MetaData(e)
    rows = list(rd)
    lengths = dict([(fieldname, max([len(row[fieldname])
                                     for row in rows]))
                    for fieldname in rd.fieldnames])
    cols = [sqlalchemy.Column(fieldname,
                              oracle.VARCHAR2(lengths[fieldname]+1)
                              if lengths[fieldname] < 1000
                              else oracle.CLOB)
            for fieldname in rd.fieldnames]
    t = sqlalchemy.Table(tablename, md, schema=schemaname, *cols)

    try:
        t.drop(bind=e)
    except:
        pass
    t.create(bind=e)

    conn = e.connect()
    tx = conn.begin()
    for row in rows:
        conn.execute(t.insert(values=row))
    tx.commit()
    conn.close()


if __name__ == '__main__':
    main()
