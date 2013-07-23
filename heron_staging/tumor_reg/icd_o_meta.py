'''icd_o_meta -- load (aka stage) ICD-O MetaData
'''

import csv
import logging

from sqlalchemy import MetaData, Table

import ocap_file

log = logging.getLogger(__name__)


def main(icd_o_2, icd_o_3, engine,
         level=logging.DEBUG):
    logging.basicConfig(level=level)

    meta = MetaData()

    f2 = ICDO2MetaFiles(icd_o_2)
    t2 = NightHeronICDO2(engine, meta)
    f3 = ICDO3MetaFiles(icd_o_3)
    t3 = NightHeronICDO3(engine, meta)

    t2.load(t2.morph, f2, f2.morph, has_header=False)
    t2.load(t2.topo, f2, f2.topo)

    t3.load(t3.morph, f3, f3.morph)


class MetaFiles(object):
    topo = 'Topoenglish.txt'

    def __init__(self, src):
        self.__src = src

    def data(self, file_name, has_header, fieldnames,
             sample_size=1000):
        filerd = self.__src.subRdFile(file_name)
        sample = filerd.inChannel().read(sample_size)
        s = csv.Sniffer()
        data = csv.DictReader(filerd.inChannel(),
                              fieldnames=fieldnames,
                              dialect=s.sniff(sample))
        if has_header:
            data.next()

        return list(data)


class ICDO2MetaFiles(MetaFiles):
    morph = 'icd-o-3-morph.csv'


class ICDO3MetaFiles(MetaFiles):
    morph = 'Morphenglish.txt'


class NightHeron(object):
    '''Schema and tables are assumed to exist:

    CREATE TABLE "WHO"."MORPH2"
    (
      "CODE"  VARCHAR2(20 BYTE) NOT NULL ENABLE,
      "LABEL" VARCHAR2(120 BYTE),
      "NOTES" VARCHAR2(120 BYTE),
      CONSTRAINT "MORPH2_PK" PRIMARY KEY ("CODE")
    );

    CREATE TABLE "WHO"."MORPH3"
    (
      "CODE"  VARCHAR2(20 BYTE) NOT NULL ENABLE,
      "LABEL" VARCHAR2(120 BYTE),
      "NOTES" VARCHAR2(120 BYTE)
    );

    CREATE TABLE "WHO"."TOPO"
    (
      "KODE"  VARCHAR2(20 BYTE) NOT NULL ENABLE,
      "LVL"   VARCHAR2(20 BYTE) NOT NULL ENABLE,
      "TITLE" VARCHAR2(80 BYTE)
    );
    '''
    who_schema = 'WHO'
    topo = 'TOPO'

    def __init__(self, engine, meta):
        self.meta = meta
        self.__engine = engine

    def load(self, table_name, mf, file_name,
             has_header=True):
        engine = self.__engine
        t = Table(table_name, self.meta,
                  autoload=True, autoload_with=engine,
                  schema=self.who_schema)

        data = mf.data(file_name, has_header,
                       [col.name for col in t.columns])
        log.info('deleting all rows in %s', t.name)
        engine.execute(t.delete())
        log.info('inserting %d rows into %s', len(data), t.name)
        engine.execute(t.insert(), data)


class NightHeronICDO2(NightHeron):
    morph = 'MORPH2'


class NightHeronICDO3(NightHeron):
    morph = 'MORPH3'


def kumc_url(db_creds, host, ssh_port,
             sid='KUMC'):
    u, pw = db_creds.inChannel().read().split()
    return 'oracle://%s:%s@%s:%d/%s' % (
        u, pw, host, ssh_port, sid)


if __name__ == '__main__':
    def _initial_caps():
        import sys
        import os
        from sqlalchemy import create_engine

        def open_rd_universal(path):
            return file(path, 'rU')

        def rd(n):
            return ocap_file.Readable(os.path.abspath(n),
                                      os.path, os.listdir,
                                      open_rd_universal)

        icd_o_2_dir, icd_o_3_dir, db_creds, host, ssh_port, sid = sys.argv[1:7]

        return dict(icd_o_2=rd(icd_o_2_dir),
                    icd_o_3=rd(icd_o_3_dir),
                    engine=create_engine(
                        kumc_url(rd(db_creds), host, int(ssh_port), sid)))

    main(**_initial_caps())
