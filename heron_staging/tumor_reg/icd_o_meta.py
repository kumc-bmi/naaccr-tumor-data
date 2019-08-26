'''icd_o_meta -- load (aka stage) ICD-O MetaData

To load CSV files from two folders into db1:

  >>> usage = 'icd_o_meta.py ICD-O-2_CSV ICD-O-3_CSV-meta creds host1 1521 db1'  # noqa

  >>> io = MockIO()
  >>> main(usage.split(), io.cwd(), io.create_engine)

  >>> io._engine.execute('select * from who.topo').fetchall() # doctest: +NORMALIZE_WHITESPACE
  [('C00', '3', 'LIP'),
   ('C00.0', 'incl', 'Upper lip, NOS'),
   ('C00.0', 'incl', 'skin of upper lip')]

See `class:MetaFiles` for details about downloading the
ICD-O-... files from WHO.

See `class:NightHeron` for assumptions about the WHO schema and tables.
'''

from io import StringIO
import csv
import logging

from sqlalchemy import MetaData, Table

log = logging.getLogger(__name__)


def main(argv, cwd, create_engine):
    icd_o_2_dir, icd_o_3_dir, db_creds, host, ssh_port, sid = argv[1:7]

    engine = create_engine(kumc_url(cwd / db_creds, host, int(ssh_port), sid))

    meta = MetaData()

    f2, f3 = [cls(cwd / fn)
              for (cls, fn) in [(ICDO2MetaFiles, icd_o_2_dir),
                                (ICDO3MetaFiles, icd_o_3_dir)]]
    t2, t3 = [cls(engine, meta)
              for cls in [NightHeronICDO2,
                          NightHeronICDO3]]

    load(f2, t2, t2.topo, f2.topo)
    load(f2, t2, t2.morph, f2.morph, has_header=False)
    load(f3, t3, t3.morph, f3.morph)


def load(src, dest, tname, fname, has_header=True):
    dest.load(tname, src.data(fname, has_header, dest.columns(tname)))


class MetaFiles(object):
    r'''Oncology MetaFiles from the World Health Organization `materials`__
    __ http://www.who.int/classifications/icd/adaptations/oncology/en/index.html # noqa

    tested with:
    b088c4e4bd2d685c9dd04e3b3c14c98b ICD-O-3_CSV-metadata.zip
    1308ce6f4ef93c67137154cc6a723fc6 ICD-O-2_CSV.zip
    '''
    topo = 'Topoenglish.txt'

    topo_sample = '''
Kode\tLvl\tTitle
C00\t3\tLIP
C00.0\tincl\t"Upper lip, NOS"
C00.0\tincl\tskin of upper lip
    '''.strip() + '\n'

    def __init__(self, src):
        self.__src = src

    def data(self, file_name, has_header, fieldnames,
             sample_size=1000):
        filerd = self.__src / file_name
        sample = filerd.open().read(sample_size)
        s = csv.Sniffer()
        data = csv.DictReader(filerd.open(),
                              fieldnames=fieldnames,
                              dialect=s.sniff(sample))
        if has_header:
            next(data)

        return list(dict(r) for r in data)


class ICDO2MetaFiles(MetaFiles):
    morph = 'icd-o-3-morph.csv'

    sample = '''
M800,Neoplasms NOS,
M8000/0,"Neoplasm, benign",
M8000/1,"Neoplasm, uncertain whether benign or malignant",
    '''.strip() + '\n'


class ICDO3MetaFiles(MetaFiles):
    morph = 'Morphenglish.txt'

    sample = '''
Code    Struct  Label
8000/0  title   "Neoplasm, benign"
8000/0  sub     "Tumor, benign"
    '''.strip() + '\n'


class NightHeron(object):
    '''Schema and tables are assumed to exist. See `schema`.'''

    schema = '''
    CREATE TABLE "WHO"."MORPH2"
    (
      "CODE"  VARCHAR2(20) NOT NULL,
      "LABEL" VARCHAR2(120),
      "NOTES" VARCHAR2(120),
      CONSTRAINT "MORPH2_PK" PRIMARY KEY ("CODE")
    );

    CREATE TABLE "WHO"."MORPH3"
    (
      "CODE"  VARCHAR2(20) NOT NULL,
      "LABEL" VARCHAR2(120),
      "NOTES" VARCHAR2(120)
    );

    CREATE TABLE "WHO"."TOPO"
    (
      "KODE"  VARCHAR2(20) NOT NULL,
      "LVL"   VARCHAR2(20) NOT NULL,
      "TITLE" VARCHAR2(80)
    );
    '''
    who_schema = 'WHO'
    topo = 'TOPO'

    def __init__(self, engine, meta):
        self.meta = meta
        self.__engine = engine

    def _table(self, table_name):
        return Table(table_name, self.meta,
                     autoload=True, autoload_with=self.__engine,
                     schema=self.who_schema)

    def columns(self, table_name):
        t = self._table(table_name)
        return [col.name for col in t.columns]

    def load(self, table_name, data):
        engine = self.__engine
        t = Table(table_name, self.meta,
                  autoload=True, autoload_with=engine,
                  schema=self.who_schema)

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
    u, pw = db_creds.open().read().split()
    return 'oracle://%s:%s@%s:%d/%s' % (
        u, pw, host, ssh_port, sid)


class MockIO(object):
    files = {
        f'./{d}/{base}': content
        for d, mf in [
                ('ICD-O-2_CSV', ICDO2MetaFiles),
                ('ICD-O-3_CSV-meta', ICDO3MetaFiles),
        ]
        for base, content in [
            (mf.topo, mf.topo_sample),
            (mf.morph, mf.sample)
        ]
    }
    files.update({'./creds': 'u\np'})

    def __init__(self, files=None,
                 path=None):
        if files is None:
            files = MockIO.files
        self._files = files
        self._path = path
        self._engine = None

    def cwd(self):
        return MockIO(path='.')

    def open(self):
        return StringIO(self._files[self._path])

    def __truediv__(self, other):
        return self.pathjoin(other)

    def pathjoin(self, other):
        from posixpath import join
        return MockIO(path=join(self._path or '.', other))

    def create_engine(self, url):
        from sqlalchemy import create_engine
        e = create_engine('sqlite://')
        e.execute("attach database ':memory:' as 'WHO'")
        for cmd in NightHeron.schema.split(';'):
            e.execute(cmd)
        self._engine = e
        return e


if __name__ == '__main__':
    def _script_io():
        from pathlib import Path
        from sys import argv
        from sqlalchemy import create_engine

        logging.basicConfig(level=logging.INFO)

        main(argv, Path('.'), create_engine)

    _script_io()
