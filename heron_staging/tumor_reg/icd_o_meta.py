'''icd_o_meta -- load (aka stage) ICD-O MetaData

Usage::
  >>> print 'python', ' '.join(Mock.caps()['argv'])
  python icd_o_meta.py ICD-O-2_CSV ICD-O-3_CSV-meta creds host1 1521 db1

See `class:MetaFiles` for details about downloading the
ICD-O-... files from WHO.

See `class:NightHeron` for assumptions about the WHO schema and tables.
'''

import StringIO
import csv
import logging

from sqlalchemy import MetaData, Table

import ocap_file

log = logging.getLogger(__name__)


def main(argv, argv_rd, mk_engine,
         level=logging.INFO):
    '''command-line interface; see also Mock() and _initial_caps().

    Ignore SQLite warnings about VARCHAR2.
    >>> from warnings import catch_warnings
    >>> with catch_warnings(record=True):
    ...     main(**Mock.caps())
    '''
    logging.basicConfig(level=level)

    icd_o_2_dir, icd_o_3_dir, db_creds, host, ssh_port, sid = argv[1:7]

    engine = mk_engine(3, db_creds, host, int(ssh_port), sid)
    meta = MetaData()

    f2, f3 = [cls(argv_rd(fn))
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
    __ http://www.who.int/classifications/icd/adaptations/oncology/en/index.html #noqa

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
        r'''
        >>> schema_line = MetaFiles.topo_sample.split('\n')[0]

        >>> mf = MetaFiles(Mock([(MetaFiles.topo, MetaFiles.topo_sample)]))
        >>> d = mf.data(MetaFiles.topo, has_header=True,
        ...             fieldnames=schema_line.split())
        >>> import pprint; pprint.pprint(d)
        [{'Kode': 'C00', 'Lvl': '3', 'Title': 'LIP'},
         {'Kode': 'C00.0', 'Lvl': 'incl', 'Title': 'Upper lip, NOS'},
         {'Kode': 'C00.0', 'Lvl': 'incl', 'Title': 'skin of upper lip'}]
        '''
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
    u, pw = db_creds.inChannel().read().split()
    return 'oracle://%s:%s@%s:%d/%s' % (
        u, pw, host, ssh_port, sid)


class Mock(object):
    def __init__(self, items):
        self._items = items

    def subRdFile(self, name):
        text = [itxt for (n, itxt) in self._items
                if n == name][0]
        return ocap_file.Readable(n, None, None,
                                  lambda n: StringIO.StringIO(text))

    @classmethod
    def dir_for(cls, mf):
        return cls([(mf.topo, mf.topo_sample),
                    (mf.morph, mf.sample)])

    @classmethod
    def caps(cls):
        files = [('ICD-O-2_CSV', Mock.dir_for(ICDO2MetaFiles)),
                 ('ICD-O-3_CSV-meta', Mock.dir_for(ICDO3MetaFiles))]

        argv = ['icd_o_meta.py'] + [n for (n, _) in files] + [
            'creds', 'host1', '1521', 'db1']

        return dict(argv=argv[:],
                    argv_rd=lambda n: dict(files)[n],
                    mk_engine=cls.engine)

    @classmethod
    def engine(cls, ix, creds, host, port, sid):
        import sqlalchemy
        e = sqlalchemy.create_engine('sqlite://')
        e.execute("attach database ':memory:' as 'WHO'")
        for cmd in NightHeron.schema.split(';'):
            e.execute(cmd)
        return e


if __name__ == '__main__':
    def _initial_caps():
        from sys import argv
        from os import path, listdir
        from sqlalchemy import create_engine

        def open_rd_universal(path):
            return file(path, 'rU')

        def rd(n):
            return ocap_file.Readable(path.abspath(n),
                                      path, listdir,
                                      open_rd_universal)

        def arg_rd(arg):
            if not arg in argv:
                raise IOError('not a CLI arg: %s', arg)
            return rd(arg)

        def mk_engine(ix, cred_fn, host, port, sid):
            if not argv[ix:ix + 4] == [cred_fn, host, str(port)]:
                raise IOError('does not match CLI args: %s' % [
                        ix, cred_fn, host, port, sid])
            return create_engine(
                kumc_url(rd(cred_fn), host, port, sid))

        return dict(argv=argv[:],
                    arg_rd=arg_rd,
                    mk_engine=mk_engine)

    main(**_initial_caps())
