'''icd_o_meta -- load (aka stage) ICD-O MetaData
'''

import csv
import logging

from sqlalchemy import MetaData, Table

import ocap_file

log = logging.getLogger(__name__)


def main(icd_o_2, icd_o_3, engine,
         morph_n='icd-o-3-morph.csv', topo_n='Topoenglish.txt',
         who_schema='WHO', morph2='MORPH2', topo2='TOPO2',
         level=logging.DEBUG):
    '''
    @param morph2: table for loading ICD-O-2 Morphology metadata.
                   Assumed to exist.
    '''
    logging.basicConfig(level=level)

    meta = MetaData()
    load_table(morph2, who_schema, icd_o_2, morph_n, meta, engine)
    load_table(topo2, who_schema, icd_o_2, topo_n, meta, engine,
               has_header=True)

    raise NotImplementedError


def load_table(table_name, schema, src, file_name, meta, engine,
               has_header=False,
               sample_size=1000):
    t = Table(table_name, meta,
              autoload=True, autoload_with=engine,
              schema=schema)

    filerd = src.subRdFile(file_name)
    sample = filerd.inChannel().read(sample_size)
    s = csv.Sniffer()
    data = csv.DictReader(filerd.inChannel(),
                          fieldnames=[c.name for c in t.columns],
                          dialect=s.sniff(sample))
    if has_header:
        data.next()

    engine.execute(t.delete())
    rows = list(data)
    engine.execute(t.insert(), rows)


def kumc_engine(create_engine, db_creds, ssh_port,
                sid='KUMC'):
    u, pw = db_creds.inChannel().read().split()
    url = 'oracle://%s:%s@%s:%d/%s' % (
        u, pw, 'localhost', ssh_port, sid)
    return create_engine(url)


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

        icd_o_2_dir, icd_o_3_dir, db_creds, ssh_port = sys.argv[1:5]

        return dict(icd_o_2=rd(icd_o_2_dir),
                    icd_o_3=rd(icd_o_3_dir),
                    engine=kumc_engine(create_engine,
                                       rd(db_creds),
                                       int(ssh_port)))

    main(**_initial_caps())
