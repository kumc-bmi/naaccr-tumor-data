'''icd_o_meta -- load (aka stage) ICD-O MetaData
'''

import csv
import logging

from sqlalchemy import MetaData, Table

import ocap_file

log = logging.getLogger(__name__)


def main(icd_o_2, icd_o_3, engine,
         morph_n='icd-o-3-morph.csv', topo_n='Topoenglish.txt',
         who_schema='WHO', morph2='MORPH2',
         level=logging.DEBUG):
    '''
    @param morph2: table for loading ICD-O-2 Morphology metadata.
                   Assumed to exist.
    '''
    logging.basicConfig(level=level)

    morph = csv.reader(icd_o_2.subRdFile(morph_n).inChannel())
    log.debug(morph.next())

    meta = MetaData()
    morph2 = Table(morph2, meta,
                   autoload=True, autoload_with=engine,
                   schema=who_schema)
    log.debug('%s', morph2.select())

    raise NotImplementedError


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
