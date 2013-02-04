'''icd_o_meta -- load (aka stage) ICD-O MetaData
'''

import csv
import logging

import ocap_file

log = logging.getLogger(__name__)


def main(icd_o_2, icd_o_3,
         morph_n='icd-o-3-morph.csv', topo_n='Topoenglish.txt',
         level=logging.DEBUG):
    logging.basicConfig(level=level)

    morph = csv.reader(icd_o_2.subRdFile(morph_n).inChannel())
    log.debug(morph.next())

    raise NotImplementedError


if __name__ == '__main__':
    def _initial_caps():
        import sys
        import os

        def open_rd_universal(path):
            return file(path, 'rU')

        def rd(n):
            return ocap_file.Readable(os.path.abspath(n),
                                      os.path, os.listdir,
                                      open_rd_universal)
        icd_o_2_dir, icd_o_3_dir = sys.argv[1:3]
        return dict(icd_o_2=rd(icd_o_2_dir),
                    icd_o_3=rd(icd_o_3_dir))

    main(**_initial_caps())
