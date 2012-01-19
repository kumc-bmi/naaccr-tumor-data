'''seer_recode -- parse the SEER Site Recode table
'''

import logging

from lxml import etree

log = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(level=logging.DEBUG)
    pgfn = argv[1]
    seer_parse(open(pgfn))


def seer_parse(fp):
    doc = etree.HTML(fp.read())
    log.debug('root elt: %s', doc.tag)
    for tr in doc.xpath('.//table/tr'):
        log.debug('row of size: %s', len(tr))


if __name__ == '__main__':
    import sys
    main(sys.argv)
