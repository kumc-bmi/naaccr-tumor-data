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
    t1 = doc.xpath('.//table')[0]
    log.debug('rows in main table: %s', len(t1))

    parents = []
    prev_label = None
    for site_row in t1.xpath('tr[td/@headers="site"]'):
        indent_txt = site_row.xpath('td[1]/table/tr/td')[0].text
        indent = len(indent_txt) if indent_txt else 0
        label = site_row.xpath('td[1]/table/tr/td')[1].text.strip()

        while parents and indent < parents[-1][0]:
            parents.pop()
        if indent > (parents[-1][0] if parents else 0):
            parents.append((indent, prev_label))

        log.debug('site: %s %s\n%s', '*' * indent, label, parents)

        prev_label = label

    for tr in doc.xpath('.//table/tr'):
        log.debug('row of size: %s', len(tr))
        log.debug('cell size: %s',
                  [(td.attrib.get('colspan', 1),
                    td.attrib.get('rowspan', 1))
                   for td in tr])
        log.debug('cell text: %s',
                  [td.text for td in tr])


if __name__ == '__main__':
    import sys
    main(sys.argv)
