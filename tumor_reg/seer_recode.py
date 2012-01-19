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
    rowspan = 0
    icdo3histology = None

    for site_row in t1.xpath('tr[td/@headers="site"]'):
        icdo3site, icdo3histology, recode, rowspan = grok_row(
            site_row, rowspan, icdo3histology)

        indent, label = build_site_tree(site_row, prev_label, parents)

        log.debug('site: %s %s\n%s', '*' * indent, label, parents)
        log.debug('recode: %s, %s -> %s', icdo3site, icdo3histology, recode)

        prev_label = label


def build_site_tree(site_row, prev_label, parents):
    '''
    >>> build_site_tree(etree.fromstring(TEST_ROW_LIP), None, [])
    ... #doctest: +NORMALIZE_WHITESPACE
    (23, 'Lip')
    '''
    indent_txt = site_row.xpath('td[1]/table/tr/td')[0].text
    indent = len(indent_txt) if indent_txt else 0
    label = site_row.xpath('td[1]/table/tr/td')[1].text.strip()

    while parents and indent < parents[-1][0]:
        parents.pop()
    if indent > (parents[-1][0] if parents else 0):
        parents.append((indent, prev_label))

    return indent, label


def grok_row(site_row, rowspan, icdo3histology):
    '''
    >>> grok_row(etree.fromstring(TEST_ROW_LIP), 1, None)
    ... #doctest: +NORMALIZE_WHITESPACE
    ('C000-C009', 'excluding 9590-9989, and sometimes 9050-9055, 9140',
     '20010', 10)
    '''
    def td(n):
        tds = [td for td in site_row if n in td.attrib['headers']]
        return tds[0] if tds else None

    def t(elt):
        return ' '.join(elt.text.split()) if elt else None

    if rowspan > 1:
        rowspan -= 1
    else:
        hist_td = td('icdo3histology')
        icdo3histology = t(hist_td)
        if hist_td:
            rowspan = int(hist_td.attrib.get('rowspan', '1'))

    return (t(td('icdo3site')), icdo3histology, t(td('recode')), rowspan)


TEST_ROW_LIP = '''
          <tr>
            <td headers='site' id="h2" valign="baseline">
              <table border='0' cellpadding='0' cellspacing='0'>
                <tr>
                  <td valign="baseline">
                  &#160;&#160;&#160;&#160;</td>

                  <td valign="baseline">Lip</td>
                </tr>
              </table>
            </td>

            <td headers="icdo3site h2" valign="baseline">
            C000-C009</td>

            <td headers="icdo3histology h2" valign="baseline"
            rowspan="10">excluding 9590-9989, and sometimes
            9050-9055, 9140<a href='#_+'><span class=
            'recodefootnote'>+</span></a></td>

            <td headers="recode h2" valign="baseline">20010</td>
          </tr>
'''


if __name__ == '__main__':
    import sys
    main(sys.argv)
