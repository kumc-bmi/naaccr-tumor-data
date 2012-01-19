'''seer_recode -- parse the SEER Site Recode table
'''

import logging
import pprint

from lxml import etree

log = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(level=logging.INFO)
    pgfn = argv[1]

    terms, rules = seer_parse(open(pgfn))

    log.info('terms:\n%s', pprint.pformat(terms))
    log.info('rules:\n%s', pprint.pformat(rules))


def seer_parse(fp):
    doc = etree.HTML(fp.read())
    tbl1 = doc.xpath('.//table')[0]

    terms = []
    parents = []
    prev_label = None
    term_id = 0

    rules = []
    hist_span = 0
    icdo3histology = None
    recode_span = 0
    recode = None

    for site_row in tbl1:
        indent, label = build_site_tree(site_row, prev_label, parents)
        if label:
            term_id += 1
            log.debug('term: %s', (term_id, label, parents))
            terms.append((term_id, label, [l for i, l in parents]))

        icdo3site, icdo3histology, recode, hist_span, recode_span = grok_row(
            site_row, icdo3histology, recode, hist_span, recode_span)
        if recode:
            rules.append((term_id, label, icdo3site, icdo3histology, recode))

        prev_label = label

    return terms, rules


def build_site_tree(site_row, prev_label, parents):
    '''
    >>> build_site_tree(etree.fromstring(TEST_ROW_LIP), None, [])
    ... #doctest: +NORMALIZE_WHITESPACE
    (23, 'Lip')
    '''
    tds = site_row.xpath('td[@headers="site"]')
    if tds:
        site_td = tds[0]
    else:
        return None, None

    indent_txt = site_td.xpath('table/tr/td')[0].text
    indent = len(indent_txt) if indent_txt else 0
    label = t(site_td.xpath('table/tr/td')[1])

    while parents and indent < parents[-1][0]:
        parents.pop()
    if indent > (parents[-1][0] if parents else 0):
        parents.append((indent, prev_label))

    return indent, label


def grok_row(site_row, icdo3histology, recode, hist_span, recode_span):
    '''
    >>> grok_row(etree.fromstring(TEST_ROW_LIP), 1, None)
    ... #doctest: +NORMALIZE_WHITESPACE
    ('C000-C009', 'excluding 9590-9989, and sometimes 9050-9055, 9140',
     '20010', 10)
    '''
    if not site_row.xpath('td[@headers]'):
        log.debug('not a recode row: %s', etree.tostring(site_row))
        return None, None, None, None, None

    def td(n):
        tds = [td for td in site_row if n in td.attrib['headers']]
        return tds[0] if tds else None

    def spanning(span, txt, n):
        if span > 1:
            span -= 1
        else:
            n_td = td(n)
            txt = t(n_td)
            if n_td is not None:
                span = int(n_td.attrib.get('rowspan', '1'))
        return span, txt

    hist_span, icdo3histology = spanning(hist_span,
                                         icdo3histology, 'icdo3histology')
    recode_span, recode = spanning(recode_span,
                                   recode, 'recode')

    return (t(td('icdo3site')), icdo3histology, recode, hist_span, recode_span)


def t(elt):
    return ' '.join(elt.text.split()) if elt is not None else None


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
