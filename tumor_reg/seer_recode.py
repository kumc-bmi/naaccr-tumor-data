'''seer_recode -- parse the SEER Site Recode table
'''

import logging
from collections import namedtuple
import itertools
import csv

from lxml import etree

log = logging.getLogger(__name__)


def main(argv):
    logging.basicConfig(level=logging.INFO)
    pgfn, termfn, rulesfn = argv[1:4]

    terms, rules = seer_parse(open(pgfn))

    open(rulesfn, 'w').write('case\n' + '\n'.join(rules)
                             + "\n/* Invalid */ else '9999'\nend")
    write_csv(open(termfn, 'w'), Term, terms)


Term = namedtuple('Term', 'hlevel path name basecode visualattributes')


def seer_parse(fp):
    doc = etree.HTML(fp.read())
    tbl1 = doc.xpath('.//table')[0]

    terms = []
    parents = []
    prev_label = None

    rules = []
    hist_span = 0
    icdo3histology = None
    recode_span = 0
    recode = None

    for site_row in tbl1:
        icdo3site, icdo3histology, recode, hist_span, recode_span = grok_row(
            site_row, icdo3histology, recode, hist_span, recode_span)
        indent, label = build_site_tree(site_row, prev_label, parents)
        if recode and label != 'Invalid':
            rules.append("/* %s */ when %s then '%s'\n" % (
                label,
                mk_rule(icdo3site, icdo3histology),
                recode))

        if label:
            log.debug('term: %s', (recode, label, parents))
            terms.append(
                Term(hlevel=len(parents),
                     path='\\'.join([seg[:20] for i, seg in parents]
                                    + [label[:20]]),
                     name=label,
                     basecode=recode,
                     visualattributes='LA' if recode else 'FA'))

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

    return indent, label or prev_label


def grok_row(site_row, icdo3histology, recode, hist_span, recode_span):
    '''
    >>> grok_row(etree.fromstring(TEST_ROW_LIP), 'h1', 'r1', 1, 1)
    ... #doctest: +NORMALIZE_WHITESPACE
    ('C000-C009', 'excluding 9590-9989, and sometimes 9050-9055, 9140',
     '20010', 10, 1)
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


def mk_rule(sites, histologies, site_col='site', hist_col='histology'):
    return '\n  and '.join(mk_clause(ranges(sites), site_col)
                           + mk_clause(ranges(histologies), hist_col))


def mk_clause(eb, col):
    excl, bounds = eb
    if not bounds:
        return []
    ground = [(("%s between '%s' and '%s'" % (col, lo, hi))
               if hi else
               "%s = '%s'" % (col, lo))
               for lo, hi in bounds]
    return [((' not ' if excl else '')
             + '('
             + '\n   or '.join(ground)
             + ')')]
                

    
def ranges(txt, sometimes=True):
    '''
    >>> ranges('')
    (False, [])
    >>> ranges('C530-C539', False)
    (False, [('C530', 'C539')])
    >>> ranges('excluding 9590-9989, and sometimes 9050-9055, 9140')
    (True, [('9590', '9989'), ('9050', '9055'), ('9140', None)])
    >>> ranges('excluding 9590-9989, and sometimes 9050-9055, 9140',
    ...        sometimes=False)
    (True, [('9590', '9989')])
    >>> ranges('All sites except C024, C098-C099, C111, C142, '
    ...        'C379, C422, C770-C779')
    ... #doctest: +NORMALIZE_WHITESPACE
    (True,
     [('C024', None), ('C098', 'C099'), ('C111', None),
      ('C142', None), ('C379', None), ('C422', None), ('C770', 'C779')])

    '''
    exc = [pfx for pfx in ('All sites except ', 'excluding ')
           if txt.startswith(pfx)]
    atoms = txt[len(exc[0]):] if exc else txt
    ti0 = atoms.split(', and sometimes ')
    t1 = ','.join(ti0 if sometimes else ti0[:1])
    hilos = [t.strip() for t in t1.split(',')]
    return (len(exc) > 0,
            [(lo, hi) for lo, hi in
             [t.split('-') if '-' in t else [t, None] for t in hilos if t]])


def flatten(lists):
    return list(itertools.chain(lists))


def write_csv(fp, klass, items):
    fields = klass._fields
    o = csv.DictWriter(fp, fields, lineterminator='\n')
    o.writerow(dict(zip(fields, fields)))
    o.writerows([item._asdict() for item in items])


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
