'''seer_recode -- parse the SEER Site Recode table

The National Cancer Institutute provides the `Site Recode ICD-O-3/WHO
2008 Definition`__ in an `ASCII text file`__ in this format:

Site Group;ICD-O-3 Site;ICD-O-3 Histology (Type);Recode
Digestive System; ; ; ;
    Stomach;C160-C169;excluding 9050-9055, 9140, 9590-9992;21020;
    Liver and Intrahepatic Bile Duct; ; ; ;
        Liver;C220;excluding 9050-9055, 9140, 9590-9992;21071;
Breast;C500-C509;excluding 9050-9055, 9140, 9590-9992;26000;
Brain and Other Nervous System; ; ;  ;
    Brain;C710-C719;excluding 9050-9055, 9140, 9530-9539, 9590-9992;31010;
    Cranial Nerves Other ...;C710-C719;9530-9539;31040 ;
    Cranial Nerves Other ...;C700-C709, C720-C729;excluding 9050-... ;31040 ;
Invalid;Site or histology code not within valid range ...;;99999;

From this, we extract
  1. A table of paths and such for building i2b2 terms
  2. A SQL expression for computing the recode based on
     primary site and histology.

Usage:

  python seer_recode.py index.txt seer_recode_terms.csv seer_recode.sql

__ http://seer.cancer.gov/siterecode/icdo3_dwhoheme/index.html
__ http://seer.cancer.gov/siterecode/icdo3_dwhoheme/index.txt

'''

import logging
from collections import namedtuple
from itertools import dropwhile, takewhile
import csv

log = logging.getLogger(__name__)


def main(arg_rd, arg_wr):
    with arg_rd(1) as page:
        rules = Rule.from_lines(page)

    with arg_wr(2) as termf:
        write_csv(termf, Term, Rule.as_terms(rules))

    with arg_wr(3) as rulef:
        rulef.write('case\n' + '\n'.join(
            rule.as_sql() for rule in rules
            if rule.recode and rule.site_group != 'Invalid') +
                    "\n/* Invalid */ else '99999'\nend")


test_lines = __doc__.split('\n\n')[2].split('\n')


class Rule(namedtuple('Rule', 'site_group site histology recode')):
    r'''Each Rule is taken from a row in the recode definition table.

    We skip the header when reading the lines:

    >>> r = Rule.from_lines(test_lines)
    >>> len(r)
    10

    The table has four columns::

    >>> r[1]
    ... # doctest: +NORMALIZE_WHITESPACE
    Rule(site_group='    Stomach',
         site='C160-C169',
         histology='excluding 9050-9055, 9140, 9590-9992',
         recode='21020')

    We can extract the paths implied by indentation::

    >>> g = Rule.site_group_paths(r)
    >>> list(g)
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [(1, ['Digestive System'], None),
     (2, ['Digestive System', 'Stomach'], '21020'),
    ...
     (1, ['Invalid'], '99999')]

    We can render them as i2b2-style terms::

    >>> terms = list(Rule.as_terms(r))
    >>> terms
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [Term(hlevel=0, path='Digestive System', name='Digestive System',
          basecode=None, visualattributes='FA'),
     Term(hlevel=1, path='Digestive System\\Stomach', name='Stomach',
          basecode='21020', visualattributes='LA'),
    ...
     Term(hlevel=0, path='Invalid', name='Invalid', basecode='99999',
          visualattributes='LA')]

    Or as SQL case clauses::

    >>> print r[1].as_sql(),
    /* Stomach */ when (site between 'C160' and 'C169')
      and  not (histology between '9050' and '9055'
       or histology = '9140'
       or histology between '9590' and '9992') then '21020'
    '''

    @classmethod
    def from_lines(cls, lines):
        # drop title, heading
        lines = dropwhile(lambda l: l.startswith('Site '), iter(lines))
        # skip stuff at the bottom
        lines = takewhile(lambda l: len(l.split(';')) == 5, lines)

        return list(
            # clean up trailing whitespace in label, recode
            cls(label.rstrip(), site, hist, recode.strip() or None)
            for line in lines
            for (label, site, hist, recode, _) in [line.split(';')])

    @classmethod
    def site_group_paths(cls, records):
        '''Generate hierarchy based on indentation
        '''
        path = []
        for label, _site, _exc, recode in records:
            indent = (ix for ix in range(len(label))
                      if label[ix] != ' ').next()
            label = label[indent:]
            path = [(i, txt) for (i, txt) in path
                    if i < indent] + [(indent, label)]
            yield len(path), [seg for (_, seg) in path], recode

    @classmethod
    def as_terms(cls, rules):
        '''Generate i2b2-like terms from seer recode rules.

        Some rules reduce to duplicate terms; be sure to skip them:

        >>> r = Rule.from_lines(test_lines)
        >>> terms = list(Rule.as_terms(r))
        >>> [t.name for t in terms if t.basecode == '31040']
        ['Cranial Nerves Other ...']

        '''
        paths = Rule.site_group_paths(rules)
        terms = (Term(hlevel=level - 1,
                      path='\\'.join(part[:20] for part in parts),
                      name=parts[-1],
                      basecode=recode,
                      visualattributes='LA' if recode else 'FA')
                 for level, parts, recode in paths)

        skip_dups = reduce(lambda acc, p: acc if p in acc else acc + [p],
                           terms, [])
        return skip_dups

    def as_sql(self, site_col='site', hist_col='histology'):
        def mk_clause(eb, col):
            excl, bounds = eb
            if not bounds:
                return []
            ground = [(("%s between '%s' and '%s'" % (col, lo, hi))
                       if hi else
                       "%s = '%s'" % (col, lo))
                      for lo, hi in bounds]
            return [((' not ' if excl else '') +
                     '(' +
                     '\n   or '.join(ground) +
                     ')')]

        clause = '\n  and '.join(mk_clause(ranges(self.site), site_col) +
                                 mk_clause(ranges(self.histology), hist_col))
        return "/* %s */ when %s then '%s'\n" % (
            self.site_group.strip(), clause, self.recode)


Term = namedtuple('Term', 'hlevel path name basecode visualattributes')


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


def write_csv(fp, klass, items):
    fields = klass._fields
    o = csv.DictWriter(fp, fields, lineterminator='\n')
    o.writerow(dict(zip(fields, fields)))
    o.writerows([item._asdict() for item in items])


if __name__ == '__main__':
    def _script():
        from sys import argv

        logging.basicConfig(level=logging.INFO)

        def arg_rd(ix):
            return open(argv[ix])

        def arg_wr(ix):
            return open(argv[ix], 'w')

        main(arg_rd, arg_wr)
    _script()
