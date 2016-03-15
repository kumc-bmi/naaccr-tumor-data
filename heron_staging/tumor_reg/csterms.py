r'''csterms -- i2b2 ontology for Collaborative Staging site-specific factors

Usage:
  csterms.py [options] terms
  csterms.py [options] sql

Options:
 terms                  Write i2b2 metadata terms (less update_date)
                        for "schema" and site-specific factors
 -m DIR --metadata=DIR  input metadata directory
                        [default: 3_CS Tables (HTML and XML)/XML Format/]
 -o F --out=FILE        where to write terms [default: cs-terms.csv]
 sql                    Render schema selection notes as SQL in a view
                        that combines primary site histology into `csschemaid`
                        and produce concept codes corresponding to terms.
 -t F --template=FILE   SQL template [default: csschema_tpl.sql]
 -s F --sql=FILE        where to write SQL [default: csschema.sql]
 --help                 Show this help and exit.

.. note: This line separates usage doc above from design/test notes below.


The American Joint Committee on Cancer (AJCC) maintains overall
management of the Collaborative Staging System and publishes a `schema
for browsing`__ as well as a `zip file with data in XML`__.

__ https://cancerstaging.org/cstage/schema/Pages/version0205.aspx
__ https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip  # noqa


For example, `Breast.xml` begins with the following markup::

    >>> text = Site._test_markup()
    >>> text.split('\n', 1)[0]
    ... # doctest: +ELLIPSIS
    '<cstgschema csschemaid="Breast" status="DRAFT" revised="06/27/2013" ...

Each document specifies a "schema", which has an id and a title:

    >>> breast = Site.from_doc(etree.fromstring(text))
    >>> breast.csschemaid, breast.maintitle
    ('Breast', 'Breast')

Each schema specifies its site-specific factors:

    >>> for ix, factor in enumerate(breast.each_site_specific_factor()):
    ...     print ix + 1, factor.subtitle
    ... # doctest: +ELLIPSIS
    1 Estrogen Receptor (ER) Assay
    2 Progesterone Receptor (PR) Assay
    3 Number of Positive Ipsilateral Level I-II Axillary Lymph Nodes
    4 Immunohistochemistry (IHC) of Regional Lymph Nodes
    ...
    22 Multigene Signature Method
    23 Multigene Signature Results
    24 Paget Disease

We can render these site-specific factors as an i2b2 ontology::

    >>> breast = Site.from_doc(etree.fromstring(text))  # restart generators
    >>> breast_terms = SchemaTerm.site_factor_terms(breast)
    >>> for t in breast_terms:
    ...     print t.c_hlevel, t.c_basecode or '_', t.c_name
    ...     print '---', t.c_fullname
    ... # doctest: +ELLIPSIS
    5 CS|Breast|1:000 000: OBSOLETE DATA CONVERTED V0203
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\000\
    5 CS|Breast|1:010 010: Positive/elevated
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\010\
    ...
    4 CS|Breast|1: 01: Estrogen Receptor (ER) Assay
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\
    ...
    4 CS|Breast|2: 02: Progesterone Receptor (PR) Assay
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 2\
    5 CS|Breast|3:000 000: All ipsilateral axillary nodes examined negative
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\000\
    5 CS|Breast|3:090 090: 90 or more nodes positive
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\090\
    ...
    4 CS|Breast|3: 03: Number of Positive Ipsilateral Level I-II Axillary Lymph Nodes
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\
    ...
    5 CS|Breast|24:999 999: Unknown or no information
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 24\999\
    4 CS|Breast|24: 24: Paget Disease
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 24\

Note that in some cases, the 3 digit values are not enumerated codes
but numeric values::

    >>> breast = Site.from_doc(etree.fromstring(text))  # restart generators
    >>> for ix, factor in enumerate(breast.each_site_specific_factor()):
    ...     for val in factor.values:
    ...         if '-' in val.code and 'OBSOLETE' not in val.descrip:
    ...           print ix + 1, factor.subtitle
    ...           print val.code, "/", val.descrip.split('\n')[0]
    3 Number of Positive Ipsilateral Level I-II Axillary Lymph Nodes
    001-089 / 1 - 89 nodes positive 
    10 HER2: Fluorescence In Situ Hybridization (FISH) Lab Value
    100-979 / Ratio of 1.00 - 9.79 
    12 HER2: Chromogenic In Situ Hybridization (CISH) Lab Value
    100-979 / Mean of 1.00 - 9.79
    23 Multigene Signature Results
    000-100 / Score of 000 - 100

TODO: c_metadataxml for numeric values.

Each schema has a list of notes for determining when it applies;
we can render these as SQL expressions:

    >>> for txt in breast.notes:
    ...     print txt
    DISCONTINUED SITE-SPECIFIC FACTORS:  SSF17, SSF18, SSF19, SSF20, SSF24
    C50.0  Nipple
    C50.1  Central portion of breast
    C50.2  Upper-inner quadrant of breast
    C50.3  Lower-inner quadrant of breast
    C50.4  Upper-outer quadrant of breast
    C50.5  Lower-outer quadrant of breast
    C50.6  Axillary Tail of breast
    C50.8  Overlapping lesion of breast
    C50.9  Breast, NOS
    Note:  Laterality must be coded for this site.

    >>> _comment, case, _problems = Site.schema_constraint(breast)
    >>> print case
    ... # doctest: +NORMALIZE_WHITESPACE
    when primary_site in ('C500', 'C501', 'C502', 'C503', 'C504', 'C505',
                          'C506', 'C508', 'C509')
      then 'Breast'

'''

from collections import namedtuple
import csv
import logging
import re

# on windows, via conda/anaconda
from lxml import etree
from docopt import docopt

from lafile import osRd
from i2b2mdmk import I2B2MetaData, Term

log = logging.getLogger(__name__)


def main(access, rd):
    opts, opt_wr = access()

    cs = CS(rd / opts['--metadata'])

    if opts['terms']:
        with opt_wr('--out') as out:
            sink = csv.writer(out)
            sink.writerow(Term._fields)

            sink.writerow(SchemaTerm.root())
            for site in cs.each_site():
                sink.writerow(SchemaTerm.from_site(site))

                for t in SchemaTerm.site_factor_terms(site):
                    sink.writerow(t)

    elif opts['sql']:
        with (rd / opts['--template']).inChannel() as tpl:
            top, bottom = tpl.read().split('&&CASES')

        with opt_wr('--sql') as out:
            out.write(top)

            for site in cs.each_site():
                comment, case, problems = Site.schema_constraint(site)
                if problems:
                    log.warn('skipping {site}: {qty} problems'.format(
                        site=site.maintitle, qty=len(problems)))
                out.write('/* {comment} */\n {case}\n\n'.format(
                    comment=comment,
                    case=('/* SKIPPED {id} */'.format(id=site.csschemaid)
                          if problems else case)))

            out.write(bottom)


class SchemaTerm(I2B2MetaData):
    r'''i2b2 term(s) for CS Schema

    >>> breast = Site.from_doc(Site._test_doc())
    >>> for t in [SchemaTerm.from_site(breast)]:
    ...     print t.c_hlevel, t.c_basecode or '_', t.c_name
    ...     print '---', t.c_fullname
    3 _ Breast
    --- \i2b2\naaccr\csterms\Breast\

    '''
    pfx = ['', 'i2b2']
    folder = ['naaccr', 'csterms']

    @classmethod
    def from_site(cls, site):
        _comment, _case, problems = Site.schema_constraint(site)
        oops = 'NOT SUPPORTED: ' if problems else ''
        return cls.term(pfx=cls.pfx,
                        parts=cls.folder + [site.csschemaid], viz='FAE',
                        name=oops + site.maintitle)

    @classmethod
    def site_factor_terms(cls, site):
        site_folder = cls.folder + [site.csschemaid]

        for factor in site.each_site_specific_factor():
            vparts = site_folder + [factor.title]
            units = None
            for val in factor.values:
                if '-' in val.code:
                    if 'OBSOLETE' not in val.descrip:
                        units = val.descrip
                    continue
                yield ValueTerm.from_value(
                    val, vparts, site.csschemaid, factor.factor_num())

            yield VariableTerm.from_variable(
                factor, site_folder, site.csschemaid, units)

    @classmethod
    def root(cls,
             name='Cancer Staging: Site-Specific Factors'):
        # Top
        return cls.term(pfx=cls.pfx,
                        parts=cls.folder,
                        name=name)


class VariableTerm(I2B2MetaData):
    '''i2b2 term for a nominal variable
    '''
    @classmethod
    def from_variable(cls, vbl, parts, csschemaid, units=None):
        '''Build an i2b2 term from a :class:`Variable`.

        @type vbl: Variable
        @param parts: path segments of i2b2 parent folder of this variable
        @type parts: [str]

        @@TODO: c_metadataxml for numeric stuff with units
        '''
        vparts = parts + [vbl.title]
        factor = vbl.factor_num()
        num = '%02d: ' % factor if factor else ''
        return cls.term(pfx=SchemaTerm.pfx,
                        parts=vparts, viz='FAE',
                        tooltip=units.replace('\n', ' ') if units else None,
                        code='CS|%s|%s:' % (csschemaid, factor),
                        name=num + (vbl.subtitle or vbl.title))


class ValueTerm(I2B2MetaData):
    '''i2b2 term for a value of a nominal variable
    '''
    @classmethod
    def from_value(cls, v, parts, csschemaid, factor):
        '''Build an i2b2 term from a :class:`Value`

        @type vbl: Variable
        @param parts: path segments of variable folder parent
        '''
        return I2B2MetaData.term(
            pfx=SchemaTerm.pfx,
            code='CS|%s|%s:%s' % (csschemaid, factor, v.code),
            parts=parts + [v.code], viz='LAE',
            name=v.code + ': ' + v.descrip.split('\n')[0])


class CS(object):
    cs_tables = '3_CSTables(HTMLandXML).zip'

    def __init__(self, xml_dir):
        self._xml_dir = xml_dir

    def each_doc(self):
        '''Generate a parsed XML document for each .xml file in the schema.
        '''
        targets = [target for target in self._xml_dir.subRdFiles()
                   if target.path.endswith('.xml')]
        log.info('XML format files: %d', len(targets))

        parser = etree.XMLParser(load_dtd=True)
        parser.resolvers.add(LAResolver(self._xml_dir))

        for f in targets:
            log.info('file: %s', f.path.split('/')[-1])
            doc = etree.parse(f.inChannel(), parser)
            yield doc.getroot()

    def each_site(self):
        for doc_elt in self.each_doc():
            yield Site.from_doc(doc_elt)


class Site(namedtuple('Site',
                      ['csschemaid', 'maintitle', 'subtitle',
                       'sitesummary', 'variables', 'notes'])):
    '''Parsed site data

    >>> breast = Site.from_doc(Site._test_doc())

    The `_parse_notes()` function is an intermediate step in
    generating the SQL site_constraint.

    >>> Site._parse_notes(breast.notes)
    ... # doctest: +NORMALIZE_WHITESPACE
    [('disc', ['SSF17', 'SSF18', 'SSF19', 'SSF20', 'SSF24']),
     ('site', ('C500', None)), ('site', ('C501', None)),
     ('site', ('C502', None)), ('site', ('C503', None)),
     ('site', ('C504', None)), ('site', ('C505', None)),
     ('site', ('C506', None)), ('site', ('C508', None)),
     ('site', ('C509', None)),
     ('note', 'Laterality must be coded for this site.')]

    >>> Site._parse_notes([
    ...     'M-8720-8790',
    ...     'M-9590-9699,9702-9729 (EXCEPT C44.1, C69.0, C69.5-C69.6)'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [('M', ('8720-8790', None)),
     ('M', ('9590-9699,9702-9729', 'C441,C690,C695-C696'))]

    >>> Site._parse_notes([
    ...     '9731 Plasmacytoma, NOS (except C441, C690, C695-C696)',
    ...     '9740     Mast cell sarcoma'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [('histology', ('9731', 'C441,C690,C695-C696')),
     ('histology', ('9740', None))]

    >>> Site._parse_notes([
    ...     'C21.0 Anus, NOS (excluding skin of anus C44.5)',
    ...     'C16.1 Fundus of stomach, proximal 5 centimeters (cm) only',
    ...     'C17.3  Meckel diverticulum (site of neoplasm)'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [('site', ('C210', 'C445')),
     ('site', ('C161', None)),
     ('site', ('C173', None))]

    '''
    @classmethod
    def from_doc(cls, doc_elt):
        title = doc_elt.xpath('schemahead/title')[0]
        subtitle = maybeNode(doc_elt.xpath('schemahead/subtitle/text()'))
        tables = doc_elt.xpath('cstable')
        notes = doc_elt.xpath('schemahead/note')
        return cls(doc_elt.xpath('@csschemaid')[0],
                   title.xpath('maintitle/text()')[0],
                   subtitle,
                   maybeNode(title.xpath('sitesummary/text()')),
                   (Variable.from_table(t) for t in tables),
                   [''.join(note.xpath('.//text()')) for note in notes])

    @classmethod
    def schema_constraint(cls, site):
        comment = '{title}\n{sitesummary}\n{notes}'.format(
            title=site.maintitle,
            sitesummary=site.sitesummary,
            notes='\n'.join(site.notes))
        if '*/' in comment:
            raise ValueError(site.notes)

        clauses = cls._parse_notes(site.notes)
        problems = [note for (tag, note) in clauses if not tag]
        if problems:
            return comment, None, problems

        case = "when {test}\n  then '{id}'".format(
            test=_sql_and([_site_check(clauses),
                           _morph_check(clauses),
                           _hist_check(clauses)]),
            id=site.csschemaid)

        return comment, case, None

    @classmethod
    def _parse_notes(cls, notes):
        '''Interpret site notes as SQL constraint
        '''
        pat = re.compile(
            r'''^((?P<site>C\d\d\.\d) # C50.1  Central portion ...
                  (?:[^\(]+|\([^C]+\))*  # words or (cm)
                  (\([^C]+(?P<excl_site>C[^\)]+)?\))?$)
               |((?P<hist_lo>\d{4}) [^\(]+
                 (\([^C]+(?P<except_site>C[^)]+)\))?$)
               |(M-(?P<M>(-|\d{4}|,|\ )+) [^\(]*
                 (\((EXCEPT)?[^C]+(?P<M_no_C>C[^)]+)\))?\s*$)
               |(DISCONTINUED\ SITE-SPECIFIC\ FACTORS:\s*(?P<disc>.*))
               |(Note(?:\s*\d+)?:\s+(?P<note>.*))
            ''', re.VERBOSE)

        nodot = lambda s: (s.replace('.', '').replace(' ', '')
                           if s else s)

        parsed = [
            ('site', (nodot(m.group('site')), nodot(m.group('excl_site'))))
            if m and m.group('site') else

            ('histology', (m.group('hist_lo'),
                           nodot(m.group('except_site'))))
            if m and m.group('hist_lo') else
            ('M', (m.group('M').replace(' ', ''), nodot(m.group('M_no_C'))))
            if m and m.group('M') else

            ('disc', m.group('disc').split(', '))
            if m and m.group('disc') else

            ('note', m.group('note'))
            if m and m.group('note')

            else (None, note)

            for note in notes
            for m in [re.match(pat, note)]]
        return parsed

    def each_site_specific_factor(self):
        for vbl in self.variables:
            if not vbl.factor_num():
                continue
            yield vbl

    @classmethod
    def _test_markup(cls):
        from pkg_resources import resource_string
        return resource_string(__name__, 'cstable_ex.xml')

    @classmethod
    def _test_doc(cls):
        return etree.fromstring(cls._test_markup())


def _site_check(items,
                col='primary_site'):
    '''Render primary site constraint.

    >>> breast = Site.from_doc(Site._test_doc())
    >>> print _site_check(Site._parse_notes(breast.notes))
    ... # doctest: +NORMALIZE_WHITESPACE
    primary_site in ('C500', 'C501', 'C502', 'C503', 'C504', 'C505',
                     'C506', 'C508', 'C509')

    >>> print _site_check([('M', ('8720-8790', None))])
    None

    '''
    tagged = lambda tag: filter(lambda i: i[0] == tag, items)
    site_items = tagged('site')
    yes = [val for (tag, (val, excl)) in site_items]
    no = [excl for (tag, (val, excl)) in site_items if excl]
    negate = lambda expr: '(not %s)' % expr

    return _sql_and(([_sql_enum(col, yes)] if yes else []) +
                    ([negate(_sql_enum(col, no))] if no else []))


def _hist_check(items,
                col='histology',
                site_col='primary_site'):
    '''Render constraint from histology items.

    >>> print _hist_check([('histology', ('9731', '441,690,695-696'))])
    (histology = '9731' and (not (primary_site = '441'
      or primary_site = '690'
      or primary_site between '695' and '696')))
    '''
    tagged = lambda tag: filter(lambda i: i[0] == tag, items)

    negate = lambda expr: '(not %s)' % expr
    clauses = [_sql_and(["{col} = '{hist}'".format(col=col, hist=hist)] +
                        ([negate(_item_expr(site_excl, site_col))]
                         if site_excl else []))
               for (_tag, (hist, site_excl)) in tagged('histology')]

    return _sql_or(clauses)


def _morph_check(items,
                col='histology',
                site_col='primary_site'):
    '''Render constraint from M- items.

    >>> _morph_check([('M', ('8720-8790', None))])
    "histology between '8720' and '8790'"

    >>> print _morph_check([('M', ('8000-8152,8247,8248,8250-8934', None))])
    (histology between '8000' and '8152'
      or histology = '8247'
      or histology = '8248'
      or histology between '8250' and '8934')

    >>> print _morph_check([('M', ('9590-9699,9738', '441,690,695-696')),
    ...                     ('M', ('9811-9818', '421,424,441,690,695-696'))])
    (((histology between '9590' and '9699'
      or histology = '9738') and (not (primary_site = '441'
      or primary_site = '690'
      or primary_site between '695' and '696')))
      or (histology between '9811' and '9818' and (not (primary_site = '421'
      or primary_site = '424'
      or primary_site = '441'
      or primary_site = '690'
      or primary_site between '695' and '696'))))

    '''
    tagged = lambda tag: filter(lambda i: i[0] == tag, items)
    negate = lambda expr: '(not %s)' % expr
    clauses = [_sql_and([_item_expr(expr, col)] +
                        ([negate(_item_expr(excl, site_col))] if excl else []))
               for (_tag, (expr, excl)) in tagged('M')]
    return _sql_or(clauses) if clauses else None


def _item_expr(expr, col):
    '''
    >>> print _item_expr('441,690,695-696', 'c1')
    (c1 = '441'
      or c1 = '690'
      or c1 between '695' and '696')
    '''
    r = _sql_ranges(col,
                    [(lo, hi)
                     for lo_hi in expr.split(',')
                     for (lo, hi) in [(lo_hi.split('-') + [None])[:2]]])
    return r


def _sql_and(conjuncts):
    flat = [c for c in conjuncts if c is not None]
    parens = lambda expr: '(%s)' % expr
    return (None if not flat else
            flat[0] if len(flat) == 1 else
            parens(' and '.join(flat)))


def _sql_or(clauses):
    flat = [c for c in clauses if c is not None]
    parens = lambda expr: '(%s)' % expr
    return (None if not flat else
            flat[0] if len(flat) == 1 else
            parens('\n  or '.join(flat)))


def _sql_enum(col, values):
    return ('{col} in ({vals})'.format(
        col=col,
        vals=', '.join("'%s'" % val for val in values)))


def _sql_ranges(col, ranges):
    assert ranges
    sql_range = lambda col, lo, hi: (
        "{col} = '{lo}'".format(
            col=col, lo=lo) if hi is None else
        "{col} between '{lo}' and '{hi}'".format(
            col=col, lo=lo, hi=hi))

    return _sql_or([sql_range(col, lo, hi)
                    for (lo, hi) in ranges])


class Variable(namedtuple('Variable', 'title subtitle values')):
    @classmethod
    def from_table(cls, table):
        name = table.xpath('tablename')[0]
        title = name.xpath('tabletitle/text()')
        subtitle = name.xpath('tablesubtitle//text()')
        log.debug('tabletitle: %s tablesubtitle: %s',
                  title, subtitle)

        return cls(' '.join(title), ' '.join(subtitle),
                   (v
                    for r in table.xpath('row')
                    for v in Value.from_row(r)))

    def factor_num(self, pfx='CS Site-Specific Factor'):
        return (self.subtitle and
                self.title.startswith(pfx) and
                int(self.title[len(pfx):]))


class Value(namedtuple('Code', 'code, descrip')):
    @classmethod
    def from_row(cls, row):
        '''Maybe generate a value from a row.
        '''
        code = maybeNode(row.xpath('code/text()'))
        descrip = maybeNode(row.xpath('descrip/text()'))
        if code and descrip:
            yield cls(code, descrip)


def maybeNode(nodes):
    return nodes[0] if len(nodes) > 0 else None


class LAResolver(etree.Resolver):
    '''Resolve entity references in context of a :class:`lafile.Rd`.
    '''
    def __init__(self, rd):
        def resolve(url, name, context):
            # log.debug('resolving: %s', url)
            return self.resolve_file(rd.subRdFile(url).inChannel(), context)

        self.resolve = resolve


if __name__ == '__main__':
    def _configure_logging(level=logging.INFO):
        logging.basicConfig(level=level)

    def _trusted_main():
        from sys import argv
        import os

        rd = osRd(os.curdir, lambda n: open(n), os.path, os.listdir)

        def access():
            usage = __doc__.split('\n.. ')[0]
            opts = docopt(usage, argv=argv[1:])

            def opt_wr(n):
                return open(opts[n], 'w')
            return opts, opt_wr

        main(access, rd)

    _configure_logging()
    _trusted_main()
