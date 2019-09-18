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
    ...     print(ix + 1, factor.subtitle)
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
    ...     print(t.c_hlevel, t.c_basecode or '_', t.c_name)
    ...     print('---', t.c_fullname)
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
    ...           print(ix + 1, factor.subtitle)
    ...           print(val.code, "/", val.descrip.split('\n')[0])
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
    ...     print(txt)
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
    >>> print(case)
    ... # doctest: +NORMALIZE_WHITESPACE
    when primary_site in ('C500', 'C501', 'C502', 'C503', 'C504', 'C505',
                          'C506', 'C508', 'C509')
      then 'Breast'

'''

from dataclasses import dataclass
from pathlib import Path as Path_T
from typing import List, Iterator, NamedTuple, Optional as Opt, Sequence, Tuple
from typing import Any, TypeVar, Type
import csv
import logging
import re

# on windows, via conda/anaconda
from lxml import etree  # type: ignore
from docopt import docopt

from i2b2mdmk import I2B2MetaData, Term

USAGE = __doc__.split('\n.. ')[0]
log = logging.getLogger(__name__)

TI = TypeVar('TI', bound='Item')


@dataclass
class Item:
    @classmethod
    def filter(cls: Type[TI], items: List['Item']) -> List[TI]:
        out = []  # type: List[TI]
        for i in items:
            if isinstance(i, cls):
                out.append(i)
        return out


@dataclass
class SiteItem(Item):
    val: str
    excl: Opt[str]


@dataclass
class HistItem(Item):
    hist: str
    site_excl: Opt[str]


@dataclass
class MItem(Item):
    expr: str
    excl: Opt[str]


@dataclass
class DiscItem(Item):
    disc: List[str]


@dataclass
class NoteItem(Item):
    note: str


@dataclass
class NoneItem(Item):
    note: str


def main(argv: List[str], cwd: Path_T) -> None:
    opts = docopt(USAGE, argv=argv[1:])

    cs = CS(cwd / opts['--metadata'])

    if opts['terms']:
        with (cwd / opts['--out']).open('w') as out:
            sink = csv.writer(out)
            sink.writerow(Term._fields)

            sink.writerow(SchemaTerm.root())
            for site in cs.each_site():
                sink.writerow(SchemaTerm.from_site(site))

                for t in SchemaTerm.site_factor_terms(site):
                    sink.writerow(t)

    elif opts['sql']:
        with (cwd / opts['--template']).open() as tpl:
            top, bottom = tpl.read().split('&&CASES')

        with (cwd / opts['--sql']).open('w') as out:
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
    ...     print(t.c_hlevel, t.c_basecode or '_', t.c_name)
    ...     print('---', t.c_fullname)
    3 _ Breast
    --- \i2b2\naaccr\csterms\Breast\

    '''
    pfx = ['', 'i2b2']
    folder = ['naaccr', 'csterms']

    @classmethod
    def from_site(cls, site: 'Site') -> Term:
        _comment, _case, problems = Site.schema_constraint(site)
        oops = 'NOT SUPPORTED: ' if problems else ''
        return cls.term(pfx=cls.pfx,
                        parts=cls.folder + [site.csschemaid], viz='FAE',
                        name=oops + site.maintitle)

    @classmethod
    def site_factor_terms(cls, site: 'Site') -> Iterator[Term]:
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
             name: str = 'Cancer Staging: Site-Specific Factors') -> Term:
        # Top
        return cls.term(pfx=cls.pfx,
                        parts=cls.folder,
                        name=name)


class VariableTerm(I2B2MetaData):
    '''i2b2 term for a nominal variable
    '''
    @classmethod
    def from_variable(cls, vbl: 'Variable', parts: List[str], csschemaid: str, units: Opt[str] = None) -> Term:
        '''Build an i2b2 term from a :class:`Variable`.

        @param parts: path segments of i2b2 parent folder of this variable

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
    def from_value(cls, v: 'Value', parts: List[str], csschemaid: str, factor: Opt[int]) -> Term:
        '''Build an i2b2 term from a :class:`Value`

        @param parts: path segments of variable folder parent
        '''
        return I2B2MetaData.term(
            pfx=SchemaTerm.pfx,
            code='CS|%s|%s:%s' % (csschemaid, factor, v.code),
            parts=parts + [v.code], viz='LAE',
            name=v.code + ': ' + v.descrip.split('\n')[0])


class CS:
    cs_tables = '3_CSTables(HTMLandXML).zip'

    def __init__(self, xml_dir: Path_T) -> None:
        self._xml_dir = xml_dir

    def each_doc(self) -> Iterator[etree.Element]:
        '''Generate a parsed XML document for each .xml file in the schema.
        '''
        targets = [target for target in self._xml_dir.iterdir()
                   if target.suffix == '.xml']
        log.info('XML format files: %d', len(targets))

        parser = etree.XMLParser(load_dtd=True)
        parser.resolvers.add(LAResolver(self._xml_dir))

        for f in targets:
            log.info('file: %s', f.name)
            doc = etree.parse(f.open(), parser)
            yield doc.getroot()

    def each_site(self) -> Iterator['Site']:
        for doc_elt in self.each_doc():
            yield Site.from_doc(doc_elt)


class Site(NamedTuple('Site',
                      [('csschemaid', str), ('maintitle', str), ('subtitle', Opt[str]),
                       ('sitesummary', Opt[str]), ('variables', List['Variable']), ('notes', List[str])])):
    '''Parsed site data

    >>> breast = Site.from_doc(Site._test_doc())

    The `_parse_notes()` function is an intermediate step in
    generating the SQL site_constraint.

    >>> Site._parse_notes(breast.notes)
    ... # doctest: +NORMALIZE_WHITESPACE
    [DiscItem(disc=['SSF17', 'SSF18', 'SSF19', 'SSF20', 'SSF24']),
     SiteItem(val='C500', excl=None), SiteItem(val='C501', excl=None),
     SiteItem(val='C502', excl=None), SiteItem(val='C503', excl=None),
     SiteItem(val='C504', excl=None), SiteItem(val='C505', excl=None),
     SiteItem(val='C506', excl=None), SiteItem(val='C508', excl=None),
     SiteItem(val='C509', excl=None),
     NoteItem(note='Laterality must be coded for this site.')]

    >>> Site._parse_notes([
    ...     'M-8720-8790',
    ...     'M-9590-9699,9702-9729 (EXCEPT C44.1, C69.0, C69.5-C69.6)'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [MItem(expr='8720-8790', excl=None),
     MItem(expr='9590-9699,9702-9729', excl='C441,C690,C695-C696')]

    >>> Site._parse_notes([
    ...     '9731 Plasmacytoma, NOS (except C441, C690, C695-C696)',
    ...     '9740     Mast cell sarcoma'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [HistItem(hist='9731', site_excl='C441,C690,C695-C696'),
     HistItem(hist='9740', site_excl=None)]

    >>> Site._parse_notes([
    ...     'C21.0 Anus, NOS (excluding skin of anus C44.5)',
    ...     'C16.1 Fundus of stomach, proximal 5 centimeters (cm) only',
    ...     'C17.3  Meckel diverticulum (site of neoplasm)'])
    ... # doctest: +NORMALIZE_WHITESPACE
    [SiteItem(val='C210', excl='C445'),
     SiteItem(val='C161', excl=None),
     SiteItem(val='C173', excl=None)]

    '''
    @classmethod
    def from_doc(cls, doc_elt: etree.Element) -> 'Site':
        title = doc_elt.xpath('schemahead/title')[0]
        subtitle = maybeNode(doc_elt.xpath('schemaheads/ubtitle/text()'))
        tables = doc_elt.xpath('cstable')
        notes = doc_elt.xpath('schemahead/note')
        return cls(doc_elt.xpath('@csschemaid')[0],
                   title.xpath('maintitle/text()')[0],
                   subtitle,
                   maybeNode(title.xpath('sitesummary/text()')),
                   [Variable.from_table(t) for t in tables],
                   [''.join(note.xpath('.//text()')) for note in notes])

    @classmethod
    def schema_constraint(cls, site: 'Site') -> Tuple[str, Opt[str], Opt[List[str]]]:
        comment = '{title}\n{sitesummary}\n{notes}'.format(
            title=site.maintitle,
            sitesummary=site.sitesummary,
            notes='\n'.join(site.notes))
        if '*/' in comment:
            raise ValueError(site.notes)

        clauses = cls._parse_notes(site.notes)
        problems = [p.note for p in NoneItem.filter(clauses)]
        if problems:
            return comment, None, problems

        case = "when {test}\n  then '{id}'".format(
            test=_sql_and([_site_check(clauses),
                           _morph_check(clauses),
                           _hist_check(clauses)]),
            id=site.csschemaid)

        return comment, case, None

    @classmethod
    def _parse_notes(cls, notes: List[str]) -> List[Item]:
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

        def nodot(s: str) -> str:
            return s.replace('.', '').replace(' ', '')

        def nodotOpt(s: Opt[str]) -> Opt[str]:
            return (nodot(s)
                    if s else s)

        parsed = [
            SiteItem(nodot(m.group('site')), nodotOpt(m.group('excl_site')))
            if m and m.group('site') else

            HistItem(m.group('hist_lo'),
                          nodotOpt(m.group('except_site')))
            if m and m.group('hist_lo') else
            MItem(m.group('M').replace(' ', ''), nodotOpt(m.group('M_no_C')))
            if m and m.group('M') else

            DiscItem(m.group('disc').split(', '))
            if m and m.group('disc') else

            NoteItem(m.group('note'))
            if m and m.group('note')

            else NoneItem(note)

            for note in notes
            for m in [re.match(pat, note)]]  # type: List[Item]
        return parsed

    def each_site_specific_factor(self) -> Iterator['Variable']:
        for vbl in self.variables:
            if not vbl.factor_num():
                continue
            yield vbl

    @classmethod
    def _test_markup(cls) -> str:
        from pkg_resources import resource_string
        return resource_string(__name__, 'cstable_ex.xml').decode('utf-8')

    @classmethod
    def _test_doc(cls) -> etree.ElementTree:
        return etree.fromstring(cls._test_markup())


def _site_check(items: List[Item],
                col: str = 'primary_site') -> Opt[str]:
    '''Render primary site constraint.

    >>> breast = Site.from_doc(Site._test_doc())
    >>> print(_site_check(Site._parse_notes(breast.notes)))
    ... # doctest: +NORMALIZE_WHITESPACE
    primary_site in ('C500', 'C501', 'C502', 'C503', 'C504', 'C505',
                     'C506', 'C508', 'C509')

    >>> print(_site_check([('M', ('8720-8790', None))]))
    None

    '''
    site_items = SiteItem.filter(items)
    yes = [i.val for i in site_items]
    no = [i.excl for i in site_items if i.excl]
    return _sql_and(([_sql_enum(col, yes)] if yes else []) +
                    ([negate(_sql_enum(col, no))] if no else []))


def negate(expr: str) -> str:
    return '(not %s)' % expr


def _hist_check(items: List[Item],
                col: str = 'histology',
                site_col: str = 'primary_site') -> Opt[str]:
    '''Render constraint from histology items.

    >>> print(_hist_check([HistItem('9731', '441,690,695-696')]))
    (histology = '9731' and (not (primary_site = '441'
      or primary_site = '690'
      or primary_site between '695' and '696')))
    '''
    clauses = [_sql_and(["{col} = '{hist}'".format(col=col, hist=i.hist)] +
                        ([negate(_item_expr(i.site_excl, site_col))]
                         if i.site_excl else []))
               for i in HistItem.filter(items)]

    return _sql_or(clauses)


def _morph_check(items: List[Item],
                 col: str = 'histology',
                 site_col: str = 'primary_site') -> Opt[str]:
    '''Render constraint from M- items.

    >>> _morph_check([MItem('8720-8790', None)])
    "histology between '8720' and '8790'"

    >>> print(_morph_check([MItem('8000-8152,8247,8248,8250-8934', None)]))
    (histology between '8000' and '8152'
      or histology = '8247'
      or histology = '8248'
      or histology between '8250' and '8934')

    >>> print(_morph_check([MItem('9590-9699,9738', '441,690,695-696'),
    ...                     MItem('9811-9818', '421,424,441,690,695-696')]))
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
    clauses = [_sql_and([_item_expr(i.expr, col)] +
                        ([negate(_item_expr(i.excl, site_col))] if i.excl else []))
               for i in MItem.filter(items)]
    return _sql_or(clauses) if clauses else None


def _item_expr(expr: str, col: str) -> str:
    '''
    >>> print(_item_expr('441,690,695-696', 'c1'))
    (c1 = '441'
      or c1 = '690'
      or c1 between '695' and '696')
    '''
    r = _sql_ranges(col,
                    [(lo, hi)
                     for lo_hi in expr.split(',')
                     for (lo, hi) in [(lo_hi.split('-') + [None])[:2]]])  # type: ignore
    return r


def _sql_and(conjuncts: Sequence[Opt[str]]) -> Opt[str]:
    flat = [c for c in conjuncts if c is not None]
    return (None if not flat else
            flat[0] if len(flat) == 1 else
            parens(' and '.join(flat)))


def parens(expr: str) -> str:
    return '(%s)' % expr


def _sql_or(clauses: List[Opt[str]]) -> Opt[str]:
    flat = [c for c in clauses if c is not None]
    return (None if not flat else
            flat[0] if len(flat) == 1 else
            parens('\n  or '.join(flat)))


def _sql_enum(col: str, values: List[str]) -> str:
    return ('{col} in ({vals})'.format(
        col=col,
        vals=', '.join("'%s'" % val for val in values)))


def _sql_ranges(col: str, ranges: List[Tuple[str, Opt[str]]]) -> str:
    assert ranges

    def sql_range(col: str, lo: str, hi: Opt[str]) -> str:
        return (
            "{col} = '{lo}'".format(
                col=col, lo=lo) if hi is None else
            "{col} between '{lo}' and '{hi}'".format(
                col=col, lo=lo, hi=hi))

    expr = _sql_or([sql_range(col, lo, hi)
                    for (lo, hi) in ranges])
    assert expr
    return expr


class Variable(NamedTuple('Variable', [('title', str), ('subtitle', str), ('values', List['Value'])])):
    @classmethod
    def from_table(cls, table: etree.Element) -> 'Variable':
        name = table.xpath('tablename')[0]
        title = name.xpath('tabletitle/text()')
        subtitle = name.xpath('tablesubtitle//text()')
        log.debug('tabletitle: %s tablesubtitle: %s',
                  title, subtitle)

        return cls(' '.join(title), ' '.join(subtitle),
                   [v
                    for r in table.xpath('row')
                    for v in Value.from_row(r)])

    def factor_num(self, pfx: str = 'CS Site-Specific Factor') -> Opt[int]:
        return (self.subtitle and
                self.title.startswith(pfx) and
                int(self.title[len(pfx):]) or None)


class Value(NamedTuple('Code', [('code', str), ('descrip', str)])):
    @classmethod
    def from_row(cls, row: etree.Element) -> Iterator['Value']:
        '''Maybe generate a value from a row.
        '''
        code = maybeNode(row.xpath('code/text()'))
        descrip = maybeNode(row.xpath('descrip/text()'))
        if code and descrip:
            yield cls(code, descrip)


def maybeNode(nodes: List[etree.Element]) -> Opt[etree.Element]:
    return nodes[0] if len(nodes) > 0 else None


class LAResolver(etree.Resolver):  # type: ignore
    '''Resolve entity references in context of a :class:`lafile.Rd`.
    '''
    def __init__(self, rd: Path_T) -> None:
        self.__rd = rd

    def resolve(self, url: str, name: str, context: object) -> Any:
        # log.debug('resolving: %s', url)
        return self.resolve_file((self.__rd / url).open(), context)


if __name__ == '__main__':
    def _script_io() -> None:
        from pathlib import Path
        from sys import argv

        logging.basicConfig(level=logging.INFO)
        main(argv[:], cwd=Path('.'))

    _script_io()
