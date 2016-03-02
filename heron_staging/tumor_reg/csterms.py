r'''ccterms -- make i2b2 terms for Collaborative Staging site-specific factors

Usage:
  csterms.py [options] terms
  csterms.py [options] explore

Options:
 -m DIR --metadata=DIR  metadata directory
                        [default: 3_CS Tables (HTML and XML)/XML Format/]
 -o F --out=FILE        where to write terms [default: cs-terms.csv]


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

For our purposes, we just need a few parts of the document::

    >>> breast = Site.from_doc(etree.fromstring(text))
    >>> breast.csschemaid, breast.maintitle, breast.sitesummary
    ('Breast', 'Breast', 'C50.0-C50.6, C50.8-C50.9')

Now we can get i2b2 style terms for site-specific factors of breast::

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
    4 _ 01: Estrogen Receptor (ER) Assay
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\
    ...
    4 _ 02: Progesterone Receptor (PR) Assay
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 2\
    5 CS|Breast|3:000 000: All ipsilateral axillary nodes examined negative
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\000\
    5 CS|Breast|3:090 090: 90 or more nodes positive
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\090\
    ...
    4 _ 03: Number of Positive Ipsilateral Level I-II Axillary Lymph Nodes
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 3\
    ...
    4 _ 24: Paget Disease
    --- \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 24\
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

    if opts['explore']:
        explore(cs)

    elif opts['terms']:
        with opt_wr('--out') as out:
            sink = csv.writer(out)
            sink.writerow(Term._fields)

            sink.writerow(SchemaTerm.root())
            for site in cs.each_site():
                sink.writerow(SchemaTerm.from_site(site))

                for t in SchemaTerm.site_factor_terms(site):
                    sink.writerow(t)


def explore(cs):
    site_codes = [str(d1) + str(d2)
                 for d1 in range(10)
                 for d2 in range(10)]

    for site in cs.each_site():
        factors = [vbl for vbl in site.variables
                   if vbl.factor_num()]
        if not factors:
            continue
        if not site.sitesummary:
            import pdb; pdb.set_trace()
        f = lambda nn: CS.site_filter(site.sitesummary, nn)
        for matched in filter(f, site_codes):
            print site.maintitle, site.sitesummary, matched


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
        return cls.term(pfx=cls.pfx,
                        parts=cls.folder + [site.csschemaid], viz='FAE',
                        name=site.maintitle)

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

            yield VariableTerm.from_variable(factor, site_folder, units)

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
    def from_variable(cls, vbl, parts, units=None):
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

    @classmethod
    def site_filter(self, sitesummary, site):
        '''
        >>> CS.site_filter('C24.0 Extrahepatic bile duct', '22')
        False
        >>> CS.site_filter('C24.0 Extrahepatic bile duct', '24')
        True
        '''
        txt = sitesummary

        while txt:
            lohi = re.match(r'^C(\d\d)\.\d-C(\d\d)\.\d(, )?', txt)
            if lohi:
                lo, hi = lohi.group(1), lohi.group(2)
                if not lo <= site <= hi:
                    return False
                txt = txt[len(lohi.group(0)):]
                continue
            konst = re.match(r'^C(\d\d)\.\d(, )?', txt)
            if konst:
                x = konst.group(1)
                if not x <= site <= x:
                    return False
                txt = txt[len(konst.group(0)):]
                continue
            words = re.match(r'^[a-zA-Z ]+', txt)
            if words:
                txt = txt[len(words.group(0)):]
                continue
            else:
                import pdb; pdb.set_trace()
        return True


class Site(namedtuple('Site',
                      ['csschemaid', 'maintitle', 'subtitle',
                       'sitesummary', 'variables', 'notes'])):
    '''Parsed site data

    >>> breast = Site.from_doc(Site._test_doc())

    Site-specific factors:

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

    '''
    @classmethod
    def from_doc(cls, doc_elt):
        title = doc_elt.xpath('schemahead/title')[0]
        subtitle = maybeNode(doc_elt.xpath('schemahead/subtitle/text()'))
        tables = doc_elt.xpath('cstable')
        notes = doc_elt.xpath('schemahead/note/text()')
        return cls(doc_elt.xpath('@csschemaid')[0],
                   title.xpath('maintitle/text()')[0],
                   subtitle,
                   maybeNode(title.xpath('sitesummary/text()')),
                   (Variable.from_table(t) for t in tables),
                   notes)

    def special_notes(self):
        return [note
                for note in self.notes
                if not (
                        # C50.9  Breast, NOS
                        re.match(r'^C\d\d\.\d +\S', note) or
                        # 9732 Multiple myeloma
                        re.match(r'^\d\d\d\d +\S', note) or
                        note.startswith('DISCONTINUED '))]

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
