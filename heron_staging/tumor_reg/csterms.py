r'''ccterms -- make i2b2 terms for Collaborative Staging site-specific factors

Usage::

  python csterms.py ,seer_site_recode.txt ss_terms_out.csv


The American Joint Committee on Cancer (AJCC) maintains overall
management of the Collaborative Staging System and publishes a `schema
for browsing`__ as well as a `zip file with data in XML`__.

__ https://cancerstaging.org/cstage/schema/Pages/version0205.aspx
__ https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip  # noqa


For example, `Breast.xml` begins with the following markup::

    >>> from pkg_resources import resource_string
    >>> text = resource_string(__name__, 'cstable_ex.xml')
    >>> text.split('\n', 1)[0]
    ... # doctest: +ELLIPSIS
    '<cstgschema csschemaid="Breast" status="DRAFT" revised="06/27/2013" ...

For our purposes, we just need a few parts of the document::

    >>> lung = Site.from_doc(etree.fromstring(text))
    >>> lung.maintitle, lung.sitesummary
    ('Breast', 'C50.0-C50.6, C50.8-C50.9')

To build basecodes, we need the SEER Site Recode rules::

    >>> seer_rules = seer_recode.Rule.from_lines(seer_recode.test_lines)

Now we can get i2b2 style terms for site-specific factors of lung::

    >>> lung_terms = SeerSiteTerm.from_site(lung, seer_rules)
    >>> from itertools import islice
    >>> for t in islice(lung_terms, 5):
    ...     print t.c_basecode, t.c_name
    ...     print t.c_fullname
    SEER_SITE:26000 Breast
    \i2b2\naaccr\csterms\Breast\
    None 01: Estrogen Receptor (ER) Assay
    \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\
    CS26000|1:000 000: OBSOLETE DATA CONVERTED V0203
    \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\000\
    CS26000|1:010 010: Positive/elevated
    \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\010\
    CS26000|1:020 020: Negative/normal; within normal limits
    \i2b2\naaccr\csterms\Breast\CS Site-Specific Factor 1\020\

'''

from collections import namedtuple
import csv
import logging

# on windows, via conda/anaconda
from lxml import etree

from lafile import osRd
from i2b2mdmk import I2B2MetaData, Term
import seer_recode

log = logging.getLogger(__name__)


def main(argv, rd, arg_wr):
    seer_fn, out_fn = argv[1:3]

    with (rd / seer_fn).inChannel() as seer_file:
        seer_rules = seer_recode.Rule.from_lines(seer_file)

    cs = CS(rd)
    terms = (t
             for s in cs.each_site()
             for t in SeerSiteTerm.from_site(s, seer_rules))

    with arg_wr(out_fn) as out:
        sink = csv.writer(out)
        sink.writerow(Term._fields)

        for t in SeerSiteTerm.hierarchy(seer_rules):
            sink.writerow(t)

        for t in terms:
            sink.writerow(t)


class SeerSiteTerm(I2B2MetaData):
    pfx = ['', 'i2b2']
    folder = ['naaccr', 'csterms']

    @classmethod
    def from_site(cls, s, rules):
        recode = s.recode(rules)
        # This could be computed just once rather than once per site.
        paths = dict((recode, path)
                     for (_level, path, recode)
                     in seer_recode.Rule.site_group_paths(rules))
        parts = cls.folder + paths[recode]
        yield cls.term(pfx=cls.pfx,
                       parts=parts, viz='FAE',
                       name=s.maintitle)

        for vbl in s.variables:
            factor = vbl.factor()
            if not factor:
                continue
            vparts = parts + [vbl.title]
            yield VariableTerm.from_variable(vbl, parts)

            for v in vbl.values:
                yield ValueTerm.from_value(
                    v, vparts, recode, factor)

    @classmethod
    def hierarchy(cls, rules,
                  name='Cancer Staging: Site-Specific Factors'):
        # Top
        yield cls.term(pfx=cls.pfx,
                       parts=cls.folder,
                       name=name)

        mix = zip(seer_recode.Rule.site_group_paths(rules), rules)

        # Interior nodes
        for ((_level, path, recode), r) in mix:
            if not recode:
                yield cls.term(pfx=cls.pfx,
                               parts=cls.folder + path,
                               name=r.site_group)


class VariableTerm(I2B2MetaData):
    @classmethod
    def from_variable(cls, vbl, parts):
        vparts = parts + [vbl.title]
        factor = vbl.factor()
        num = '%02d: ' % factor if factor else ''
        return cls.term(pfx=SeerSiteTerm.pfx,
                        parts=vparts, viz='FAE',
                        name=num + (vbl.subtitle or vbl.title))


class ValueTerm(I2B2MetaData):
    @classmethod
    def from_value(cls, v, parts, recode, factor):
        return I2B2MetaData.term(
            pfx=SeerSiteTerm.pfx,
            code='CS%s|%s:%s' % (recode, factor, v.code),
            parts=parts + [v.code], viz='LAE',
            name=v.code + ': ' + v.descrip.split('\n')[0])


class CS(object):
    cs_tables = '3_CSTables(HTMLandXML).zip'

    xml_format = '3_CS Tables (HTML and XML)/XML Format/'

    def __init__(self, rd):
        self._xml_dir = (rd / self.xml_format)

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


class Site(namedtuple('Site', 'maintitle subtitle sitesummary variables')):
    @classmethod
    def from_doc(cls, doc_elt):
        title = doc_elt.xpath('schemahead/title')[0]
        subtitle = maybeNode(doc_elt.xpath('schemahead/subtitle/text()'))
        tables = doc_elt.xpath('cstable')
        return cls(title.xpath('maintitle/text()')[0],
                   subtitle,
                   maybeNode(title.xpath('sitesummary/text()')),
                   (Variable.from_table(t) for t in tables))

    def recode(self, rules):
        if not self.sitesummary:
            return None

        # C50.0-C50.6, C50.8-C50.9 => C500
        needle = self.sitesummary[:5].replace('.', '')
        try:
            return (rule.recode
                    for rule in rules
                    if rule.site.startswith(needle)).next()
        except StopIteration:
            return None


class Variable(namedtuple('Variable', 'name title subtitle values')):
    @classmethod
    def from_table(cls, table):
        name = table.xpath('tablename')[0]
        title = name.xpath('tabletitle/text()')
        subtitle = name.xpath('tablesubtitle//text()')
        log.debug('tabletitle: %s tablesubtitle: %s',
                  title, subtitle)

        return cls(name, ' '.join(title), ' '.join(subtitle),
                   (v
                    for r in table.xpath('row')
                    for v in Value.from_row(r)))

    def factor(self, pfx='CS Site-Specific Factor'):
        return self.title.startswith(pfx) and int(self.title[len(pfx):])


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

        def arg_wr(n):
            if n not in argv:
                raise IOError
            return open(n, 'w')

        main(argv, rd, arg_wr)

    _configure_logging()
    _trusted_main()
