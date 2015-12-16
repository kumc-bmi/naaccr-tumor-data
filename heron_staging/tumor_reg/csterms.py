'''ccterms -- make i2b2 terms from Collaborative Staging tables
especially ER/PR status for breast cancer.

Usage::

first, download and unzip 3_CSTables(HTMLandXML).zip; see Makefile for details.

???@@TODO

'''

import csv
import logging

# on windows, via conda/anaconda
from lxml import etree

from lafile import osRd
from i2b2mdmk import I2B2MetaData, Term

log = logging.getLogger(__name__)


def main(argv, rd, arg_wr):
    out_fn = argv[1]

    xml_dir = rd / CS.xml_format

    sink = csv.writer(arg_wr(out_fn))
    sink.writerow(Term._fields)

    for doc in each_document(xml_dir):
        for t in doc_terms(doc):
            sink.writerow(t)


class CS(object):
    '''
    reference:

    Collaborative Stage Version 02.05
    (c) Copyright 2014 American Joint Committee on Cancer.
    https://cancerstaging.org/cstage/Pages/default.aspx
    '''

    # https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip  # noqa
    cs_tables = '3_CSTables(HTMLandXML).zip'

    xml_format = '3_CS Tables (HTML and XML)/XML Format/'


def each_document(xml_dir):
    targets = [target for target in xml_dir.subRdFiles()
               if target.path.endswith('.xml')]
    log.info('XML format files: %d', len(targets))

    parser = etree.XMLParser(load_dtd=True)
    parser.resolvers.add(LAResolver(xml_dir))

    for f in targets:
        log.debug("document: %s", f)
        doc = etree.parse(f.inChannel(), parser)
        yield doc.getroot()


def doc_terms(doc_elt):
    '''Extract i2b2 terms from CS Tables XML document.

    :param doc_elt: root element of XML document from CS Tables
    :return: generator of terms in doc

    >>> from pkg_resources import resource_string
    >>> text = resource_string(__name__, 'cstable_ex.xml')

    >>> doc = etree.fromstring(text)
    >>> terms = list(doc_terms(doc))
    >>> for t in terms:
    ...     print t.c_fullname
    \i2b2\Cancer Cases\CS Terms\Breast\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\000\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\001-988\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\989\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\990\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\991\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\992\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\993\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\994\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\995\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\996\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\997\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\998\
    \i2b2\Cancer Cases\CS Terms\Breast\ROLE_TUMOR_SIZE_aab\999\

    '''
    log.debug("root tag: %s", doc_elt.tag)
    log.debug('document: %s', etree.tostring(doc_elt, pretty_print=True))

    maintitle, sitesummary, tables = doc_info(doc_elt)
    log.debug('title: %s summary: %s',
              maintitle, sitesummary)
    parts = ['Cancer Cases', 'CS Terms', sitesummary or maintitle]
    yield I2B2MetaData.term(pfx=['', 'i2b2'],
                            parts=parts, viz='FAE',
                            name=maintitle)

    for table in tables:
        tt, sub_parts, rows = table_term(table, parts)
        if tt:
            yield tt

        for row in rows:
            rt = row_term(row, sub_parts)
            if rt:
                yield rt


def maybeNode(nodes):
    return nodes[0] if len(nodes) > 0 else None


def doc_info(doc_elt):
    title = doc_elt.xpath('schemahead/title')[0]
    tables = doc_elt.xpath('cstable')
    return (title.xpath('maintitle/text()')[0],
            maybeNode(title.xpath('sitesummary/text()')),
            tables)


def table_term(table, parts):
    segment = '%s_%s' % (table.attrib['role'], table.attrib['tableid'])
    name = table.xpath('tablename')[0]
    title = name.xpath('tabletitle/text()')
    subtitle = name.xpath('tablesubtitle//text()')
    log.debug('tabletitle: %s tablesubtitle: %s',
              title, subtitle)

    parts = parts + [segment]
    return (I2B2MetaData.term(pfx=['', 'i2b2'],
                              parts=parts, viz='FAE',
                              name=(subtitle + title)[0]),
            parts,
            table.xpath('row'))


def row_term(row, parts):
    code = maybeNode(row.xpath('code/text()'))
    descrip = maybeNode(row.xpath('descrip/text()'))
    log.debug('code: %s descrip: %s',
              code, descrip)
    if not (code and descrip):
        return None

    return I2B2MetaData.term(pfx=['', 'i2b2'],
                             parts=parts + [code], viz='LAE',
                             name=descrip)


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
