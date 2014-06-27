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
from i2b2mdmk import I2B2MetaData

log = logging.getLogger(__name__)


def main(argv, rd, arg_wr):
    out_fn = argv[1]

    xml_dir = rd / CS.xml_format

    sink = csv.writer(arg_wr(out_fn))

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

        # For now, let's just parse one file.
        if not f.path.endswith('Breast.xml'):  #@@
            continue

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
    >>> terms[:2]
    ['@@TABLE THINGY', '@@ROW THINGY']

    '''
    log.debug("root tag: %s", doc_elt.tag)
    log.debug('document: %s', etree.tostring(doc_elt, pretty_print=True))

    title, maintitle, sitesummary, tables = doc_info(doc_elt)
    log.debug('title: %s summary: %s',
              maintitle, sitesummary)

    for table in tables:
        tt, rows = table_term(table, maintitle)
        if tt:
            yield tt

        for row in rows:
            yield row_term(row)


def maybeNode(nodes):
    return nodes[0] if len(nodes) > 0 else None


def doc_info(doc_elt):
    title = doc_elt.xpath('schemahead/title')[0]

    return (title,
            title.xpath('maintitle/text()'),
            title.xpath('sitesummary/text()'),
            doc_elt.xpath('cstable'))


def table_term(table, maintitle):
    name = table.xpath('tablename')[0]
    title = name.xpath('tabletitle/text()')
    subtitle = name.xpath('tablesubtitle//text()')
    log.debug('tabletitle: %s tablesubtitle: %s',
              title, subtitle)

    return ((I2B2MetaData.term(pfx=['', 'i2b2'],
                               parts=['Cancer Cases', 'CS Terms'],
                               name=subtitle[0]), table.xpath('row'))
            if subtitle else (None, []))


def row_term(row):
    code = maybeNode(row.xpath('code/text()'))
    descrip = maybeNode(row.xpath('descrip/text()'))
    log.debug('code: %s descrip: %s',
              code, descrip)
    return I2B2MetaData.term(pfx=['', 'i2b2'],
                             parts=['Cancer Cases', 'CS Terms'],
                             name=descrip)


class LAResolver(etree.Resolver):
    '''Resolve entity references in context of a :class:`lafile.Rd`.
    '''
    def __init__(self, rd):
        def resolve(url, name, context):
            #log.debug('resolving: %s', url)
            return self.resolve_file(rd.subRdFile(url).inChannel(), context)

        self.resolve = resolve


if __name__ == '__main__':
    def _configure_logging(level=logging.DEBUG):
        logging.basicConfig(level=level)

    def _trusted_main():
        from sys import argv
        import os

        rd = osRd(os.curdir, lambda n: open(n), os.path, os.listdir)

        def arg_wr(n):
            if n not in argv: raise IOError
            return open(n, 'w')

        main(argv, rd, arg_wr)

    _configure_logging()
    _trusted_main()
