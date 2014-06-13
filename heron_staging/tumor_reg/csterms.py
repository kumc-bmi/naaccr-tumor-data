'''ccterms -- make i2b2 terms from Collaborative Staging tables
especially ER/PR status for breast cancer.

Usage::

first, download and unzip 3_CSTables(HTMLandXML).zip; see Makefile for details.

???@@TODO

'''

import logging

# on windows, via conda/anaconda
from lxml import etree

from lafile import osRd


log = logging.getLogger(__name__)


def main(rd):
    xml_dir = rd / CS.xml_format
    for doc in each_document(xml_dir):
        terms = list(doc_terms(doc))
        log.debug('@@terms doc: %s %d', doc, len(terms))


class CS(object):
    '''
    reference:

    Collaborative Stage Version 02.05
    (c) Copyright 2014 American Joint Committee on Cancer.
    https://cancerstaging.org/cstage/Pages/default.aspx
    '''

    # https://cancerstaging.org/cstage/software/Documents/3_CSTables(HTMLandXML).zip
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

    title = doc_elt.xpath('schemahead/title')[0]
    maintitle = title.xpath('maintitle/text()')
    sitesummary = title.xpath('sitesummary/text()')
    log.debug('title: %s summary: %s',
              maintitle, sitesummary)

    for table in doc_elt.xpath('cstable'):
        tablename = table.xpath('tablename')[0]
        tabletitle = tablename.xpath('tabletitle/text()')
        tablesubtitle = tablename.xpath('tablesubtitle//text()')
        log.debug('tabletitle: %s tablesubtitle: %s',
                  tabletitle, tablesubtitle)

        yield '@@TABLE THINGY'

        for row in table.xpath('row'):
            code = row.xpath('code/text()')[0]
            descrip = row.xpath('descrip/text()')[0]
            log.debug('code: %s descrip: %s',
                      code, descrip)
            yield '@@ROW THINGY'


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
        import os

        rd = osRd(os.curdir, lambda n: open(n), os.path, os.listdir)

        main(rd)

    _configure_logging()
    _trusted_main()
