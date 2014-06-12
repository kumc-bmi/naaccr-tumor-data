'''ccterms -- make i2b2 terms from Collaborative Staging tables
especially ER/PR status for breast cancer.

Usage::

first, download and unzip 3_CSTables(HTMLandXML).zip

???@@TODO

'''

import logging

# on windows, via conda/anaconda
from lxml import etree

from lafile import osRd


log = logging.getLogger(__name__)


def main(rd):
    xml_dir = rd / CS.xml_format
    targets = [target for target in xml_dir.subRdFiles()
               if target.path.endswith('.xml')]
    log.info('XML format files: %d', len(targets))
    xstuff(xml_dir, targets)


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


def xstuff(xml_dir, targets):
    parser = etree.XMLParser(load_dtd=True)
    parser.resolvers.add(LAResolver(xml_dir))
    for f in targets:
        if not f.path.endswith('Breast.xml'):  #@@
            continue
        log.debug("document: %s", f)
        tree = etree.parse(f.inChannel(), parser)
        root = tree.getroot()
        log.debug("root tag: %s", root.tag)
        title = root.xpath('schemahead/title')[0]
        maintitle = title.xpath('maintitle/text()')
        sitesummary = title.xpath('sitesummary/text()')
        log.debug('title: %s summary: %s',
                  maintitle, sitesummary)

        for table in root.xpath('cstable'):
            tablename = table.xpath('tablename')[0]
            tabletitle = tablename.xpath('tabletitle/text()')
            tablesubtitle = tablename.xpath('tablesubtitle//text()')
            log.debug('tabletitle: %s tablesubtitle: %s',
                      tabletitle, tablesubtitle)

            for row in table.xpath('row'):
                code = row.xpath('code/text()')[0]
                descrip = row.xpath('descrip/text()')[0]
                log.debug('code: %s descrip: %s',
                          code, descrip)


        log.debug('%s', etree.tostring(root, pretty_print=True))


class LAResolver(etree.Resolver):
    '''Resolve entity references in context of a :class:`lafile.Rd`.
    '''
    def __init__(self, rd):
        def resolve(url, name, context):
            log.debug('resolving: %s', url)
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
