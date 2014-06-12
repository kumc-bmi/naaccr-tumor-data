'''ccterms -- make i2b2 terms from Collaborative Staging tables
especially ER/PR status for breast cancer.

Usage::

first, download and unzip 3_CSTables(HTMLandXML).zip

???@@TODO

'''


import os

# on windows, via conda/anaconda
from lxml import etree


def main():
    all_files = os.listdir(CS.xml_format)
    names = xml_items(all_files)
    print 'XML format files:', len(names)
    xstuff(names)


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


def xml_items(contents):
    good = [name
          for name in contents
          if name.endswith('.xml')]

    return good


def xstuff(names):
    os.chdir(CS.xml_format)

    parser = etree.XMLParser(load_dtd=True)
    for name in names:
        print "document:", name
        text_stream = open(name)
        tree = etree.parse(text_stream, parser)
        root = tree.getroot()
        print name, root.tag, root.attrib
        print(etree.tostring(root, pretty_print=True))
        break


if __name__ == '__main__':
    main()
