'''ccterms -- make i2b2 terms from Collaborative Staging tables
especially ER/PR status for breast cancer.

Usage::
???@@TODO

'''


import zipfile
import xml.etree


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


def h(thezip):
    contents = thezip.namelist()
    #print 'the zip file has this many doodads:', len(contents)

    good = [name
          for name in contents
          if name.startswith(CS.xml_format)]

    return good

# ZipFile objects
mydownloads = 'C:\\Users\\j189c736\\Downloads\\'
myzip = zipfile.ZipFile(mydownloads + CS.cs_tables)
print 'CS zipfile', myzip

print 'XML format files:', len(h(myzip))
