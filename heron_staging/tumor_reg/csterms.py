'''ccterms -- do something cools @@TODO

Usage::
???@@TODO

'''


import zipfile
import xml.etree


def h(thezip):
    contents = thezip.namelist()
    #print 'the zip file has this many doodads:', len(contents)

    xml_format = '3_CS Tables (HTML and XML)/XML Format/'
    good = [name
          for name in contents
          if name.startswith(xml_format)]

    return good

# ZipFile objects
myzip = zipfile.ZipFile('C:\\Users\\j189c736\\Downloads\\3_CSTables(HTMLandXML).zip')
print 'CS zipfile', myzip

print 'XML format files:', len(h(myzip))
