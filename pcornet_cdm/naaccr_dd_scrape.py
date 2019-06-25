"""naaccr_dd_scrape: scrape from HTML format NAACCR data dictionary

Usage:

  $ pcornet_cdm/naaccr_dd_scrape.py naaccr-ch10.html ch10.csv
  item descriptions found: 890

Chapter VII: Record Layout Table (Column # Order)
http://datadictionary.naaccr.org/?c=7

Chapter X: Data Dictionary
http://datadictionary.naaccr.org/?c=10
"""

from collections import namedtuple
from html.parser import HTMLParser
from xml.etree import ElementTree as ET
import csv


def main(argv, stderr, cwd):
    [ch10, out] = argv[1:4]

    ch10doc = Builder.doc((cwd / ch10).open())
    items = {}
    ea = csv_export(cwd / out, ItemDescription._fields,
                    ItemDescription.scrape(ch10doc),
                    gen=True)
    for item in ea:
        items[item.item] = item
    stderr.write('item descriptions found: %d\n' % len(items))


def csv_export(dest, cols, rows,
               gen=False):
    with dest.open('w') as fp:
        data = csv.writer(fp)
        data.writerow(cols)
        for row in rows:
            data.writerow(row)
            if gen:
                yield row


class ItemDescription(namedtuple(
        'ItemDescription', ['item', 'xmlId', 'parentTag', 'description'])):

    @classmethod
    def scrape(cls, doc):
        item = xmlId = parentTag = description = None

        for section in doc.findall('body/form/div[@id="Panel2"]/*'):
            if section.tag == 'table' and section.find(
                    'tr[@class="tableColTitle"]'):
                if item:
                    yield cls(item, xmlId, parentTag, description)
                    item = xmlId = parentTag = description = None
                detail = section.find('tr[@class="tableColData"]')
                item = _text(detail.find('td'))
            elif section.tag == 'table' and 'XML NAACCR ID' in _text(section):
                rows = section.findall('tr')
                xmlId = _text(rows[1].findall('td')[1])
                parentTag = _text(rows[2].findall('td')[1])
            elif section.tag == 'div' and _text(section) == 'Description':
                description = ''
            elif section.tag == 'div' and description == '':
                description = _text(section)
            else:
                # print(section.tag, section.attrib)
                pass

        if item:
            yield cls(item, xmlId, parentTag, description)


def _text(elt):
    return ''.join(elt.itertext())


class Builder(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.root = None
        self._stack = []
        self._texting = None
        self._tailing = None

    @classmethod
    def doc(cls, fp):
        b = cls()
        b.feed(fp.read())
        return b.root

    def handle_starttag(self, tag, attrs):
        if self._stack:
            parent = self._stack[-1]
            elt = ET.SubElement(parent, tag, dict(attrs))
        else:
            elt = ET.Element(tag, dict(attrs))
            self.root = elt
        self._stack.append(elt)
        self._texting = elt
        self._tailing = None

    def handle_data(self, data):
        if self._tailing is not None:
            self._tailing.tail = (self._tailing.tail or '') + data
        elif self._texting is not None:
            self._texting.text = (self._texting.text or '') + data
        else:
            if data.strip():
                raise ValueError((data, [e.tag for e in self._stack]))

    def handle_endtag(self, tag):
        while tag != self._stack[-1].tag:
            self._pop()
        self._pop()

    def _pop(self):
        # print([e.tag for e in self._stack] +
        #       [e.text for e in self._stack[-1:]])
        elt = self._stack.pop()
        self._tailing = elt
        self._texting = None


if __name__ == '__main__':
    def _script():
        from pathlib import Path
        from sys import argv, stderr

        main(argv[:], stderr, Path('.'))

    _script()
