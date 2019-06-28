"""naaccr_dd_scrape: scrape from HTML format NAACCR data dictionary

Usage:

  $ scrape.py
  INFO:no store at ?c=7; fetching: http://datadictionary.naaccr.org/?c=7
  INFO:record_layout.csv: 802 items
  INFO:no store at ?c=10; fetching: http://datadictionary.naaccr.org/?c=10
  INFO:descriptions.csv: 890 items

Chapter VII: Record Layout Table (Column # Order)
http://datadictionary.naaccr.org/?c=7

Chapter X: Data Dictionary
http://datadictionary.naaccr.org/?c=10
"""

from collections import namedtuple
from html.parser import HTMLParser
from urllib.parse import urljoin
from xml.etree import ElementTree as ET
import csv
import logging
import re

log = logging.getLogger(__name__)

URL = 'http://datadictionary.naaccr.org/'


def main(argv, stderr, cwd, urlopener):
    cache = WebCache(URL, urlopener, cwd)

    ddl = []
    for cls in [
            RecordLayout,
            DataDescriptor,
            ItemDescription,
    ]:
        cls.scrape(cache / ('?c=%d' % cls.chapter),
                   cwd / cls.filename)
        ddl.append(cls.create_ddl())

    with (cwd / 'schema.sql').open('w') as fp:
        fp.write('\n'.join(s + ';\n' for s in ddl))


class WebCache(object):
    def __init__(self, addr, urlopener, store):
        def joinpath(ref):
            there = urljoin(addr, ref)
            return WebCache(there, urlopener, store / ref)

        self.joinpath = joinpath

        def open(mode='r'):
            if not mode.startswith('r'):
                raise IOError(mode)
            if not store.exists():
                log.info('no store at %s; fetching: %s', store, addr)
                content = urlopener.open(addr).read()
                store.open('wb').write(content)
            return store.open(mode=mode)

        self.open = open

    def __truediv__(self, there):
        return self.joinpath(there)


def csv_export(dest, cols, rows):
    qty = 0
    with dest.open('w') as fp:
        data = csv.writer(fp)
        data.writerow(cols)
        for row in rows:
            data.writerow(row)
            qty += 1

    return qty


class PageData(object):
    @classmethod
    def scrape(cls, src, dest):
        toSave = cls.scrapeDoc(Builder.doc(src.open()))
        saved = csv_export(dest, cls._fields, toSave)
        log.info('%s: %d items', dest, saved)

    @classmethod
    def _table_rows(cls, doc,
                    tpath='body/form/div[@id="Panel2"]/table'):
        trs = doc.findall(tpath + '/tbody/tr')
        for tr in trs:
            tds = tr.findall('td')
            yield [_text(td) for td in tds]

    int_fields = []

    @classmethod
    def create_ddl(cls):
        cols = [(f, 'int' if f in cls.int_fields else 'text')
                for f in cls._fields]
        coldefs = ['  %s %s' % (n, ty)
                   for n, ty in cols]
        return 'create table {name} (\n{coldefs}\n)'.format(
            name=snake_case(cls.__name__),
            coldefs=',\n'.join(coldefs),
        )


def snake_case(n):
    """
    >>> snake_case(DataDescriptor.__name__)
    'data_descriptor'
    """
    return (n[:1] + re.sub('([A-Z])', r'_\1', n[1:])).lower()


class DataDescriptor(PageData, namedtuple(
        'DataDescriptor',
        # Item #	Item Name
        # Format	Allowable Values
        # Length	Note
        ['item', 'name', 'format', 'allow_value', 'length', 'note'])):
    """
    >>> print(DataDescriptor.create_ddl())
    create table data_descriptor (
      item int,
      name text,
      format text,
      values text,
      length int,
      note text
    )
    """
    chapter = 9
    filename = 'data_descriptor.csv'
    int_fields = ['item', 'length']

    @classmethod
    def scrapeDoc(cls, doc):
        for fields in cls._table_rows(doc):
            if len(fields) != len(cls._fields):
                # print(tds)
                continue
            yield cls(*fields)


class RecordLayout(PageData, namedtuple(
        'RecordLayout',
        # Column #	Length	Item #	Item Name
        #   XML NAACCR ID	PARENT XML ELEMENT
        #   Section	Note
        ['start', 'end', 'length',
         'item', 'name', 'xmlId', 'parentTag',
         'section', 'note'])):

    chapter = 7
    filename = 'record_layout.csv'
    int_fields = ['start', 'end', 'length', 'item']

    @classmethod
    def scrapeDoc(cls, doc):
        for fields in cls._table_rows(doc):
            if len(fields) + 1 != len(cls._fields):
                # print(tds)
                continue
            fields[:1] = fields[0].replace(' ', '').split('-')
            yield cls(*fields)


class ItemDescription(PageData, namedtuple(
        'ItemDescription', ['item', 'xmlId', 'parentTag', 'description'])):

    chapter = 10
    filename = 'item_description.csv'

    @classmethod
    def scrapeDoc(cls, doc):
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
        from urllib.request import build_opener

        logging.basicConfig(
            level=logging.DEBUG if '--verbose' in argv
            else logging.INFO)
        main(argv[:], stderr, Path('.'), build_opener())

    _script()
