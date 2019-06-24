import csv
from xml.etree import ElementTree as ET
from html.parser import HTMLParser


def main(argv, stderr, cwd):
    [page, out] = argv[1:3]
    doc = RefDoc.structure((cwd / page).open())

    items = {}
    with (cwd / out).open('w') as fp:
        data = csv.writer(fp)
        data.writerow(RefDoc.columns)
        for item in RefDoc.items(doc):
            data.writerow(item)
            items[item[0]] = item
    stderr.write('items found: %d\n' % len(items))


class RefDoc(object):

    columns = 'item xmlId parentTag description'.split()

    @classmethod
    def structure(cls, fp):
        b = Builder()
        b.feed(fp.read())
        return b.root

    @classmethod
    def items(cls, doc):
        item = xmlId = parentTag = description = None

        for section in doc.findall('body/form/div[@id="Panel2"]/*'):
            if section.tag == 'table' and section.find(
                    'tr[@class="tableColTitle"]'):
                if item:
                    yield (item, xmlId, parentTag, description)
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
            yield (item, xmlId, parentTag, description)


def _text(elt):
    return ''.join(elt.itertext())


class Builder(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.root = None
        self._stack = []
        self._texting = None
        self._tailing = None

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
