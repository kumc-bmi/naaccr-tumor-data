
import csv
from collections import namedtuple
from string import strip

Item = namedtuple('Item', 'start, end, length, num, name, section, note')

def to_schema(infp):
    rows = csv.reader(infp)
    header = rows.next()
    return [map(int, [start, end, length or 0, num.replace(',' ,'')])
            + map(strip, [name, section, note])
            for (start, end, length, num, name, section, note) in
            [(row[0].split('-') + row[1:]) for row in rows]]

def parse_data_dict(line):
    '''
      >>> parse_data_dict('1-1 1 10 Record Type')
      Item(start=1, end=1, length=1, num=10, name='Record Type', section=None, note=None)

    .. todo: handle section, not
    '''
    cols, length, num, rest = line.split(' ', 3)
    start, end = cols.split('-')
    return Item(int(start), int(end), int(length), int (num), rest, None, None)


def parse_record(line, schema):
    '''
      >>> n=Item(1, 3, 3, 0, 'n', None, None)
      >>> s=Item(5, 8, 3, 1, 's', None, None)
      >>> parse_record('123 xyz', [n, s])
      ['123', 'xyz']
    '''
    return [line[i.start-1:i.end] for i in schema]


if __name__ == '__main__':
    import sys, pprint
    record_layout_filename = sys.argv[1]
    pprint.pprint(to_schema(open(record_layout_filename)))
