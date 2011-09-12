
import sys
import csv
from collections import namedtuple
from string import strip

Item = namedtuple('Item', 'start, end, length, num, name, section, note')


def main(argv,
         record_layout_filename='record_layout.csv',
         schema='NAACR',
         table='EXTRACT',
         ctl='naacr_extract.ctl',
         ddl='naacr_extract.sql'):
    #record_layout_filename = argv[1]
    spec = to_schema(open(record_layout_filename))
    write_iter(open(ctl, "w"), make_ctl(spec, table, schema))
    write_iter(open(ddl, "w"), table_ddl(spec, table, schema))

    
def explore(record_layout_filename, data_filename):
    import pprint
    naaccr_schema = to_schema(open(record_layout_filename))
    pprint.pprint(naaccr_schema)

    lines = open(data_filename)
    for line in lines:
        cols = parse_record(line, naaccr_schema)
        record = dict(zip([i.name for i in naaccr_schema], cols))
        print
        print '==============='
        print
        pprint.pprint(record)


def to_schema(infp):
    rows = csv.reader(infp)
    header = rows.next()
    return [Item._make(map(int, [start, end, length or 0, num.replace(',' ,'')])
                       + map(strip, [name, section, note]))
            for (start, end, length, num, name, section, note) in
            [(row[0].split('-') + row[1:]) for row in rows]]



def make_ctl(spec, table, schema):
    yield '''LOAD DATA
APPEND
INTO TABLE "%s"."%s" (
''' % (schema, table)

    # itertools join, perhaps?
    yield ',\n'.join([
            '"%s" position(%d:%d) CHAR' % (i.name, i.start, i.end)
            for i in spec if i.length])

    yield ")\n"


def table_ddl(spec, table, schema):
    yield 'create table "%s"."%s" (\n' % (schema, table)
    first = True
    yield ',\n'.join(['"%s" varchar2(%d)' % (i.name, i.length)
                      for i in spec if i.length])
    yield '\n)\n'


def write_iter(outfp, itr):
    for chunk in itr:
        outfp.write(chunk)


def parse_record(line, schema):
    '''
      >>> n=Item(1, 3, 3, 0, 'n', None, None)
      >>> s=Item(5, 8, 3, 1, 's', None, None)
      >>> parse_record('123 xyz', [n, s])
      ['123', 'xyz']
    '''
    return [line[i.start-1:i.end].strip() for i in schema]


if __name__ == '__main__':
    #record_layout_filename, data_filename = sys.argv[1:3]
    #explore(record_layout_filename, data_filename)
    main(sys.argv)

