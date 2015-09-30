# see Makefile for usage

import sys
import csv
from collections import namedtuple
from string import strip

Item = namedtuple('Item', 'start, end, length, num, name, section, note')


def main(argv,
         spec_text_filename='naaccr12_1.txt',
         schema='NAACR',
         table='EXTRACT',
         view='EXTRACT_EAV',
         ctl='naaccr_extract.ctl',
         ddl='naaccr_extract.sql'):
    spec = list(grok_schema(open(spec_text_filename)))
    write_iter(open(ctl, "w"), make_ctl(spec, table, schema))
    ddlfp = open(ddl, "w")
    write_iter(ddlfp, table_ddl(spec, table, schema))
    ddlfp.write(';\n')
    write_iter(ddlfp, eav_view_ddl(spec, table, view, schema))
    ddlfp.write(';\n')


def explore(record_layout_filename, data_filename):
    import pprint
    naaccr_schema = to_schema(open(record_layout_filename))
    pprint.pprint(naaccr_schema)

    lines = open(data_filename)
    for line in lines:
        cols = parse_record(line, naaccr_schema)
        record = dict([(k, v)
                       for (k, v) in zip([(i.num, i.name)
                                          for i in naaccr_schema], cols)
                       if v])
        print
        print '==============='
        print
        pprint.pprint(record)


def to_schema(infp):
    rows = csv.reader(infp)
    rows.next()  # skip header
    return [Item._make(map(int, [start, end, length or 0,
                                 num.replace(',', '')]) +
                       map(strip, [name, section, note]))
            for (start, end, length, num, name, section, note) in
            [(row[0].split('-') + row[1:]) for row in rows]]


def grok_schema(infp):
    # oops... use Item
    meta = ('Column #', 'Length', 'Item #', 'Item Name', 'Section', 'Note')

    # skip to record layout table
    for line in infp:
        if [c for c in meta if c not in line]:
            continue
        # print "found record layout table"
        break

    for line in infp:
        if line.startswith('CHAPTER VIII:'):
            break
        elif 'Chapter VII:  Record Layout Table' in line:
            continue
        elif line.strip() and line.strip()[0].isdigit():
            yield grok_item(line)
        # else:
        #    print "skipping: ", line.strip()


def grok_item(txt):
    r'''
      >>> grok_item('1-1   1  10  Record Type  Record ID \n')
      ... # doctest: +NORMALIZE_WHITESPACE
      Item(start=1, end=1, length=1, num=10, name='Record Type',
           section='Record ID', note=None)

      >>> grok_item('428-433   6  135  Census Tract 2010  Demographic  New')
      ... # doctest: +NORMALIZE_WHITESPACE
      Item(start=428, end=433, length=6, num=135, name='Census Tract 2010',
           section='Demographic', note='New')

    @raises IndexError on unknown section
    '''

    def match_tail(tails):
        for t in tails:
            if txt.endswith(t):
                return txt[:-len(t)].strip(), t
        else:
            return txt, None

    txt = txt.strip()
    txt, note = match_tail(('New', 'Revised', 'Group', 'Subfield'))

    txt, section = match_tail((
        'Record ID', 'Demographic',
        'Cancer Identification',
        'Hospital-Specific', 'Stage/Prognostic Factors',
        'Treatment-1st Course',
        'Treatment-Subsequent & Other',
        'Edit Overrides/Conversion History/System Admin',
        'Follow-up/Recurrence/Death', 'Special Use',
        'Patient-Confidential', 'Hospital-Confidential', 'Other-Confidential',
        'Pathology', 'Text-Diagnosis', 'Text-Treatment',
        'Text-Miscellaneous'))
    if not section:
        raise ValueError('unknown section: ' + txt)

    cols, length, num, name = txt.split(None, 3)
    s, e = cols.split('-')
    return Item(int(s), int(e), int(length),
                int(num) if num != 'Reserved' else None,
                name.strip(), section, note)


def make_ctl(spec, table, schema):
    yield '''LOAD DATA
TRUNCATE
INTO TABLE "%s"."%s" (
''' % (schema, table)

    # itertools join, perhaps?
    yield ',\n'.join([
        '"%s" position(%d:%d) CHAR' % (i.name, i.start, i.end)
        for i in spec if i.length])

    yield ")\n"


def table_ddl(spec, table, schema):
    yield 'create table "%s"."%s" (\n' % (schema, table)
    yield ',\n'.join(['"%s" varchar2(%d)' % (i.name, i.length)
                      for i in spec if i.length])
    yield '\n)\n'


def eav_view_ddl(spec, table, view, schema):
    yield 'create or replace view "%s"."%s" as \n' % (schema, view)
    for item in spec:
        if (item.length < 1 or
                not item.num):
            continue
        if item.num != 10:
            yield '\nunion all\n'
        yield 'select "Accession Number--Hosp", "Sequence Number--Hospital", \n'
        yield '%s as ItemNbr,\n' % item.num
        yield '\'%s\' as ItemName,\n' % item.name
        yield '"%s" as value\n' % item.name
        yield 'from "%s"."%s"\n' % (schema, table)


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
    main(sys.argv)
