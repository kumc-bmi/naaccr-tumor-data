'''
Usage:

  python csvdb.py --dump csv_dir_name db_filename
or
  python csvdb.py --load csv_dir_name db_filename
or
  python csvdb.py --initz zip_file_name db_filename
or
  python csvdb.py --loadz zip_file_name db_filename
or
  python csvdb.py --queryz zip_file_name sql_query

Design note: this is a single-file script with no dependencies
outside the python stdlib.

'''
import csv
import fnmatch
from io import TextIOWrapper
from posixpath import join as posix_join, basename, splitext
from zipfile import ZipFile
import logging

log = logging.getLogger(__name__)
RAM = ':memory:'


def main(argv, stdout, stderr, cwd, basename, connect):
    '''See usage above.

    :type argv: list[str]
    :param Path cwd: access to working files
    :param connect: access to DB of a path
    :type connect: (Path) -> Connection
    '''
    [flag, csv_dir_name, db_filename] = argv[1:4]
    csv_storage = cwd / csv_dir_name
    db = connect(RAM) if flag == '--queryz' else connect(cwd / db_filename)

    logging.basicConfig(level=logging.DEBUG if '--verbose' in argv
                        else logging.INFO)
    if flag == '--dump':
        dump_db(db, csv_storage)
    elif flag == '--load':
        load_db(db, csv_storage)
    elif flag == '--initz':
        initz_db(db, csv_storage)
    elif flag == '--loadz':
        loadz_db(db, csv_storage, basename(csv_dir_name))
    elif flag == '--queryz':
        log.setLevel(logging.WARN)
        loadz_db(db, csv_storage, basename(csv_dir_name))
        query = db_filename
        run_query(db, query, stdout)
    else:
        print(__doc__, file=stderr)
        raise SystemExit(1)


def run_query(db, query, out):
    cur = db.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    if not rows:
        raise IOError('query returned no rows')
    header = [d[0] for d in cur.description]
    widths = [
        (name, max(len(name),
                   max(len(str(row[col])) for row in rows)))
        for (col, name) in enumerate(header)
    ]
    sep = ['-' * w for (_, w) in widths]
    fmt = '  '.join('{{{ix}:{w}}}'.format(w=w, ix=ix)
                    for (ix, (_, w)) in enumerate(widths))
    fmt_record = lambda row: fmt.format(*row)  # noqa
    print(fmt_record(header), file=out)
    print(fmt_record(sep), file=out)
    for row in rows:
        print(fmt_record(row), file=out)


def initz_db(db, csv_storage):
    zstore = zip_path(csv_storage)
    work = db.cursor()
    for data in zstore.glob('*/*.csv'):
        name = data.stem
        fields = next(csv.reader(data.open()))
        col_specs = ', '.join(f'"{col}" text' for col in fields)
        ddl = f'create table "{name}" ({col_specs})'
        log.info('create %s for %s', name, data)
        work.execute(f'drop table if exists "{name}"')
        work.execute(ddl)


def loadz_db(db, csv_storage, csv_dir_name):
    zstore = zip_path(csv_storage)
    to_path = {data.stem: data
               for data in zstore.glob('*/*.csv')}
    load_db(db, zip_path(csv_storage),
            find_data=lambda t: to_path[t])


def load_db(dest, src,
            setup=None,
            find_data=None):
    '''Initialize DB and for each table, load data from CSV file, if available.

    :param Connection dest:
    :param Path src: directory of CSV files
    :param setup: schema set-up scripts to run before loading tables
    :type setup: list[Path] | None
    '''
    if setup is None:
        setup = [(src / 'schema.sql')]
    if find_data is None:
        find_data = lambda t: src / (t.lower() + '.csv')  # noqa

    def get_rows(table):
        data = find_data(table)
        if data.exists():
            rows = csv.reader(data.open())
            return next(rows), rows
        else:
            log.debug('skipping %s: no data file', table)
            return [], []
    load_tables(dest, get_rows, setup)


def load_tables(dest, get_rows, setup):
    '''Initialize DB and load rows

    :type dest: Connection
    :type get_rows: (str) -> iter[tuple[str]]
    :type setup: list[Path]
    '''
    for script in setup:
        if script.exists():
            log.info('running setup script: %s', script)
            dest.executescript(script.open(mode='r').read())

    for table in db_tables(dest):
        header, rows = get_rows(table)
        log.debug('calling load_table(%s)', table)
        load_table(dest, table, header, rows)


def db_tables(db):
    '''
    :type db: Connection
    :rtype: list[str]
    '''
    work = db.cursor()
    work.execute(
        "select name from sqlite_master where type = 'table'")
    tables = [name for (name,) in work.fetchall()]
    log.info('found %s tables...', len(tables))
    return tables


def load_table(dest, table, header, rows,
               batch_size=500):
    '''Load rows of a table in batches.

    :type dest: Connection
    :type table: str
    :type header: list[str]
    :param rows: data to load
    :type batch_size: int
    :type rows: iter[list[str]]
    '''
    log.info('loading %s', table)
    work = dest.cursor()
    stmt = """
      insert into "{table}" ({header})
      values ({placeholders})""".format(
        table=table,
        header=', '.join(f'"{col}"' for col in header),
        placeholders=', '.join('?' for _ in header)
    )
    log.debug('%s', stmt)
    batch = []

    def do_batch():
        work.executemany(stmt, batch)
        log.info('inserted %s rows into %s', len(batch), table)
        del batch[:]

    for row in rows:
        if row:  # skip blank row at end
            blank2null = [None if val == '' else val for val in row]
            batch.append(blank2null)
        if len(batch) >= batch_size:
            do_batch()
    if batch:
        do_batch()
    dest.commit()


def dump_db(db, csv_storage,
            chunk_size=1000):
    '''
    :type db: Connection
    :param Path csv_storage: directory in which to stor .csv files
    :type chunk_size: int
    '''
    for table in db_tables(db):
        with (csv_storage / (table.lower() + '.csv')).open(mode='wb') as out:
            formatter = csv.writer(out)
            q = db.cursor()
            q.execute("select * from {table}".format(table=table))
            header = [name for (name, _2, _3, _4, _5, _6, _7) in q.description]
            formatter.writerow(header)
            while 1:
                chunk = q.fetchmany(chunk_size)
                if not chunk:
                    break
                formatter.writerows(chunk)


class Path(object):
    '''Just the parts of the pathlib API that we use.

    :type joinpath: (str) -> Path
    :type open: (...) -> Path
    :type exists: () -> bool
    '''
    def __init__(self, here, ops):
        '''
        :param str here:
        '''
        io_open, path_join, path_exists, glob = ops
        make = lambda there: Path(there, ops)  # noqa
        self.joinpath = lambda there: make(path_join(here, there))
        self.open = lambda **kwargs: io_open(here, **kwargs)
        self.exists = lambda: path_exists(here)
        self.glob = lambda pattern: (make(it)
                                     for it in glob(path_join(here, pattern)))
        self._path = here

    @property
    def stem(self):
        s, _ = splitext(self._path)
        return basename(s)

    def __repr__(self):
        return '{cls}(...)'.format(cls=self.__class__.__name__)

    def __str__(self):
        return self._path

    def __truediv__(self, there):
        '''
        :param str there:
        :rtype: Path
        '''
        return self.joinpath(there)


def zip_path(backing_store,
             encoding='ISO-8859-1'):
    """Adapt Zipfile to pathlib API

    Ugh! LoincIeeeMedicalDeviceCodeMappingTable.csv is ISO8859-1

    :type backing_store: Path
    """
    zbak = ZipFile(backing_store.open(mode='rb'))

    def zip_open(there, mode='r'):
        mode = mode[:1]  # zipfile supports r but not rb
        return TextIOWrapper(zbak.open(there, mode=mode),
                             encoding=encoding)

    def path_exists(there):
        return there in zbak.namelist()

    def glob(pat):
        return fnmatch.filter(zbak.namelist(), pat)

    return Path('', (zip_open, posix_join, path_exists, glob))


class Connection(object):
    '''type stubs from DBAPI
    '''
    def cursor(self):
        '''
        :rtype: Cursor
        '''
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def executescript(self, script):
        '''
        :param str script:
        :rtype: None
        '''
        raise NotImplementedError


class Cursor(object):
    def __init__(self):
        self.description = ('', '', '', '', '', '', '')
        raise NotImplementedError

    def execute(self, sql):
        '''
        :param str sql:
        :rtype: None
        '''
        raise NotImplementedError

    def executemany(self, sql, records):
        '''
        :type sql: str
        :type records: list[list[object]]
        :rtype: None
        '''
        raise NotImplementedError

    def fetchall(self):
        '''
        :rtype: list[list[str | int]]
        '''
        raise NotImplementedError

    def fetchmany(self, qty=1000):
        '''
        :rtype: list[list[str | int]]
        '''
        raise NotImplementedError


if __name__ == '__main__':
    def _privileged_main():
        from glob import glob
        from io import open as io_open
        import os.path as os_path
        from sqlite3 import connect
        from sys import argv, stdout, stderr

        main(argv, stdout, stderr,
             cwd=Path('.', (io_open, os_path.join, os_path.exists, glob)),
             basename=os_path.basename,
             connect=lambda path: connect(str(path)))

    _privileged_main()
