"""tumor_table - write PCORNet CDM style tumor table definition

Usage:

To write fields, valuesets in CSV:

  python tumor_table.py data-raw out/

Where data-raw is from https://github.com/WerthPADOH/naaccr

"""

import re

# See requirements.txt, CONTRIBUTING.md
import pandas as pd


def main(argv, cwd):
    [data, out] = argv[1:3]

    v18 = RecordFormat(cwd / data)
    out = cwd / out
    if not out.exists():
        out.mkdir()
    v18.fields(cwd / data).to_csv((out / 'fields.csv').open('w'))
    v18.valuesets(cwd / data).to_csv((out / 'valuesets.csv').open('w'))


class RecordFormat(object):
    def __init__(self, data_raw,
                 version=18):
        self.items = pd.read_csv(
            data_raw / 'record-formats' / ('version-%s.csv' % version)
        ).set_index('item')
        info = pd.read_csv(
            data_raw / 'field_info.csv').set_index('item')
        self.info = self.items.merge(info,
                                     left_index=True, right_index=True)

    table_name = 'TUMOR'
    description = '''
    tumor stuff...@@@
    '''

    def domains(self):
        return pd.Series(dict(
            TABLE_NAME=self.table_name,
            # TODO: DOMAIN_DESCRIPTION=self.description,
            DOMAIN_ORDER=-1,
        ))

    # TODO: RELATIONAL: table_name, relation, integrity details, order
    # TODO: constraints

    def fields(self, data_raw):
        fields = pd.DataFrame({
            'TABLE_NAME': self.table_name,
            'FIELD_NAME': self.info.name.apply(upper_snake_case),
            # TODO: 'RDBMS_DATA_TYPE': self.info.type,
            # TODO: 'SAS_DATA_TYPE': self.info.type,
            # TODO: 'DATA_FORMAT': self.field_info.type,
            # TODO: 'REPLICATED_FIELD': 'NO',
            # TODO: 'UNIT_OF_MEASURE': '',
            # TODO: 'FIELD_DEFINITION': 'TODO',
        })
        fields['FIELD_ORDER'] = range(1, len(fields) + 1)
        fields = fields.set_index(['TABLE_NAME', 'FIELD_NAME'])\
                       .sort_values('FIELD_ORDER')
        vals = self.valuesets(data_raw).reset_index()
        vals = vals.groupby(['TABLE_NAME', 'FIELD_NAME'])
        fields['VALUESET'] = vals.VALUESET_ITEM.apply(';'.join)
        desc = vals.VALUESET_ITEM_DESCRIPTOR.apply(';'.join)
        fields['VALUESET_DESCRIPTOR'] = desc
        print(fields.columns)
        return fields

    def valuesets(self, data_raw):
        found = []
        for info in (data_raw / 'code-labels').glob('*.csv'):
            skiprows = 0
            if info.open().readline().startswith('#'):
                skiprows = 1
            codes = pd.read_csv(info, skiprows=skiprows,
                                na_filter=False,
                                dtype={'code': str, 'label': str})
            if 'code' not in codes.columns or 'label' not in codes.columns:
                raise ValueError((info, codes.columns))
            codes['TABLE_NAME'] = self.table_name
            codes['FIELD_NAME'] = info.stem
            codes['VALUESET_ITEM'] = codes.code
            codes['VALUESET_ITEM_DESCRIPTOR'] = codes.code + '=' + codes.label
            found.append(codes)
        vals = pd.concat(found)
        vals = vals.set_index(['TABLE_NAME', 'FIELD_NAME', 'VALUESET_ITEM'])
        return vals[['VALUESET_ITEM_DESCRIPTOR', 'means_missing']]


def upper_snake_case(camel):
    """
    >>> upper_snake_case('dateOfBirth')
    'DATE_OF_BIRTH'
    """
    return re.sub('([A-Z])', r'_\1', camel).upper()


if __name__ == '__main__':
    def _script():
        from pathlib import Path
        from sys import argv

        main(argv[:], Path('.'))

    _script()
