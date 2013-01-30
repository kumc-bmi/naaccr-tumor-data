'''naaccr_prompts -- normalize NAACCR prompt data
'''

import csv
import logging

log = logging.getLogger(__name__)


def main(about_items, get_aux, stdout, level=logging.INFO):
    logging.basicConfig(level=level)
    dd = DataDict(about_items, get_aux)
    csv.writer(stdout).writerows(dd.normalize())


class DataDict(object):
    def __init__(self, about_items, get_aux):
        self.__about_items = about_items
        self.__get_aux = get_aux

    def normalize(self):
        items = csv_table(self.__about_items)
        text_items = [item for item in items
                      if item['prompting'] == 'T'
                      and item['promptingvalue']]
        value_maps = [(item['naaccr_item_no'], item['fieldname'],
                       self.get_map(item))
                      for item in text_items]
        return [('naaccr_item_no', 'fieldname', 'value', 'label')] + [
            (no, name, term['value'], term['label'])
            for (no, name, terms) in value_maps
            for term in terms]

    def get_map(self, item):
        return csv_table(self.__get_aux(item['promptingvalue']))


def csv_table(fp):
    t = list(csv.DictReader(fp))
    return [dict([(k.lower(), v) for k, v in row.iteritems()])
            for row in t]


if __name__ == '__main__':
    def _initial_caps():
        import sys
        import os

        items_fn, aux_dir = sys.argv[1:3]

        def get_aux(n):
            # TODO: only allow local n
            log.debug('get_aux(%s)', n)
            return open(os.path.join(aux_dir, n.lower() + '.csv'))

        return dict(about_items=open(items_fn),
                    get_aux=get_aux,
                    stdout=sys.stdout)

    main(**_initial_caps())
