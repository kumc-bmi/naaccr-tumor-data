'''naaccr_prompts -- normalize NAACCR prompt data
'''

import csv
import logging

log = logging.getLogger(__name__)


def main(about_items, get_aux, stdout, level=logging.DEBUG):
    logging.basicConfig(level=level)
    t = normalize(about_items, get_aux)
    csv.writer(stdout).writerows(t)


def normalize(about_items, get_aux):
    items = csv_table(about_items)
    text_items = [item for item in items
                  if item['Prompting'] == 'T']
    value_maps = [(item['NAACCR_Item_No'], item['FieldName'],
                   csv_table(get_aux(item['PromptingValue'])))
                  for item in text_items]
    return [('NAACCR_Item_No', 'FieldName', '@@code', '@@label')] + [
        (no, name, term[0], term[1])
        for (no, name, terms) in value_maps
        for term in terms]


def csv_table(fp):
    return list(csv.DictReader(fp))


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
