from os import path
from django.shortcuts import render
import time
from .models import all_tables, insert_directly, update_directly, delete_directly, CargoType


# def index(request):
#     """View function for home page of site."""
#
#     num_cargo_types = CargoType.objects.all().count()
#     context = {
#         'num_cargo_types': num_cargo_types,
#     }
#
#     return render(request, 'index.html', context=context)


def imported(request):
    """View function for home page of site."""
    from .apps import DictsConfig
    import logging

    logger = logging.getLogger('impex.dicts')
    logger.info(time.strftime('%H:%M:%S'))
    # logger = logging.getLogger('impex.dicts')
    BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
    file_path = path.join(BASE_DIR, DictsConfig.name, 'data')
    report_added = []
    errors_list = []

    for obj in all_tables():
        external_name = obj.external_name
        current_dict = {c.key_number: c.csv_hash() for c in obj.objects.all()}
        current = set(current_dict.values())
        current_keys = set(current_dict.keys())

        exported = set()
        exported_keys = set()
        with open(f'{file_path}\{external_name}.gof') as fr:
            i = 0
            for line in fr:
                i = i + 1
                if i < 4 or line[0] == '^' or line == '\n': continue
                record = [r.strip() for r in line.split('~')]

                if len(record) < 3:
                    errors_list.append(';'.join([external_name, line]))
                    continue
                while len(record) < 5:
                    record.append('')

                exported.add(';'.join((record[0], record[1], record[2], record[3], record[4],)))
                exported_keys.add(record[0])

        deleted = current_keys.difference(exported_keys)
        delete_list = tuple(deleted)
        delete_directly(obj._meta.db_table, delete_list)

        insert_list = []
        update_list = []
        difference = exported.difference(current)
        if difference:
            ts = (str(int(time.time())),)

            for line in difference:
                data = tuple(line.split(';')) + ts
                if data[0] in current_keys:
                    update_list.append(data[1:]+(data[0],))
                else:
                    insert_list.append(data)

            update_directly(obj._meta.db_table, update_list)
            insert_directly(obj._meta.db_table, insert_list)

        report_added.append([f'{obj._meta.db_table} ({external_name})', len(insert_list), len(update_list), len(delete_list)])
        logger.info(report_added[-1])
        logger.info(time.strftime('%H:%M:%S'))

    context = {
        'caption': 'Импорт справочников',
        'report': report_added,
    }

    return render(request, 'dicts.html', context=context)
