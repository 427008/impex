from os import path
from django.shortcuts import render
import time
from hashlib import md5

from .models import BaseTable, all_tables
from .apps import ReceiptConfig


def imported(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.receipt')
    logger.info(time.strftime('%H:%M:%S'))

    BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
    file_path = path.join(BASE_DIR, ReceiptConfig.name, 'data')
    report_added = []
    errors_list = []

    for model in all_tables():
        table_dict = {c[0]: c[1] for c in model.objects.values_list('md5hash', 'key_number')}
        file_dict = {}

        with open(f'{file_path}\{model.external_name}.gof', buffering=16777216) as fr:
            i = 0
            for line in fr:
                i = i + 1
                if i < 4 or line[0] == '^' or line == '\n': continue
                record = [r.strip() for r in line.split('~')]

                rec, err = model.csv_ext(record)
                if err is not None:
                    errors_list.append(err)
                else:
                    file_dict[md5(str(rec).encode('utf-8')).hexdigest()] = [rec[0], rec]

        deleted_count = BaseTable.delete_directly(model, file_dict, table_dict)
        inserted_count, updated_count = BaseTable.add_directly(model, file_dict, table_dict)
        report_added.append([f'{model._meta.db_table} ({model.external_name})',
                             inserted_count, updated_count, deleted_count])

        logger.info(report_added[-1])
        logger.info(time.strftime('%H:%M:%S'))

    context = {
        'report': report_added,
        'errors': errors_list,
    }

    return render(request, 'imported.html', context=context)
