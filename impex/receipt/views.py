from os import path
from django.shortcuts import render
import time
from hashlib import md5

from .models import BaseTable, RcwError, all_tables
from .apps import ReceiptConfig


def imported(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.receipt')
    logger.info(time.strftime('%Y-%m-%d %H:%M:%S'))

    BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
    file_path = path.join(BASE_DIR, ReceiptConfig.name, 'data')
    report_added = []

    for model in all_tables():
        errors_list = []
        table_dict = {c[0]: c[1] for c in model.objects.values_list('md5hash', 'key_number')}
        file_dict = {}
        t0 = time.time()

        with open(f'{file_path}\{model.external_name}.gof', buffering=16777216) as fr:
            i = 0
            for line in fr:
                i = i + 1
                if i < 4 or line[0] == '^' or line == '\n': continue
                record = [r.strip() for r in line.split('~')]

                rec, err = model.csv_ext(record)
                if err is not None:
                    errors_list.append(RcwError(file_name=model.external_name, data=err, date_import=t0))
                elif rec is not None:
                    file_dict[md5(str(rec).encode('utf-8')).hexdigest()] = [rec[0], rec]

        deleted_count = BaseTable.delete_directly(model, file_dict, table_dict)
        inserted_count, updated_count = BaseTable.add_directly(model, file_dict, table_dict)
        report_added.append([f'{model._meta.db_table} ({model.external_name})',
                             inserted_count, updated_count, deleted_count])
        RcwError.objects.bulk_create(errors_list)

        logger.info(report_added[-1])
        t1 = time.time() - t0
        logger.info(f'{int(t1)}c x{"x" * int(t1/10.)}')
        logger.info(f'with {len(errors_list)} errors')

    context = {
        'caption': 'Импорт "прием на пути"',
        'report': report_added,
    }

    return render(request, 'receipt.html', context=context)
