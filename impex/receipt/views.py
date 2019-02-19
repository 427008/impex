from os import path
from django.shortcuts import render
import time
from hashlib import md5

from .models import BaseTable, RcwError, Separator, all_tables
from .apps import ReceiptConfig


def log_time(t0, logger, message=''):
    t1 = time.time()
    t = t1 - t0
    logger.info(f'{message} {int(t)}c x{"x" * int(t / 10.)}')
    return t1


def imported(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.receipt')
    logger.info(time.strftime('%Y-%m-%d %H:%M:%S'))

    BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))
    file_path = path.join(BASE_DIR, ReceiptConfig.name, 'data')
    report_added = []
    message = []
    BaseTable.separator_set = {i.key_number for i in Separator.objects.all()}
    if len(BaseTable.separator_set) == 0:
        only_old_data = True
    else:
        only_old_data = False

    models = all_tables()
    for model in models:
        if only_old_data and hasattr(model, 'skip_for_old_data'):
            continue

        errors_list = []
        table_dict = {c[0]: c[1] for c in model.objects.values_list('md5hash', 'key_number')}
        file_dict = {}
        t0 = time.time()
        if hasattr(model, 'large_trash'):
            with open(f'{file_path}\{model.external_name}_trash.csv', buffering=20480) as trash_file:
                model.large_trash = {key.strip() for key in trash_file.readlines()}

        with open(f'{file_path}\{model.external_name}.gof', buffering=16777216) as fr:
            i = 0
            for line in fr:
                i = i + 1
                if i < 4 or line[0] == '^' or line == '\n': continue
                record = [r.strip() for r in line.split('~')]

                rec, err = model.csv_ext(record, only_old_data)
                if err is not None:
                    errors_list.append(RcwError(file_name=model.external_name, data=err, date_import=t0))
                elif rec is not None:
                    file_dict[md5(str(rec).encode('utf-8')).hexdigest()] = rec

        t1 = log_time(t0, logger, '--read')

        if only_old_data and hasattr(model, 'separator_creator'):
            tmp = {}
            for rec in file_dict.values():
                if rec[1] in tmp.keys():
                    tmp[rec[1]].append(rec[3])
                else:
                    tmp[rec[1]] = [rec[3]]

            BaseTable.separator_set = {key for key, val in tmp.items() if all(d < '2016-01-01' for d in val)}
            separator_list = [Separator(key_number=key) for key in BaseTable.separator_set]
            Separator.objects.bulk_create(separator_list)
            t2 = log_time(t1, logger, '--separator')
        else:
            t2 = t1

        deleted_count = 0 #BaseTable.delete_directly(model, file_dict, table_dict)

        inserted_count, updated_count = model.add_directly(file_dict, table_dict, only_old_data)
        report_added.append([f'{model._meta.db_table} ({model.external_name})',
                             inserted_count, updated_count, deleted_count])
        t3 = log_time(t2, logger, '--write')
        if len(errors_list):
            RcwError.objects.bulk_create(errors_list)
            log_time(t3, logger, '--errors')
            logger.info(f'----with {len(errors_list)}')

        logger.info(report_added[-1])
        log_time(t0, logger, 'total')

    for model in models:
        if only_old_data and hasattr(model, 'skip_for_old_data'):
            continue
        if hasattr(model, 'check_data'):
            message.extend(model.check_data(only_old_data))

    context = {
        'caption': 'Импорт "прием на пути"',
        'report': report_added,
        'message': message,
    }

    return render(request, 'receipt.html', context=context)
