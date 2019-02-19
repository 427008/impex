from django.shortcuts import render
import time

from .models import Mode, all_tables


def imported(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.adapter')
    logger.info(time.strftime('%H:%M:%S'))

    report_added = []
    errors_list = []

    # for model in all_tables():
    #     deleted_count, inserted_count, updated_count = model.update_from_source()
    #     report_added.append([f'{model._meta.db_table}', inserted_count, updated_count, deleted_count])
    #     logger.info(report_added[-1])
    #     logger.info(time.strftime('%H:%M:%S'))

    context = {
        'caption': 'Создания адаптера "прием на пути" НОВЫЙ',
        'report': report_added,
    }

    return render(request, 'adapter.html', context=context)


def imported_old(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.adapter')
    logger.info(time.strftime('%H:%M:%S'))

    report_added = []
    errors_list = []

    count = Mode.objects.all().count()
    if count == 0:
        for model in all_tables(True):
            deleted_count, inserted_count, updated_count = model.insert_from_source()
            report_added.append([f'{model._meta.db_table}', inserted_count, updated_count, deleted_count])
            logger.info(report_added[-1])
            logger.info(time.strftime('%H:%M:%S'))

        context = {
            'caption': 'Создание адаптера "прием на пути" СТАРЫЙ',
            'report': report_added,
        }
    else:
        report_added.append(['ОШИБКА', 0, 0, 0])
        context = {
            'caption': 'Повторное создания адаптера "прием на пути" СТАРЫЙ не предусмотрено',
            'report': report_added,
        }

    return render(request, 'adapter.html', context=context)
