from django.shortcuts import render
import time

from .models import all_tables


def imported(request):
    """Load and View function for import page of receipt."""
    import logging

    logger = logging.getLogger('impex.adapter')
    logger.info(time.strftime('%H:%M:%S'))

    report_added = []
    errors_list = []

    for model in all_tables():
        deleted_count, inserted_count, updated_count = model.update_from_source()
        report_added.append([f'{model._meta.db_table}', inserted_count, updated_count, deleted_count])
        logger.info(report_added[-1])
        logger.info(time.strftime('%H:%M:%S'))

    context = {
        'report': report_added,
        'errors': errors_list,
    }

    return render(request, 'imported.html', context=context)
