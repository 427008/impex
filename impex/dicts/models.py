from django.db import models


class BaseTable(models.Model):
    class Meta:
        abstract = True

    key_number = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    name_short = models.CharField(max_length=255)
    name_lat = models.CharField(max_length=255)
    name_lat_short = models.CharField(max_length=255)
    date_import = models.IntegerField(default=0)
    date_export = models.IntegerField(default=0)

    def __str__(self):
        return self.name_short

    def csv_hash(self):
        return ';'.join((self.key_number, self.name, self.name_short, self.name_lat, self.name_lat_short,))


class CargoType(BaseTable):
    external_name = 'B1nsi1'


class LegalPerson(BaseTable):
    external_name = 'B1nsi3'


class Stations(BaseTable):
    external_name = 'B1nsi5'


class Contract(BaseTable):
    external_name = 'B1nsi9'


class Warehouse(BaseTable):
    external_name = 'B1nsi19'


class Paths(BaseTable):
    external_name = 'B1nsi42'


class Defects(BaseTable):
    external_name = 'B1nsi15'


def all_tables():
    from sys import modules
    from inspect import getmembers, isclass

    current_module = modules[__name__]
    x = [obj for _, obj in getmembers(current_module, isclass)]
    y = [obj for obj in x if hasattr(obj, 'external_name')]
    # z = [obj for obj in y if not obj._meta.abstract]
    return y


def insert_directly(name, values):
    import sqlite3
    from os.path import abspath, dirname, join
    from .apps import DictsConfig

    BASE_DIR = dirname(dirname(abspath(__file__)))
    base_name = f'{DictsConfig.name}.sqlite3'
    with sqlite3.connect(join(BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            f'INSERT INTO {name} (key_number, name, name_short, name_lat, name_lat_short, date_import, date_export) '
            f'VALUES(?, ?, ?, ?, ?, ?, 0)', values)


def update_directly(name, values):
    import sqlite3
    from os.path import abspath, dirname, join
    from .apps import DictsConfig

    BASE_DIR = dirname(dirname(abspath(__file__)))
    base_name = f'{DictsConfig.name}.sqlite3'
    with sqlite3.connect(join(BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            f'UPDATE {name} SET name=?, name_short=?, name_lat=?, name_lat_short=?, date_import=?, date_export=0 '
            f'WHERE key_number=?', values)


def delete_directly(name, values):
    import sqlite3
    from os.path import abspath, dirname, join
    from .apps import DictsConfig

    BASE_DIR = dirname(dirname(abspath(__file__)))
    base_name = f'{DictsConfig.name}.sqlite3'
    with sqlite3.connect(join(BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            f'UPDATE {name} SET date_import=0 WHERE key_number=?', values)
