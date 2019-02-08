from django.db import models
from impex import utils
from decimal import Decimal, getcontext


class BaseTable(models.Model):
    class Meta:
        abstract = True

    key_number = models.CharField(max_length=255, unique=True)
    date_import = models.IntegerField(default=0)
    md5hash = models.CharField(max_length=32, default='')

    def __str__(self):
        return self.key_number

    @staticmethod
    def query_delete(name):
        return f'DELETE FROM {name} WHERE key_number=?'

    @staticmethod
    def add_directly(obj, ext_dict, current_dict):
        import time

        inserted_count = 0
        updated_count = 0
        diff = set(ext_dict) - set(current_dict)
        if diff:
            insert_list = []
            update_list = []

            t = int(time.time())

            for md5hash in diff:
                key = ext_dict[md5hash][0]
                data = ext_dict[md5hash][1]

                if key in current_dict.values():
                    update_list.append(data[1:] + (md5hash, t, data[0],))
                else:
                    insert_list.append(data + (md5hash, t,))

            crud_directly(obj.query_update(), update_list)
            updated_count = len(update_list)
            crud_directly(obj.query_insert(), insert_list)
            inserted_count = len(insert_list)

        return inserted_count, updated_count

    @staticmethod
    def delete_directly(obj, ext_dict, current_dict):
        deleted_count = 0

        deleted = set(current_dict.values()) - {ret[0] for ret in ext_dict.values()}
        if deleted:
            delete_list = [(key,) for key in deleted]
            crud_directly(BaseTable.query_delete(obj._meta.db_table), delete_list)
            deleted_count = len(delete_list)

        return deleted_count


class RwbReceipt(BaseTable):
    external_name = 'B1'

    doc_date = models.CharField(max_length=50)
    sender = models.IntegerField(default=0)
    recipient = models.IntegerField(default=0)
    cargo = models.IntegerField(default=0)

    qty = models.IntegerField(default=0)
    cargo_net = models.CharField(max_length=50)
    contract = models.IntegerField(default=0)
    customer = models.IntegerField(default=0)
    recipient2 = models.IntegerField(default=0)
    station = models.IntegerField(default=0)

    def __str__(self):
        return f'{self.key_number} от {self.doc_date}'

    @staticmethod
    def large_trash():
        return []

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None
        if len(rec) < 11:
            err = ';'.join(rec) + f': мало данных ({len(rec)})'
        elif rec[0] != '':
            key = rec[0]
            if key in RwbReceipt.large_trash():
                return ret, err

            key = key.replace('.', '*')
            while len(rec) < 16:
                rec.append('')
            doc_date = utils.date_format(rec[2])
            sender = int(rec[3]) if rec[3] else 0
            recipient = int(rec[4]) if rec[4] else 0
            cargo = int(rec[6]) if rec[6] else 0

            qty = int(rec[7]) if rec[7] else 0
            getcontext().prec = 4
            cargo_net = str(Decimal(rec[8])) if rec[8] else '0.0'
            contract = int(rec[11]) if rec[11] else 0
            customer = int(rec[12]) if rec[12] else 0
            recipient2 = int(rec[14]) if rec[14] else 0
            station = int(rec[15]) if rec[15] else 0

            ret = (key, doc_date, sender, recipient, cargo,
                   qty, cargo_net, contract, customer, recipient2, station,)

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_rwbreceipt (key_number, doc_date, sender, recipient, cargo, 
            qty, cargo_net, contract, customer, recipient2, station, md5hash, date_import)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_rwbreceipt SET doc_date=?, sender=?, recipient=?, cargo=?,  
            qty=?, cargo_net=?, contract=?, customer=?, recipient2=?, station=?, md5hash=?, date_import=? 
            WHERE key_number=?"""
        return query


class RwbReceiptCar(BaseTable):
    external_name = 'B2'

    rwbill_no = models.CharField(max_length=50)
    car_no = models.CharField(max_length=10)
    cargo_net = models.CharField(max_length=50)
    contract = models.IntegerField(default=0)
    gtd = models.CharField(max_length=50)

    condition = models.IntegerField(default=0)
    state = models.IntegerField(default=0)
    contract_trade = models.IntegerField(default=0)
    owner = models.IntegerField(default=0)

    @staticmethod
    def trash():
        return ["021087:59596064", "021087:59597062", "021087:59806570", "021087:59806877", "021087:59807008",
                "021087:59807271", "021087:59813881", "021087:59814053", "021087:59814525", "021087:59831388",
                "021087:59831743", 'Б/Н', 'Б/Н1', 'Б/Н.', 'б/н']

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None
        if len(rec) < 10:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        else:
            key = ':'.join([rec[0], rec[2]])
            if key == ':':
                return ret, err
            if key in RwbReceiptCar.trash() and rec[1] == 'б/н':
                return ret, err
            if rec[2] in RwbReceiptCar.trash():
                return ret, err

            if rec[0] == '' or rec[2] == '':
                err = ';'.join(rec + [f'плохой ключ: {key}'])
            elif not rec[2].isdigit():
                err = f'плохой вагон: {rec[2]}'
            else:
                rwbill = rec[0].replace('.', '*')
                key = ':'.join([rwbill, rec[2]])
                while len(rec) < 36:
                    rec.append('')

                getcontext().prec = 4
                cargo_net = str(Decimal(rec[9])) if rec[9] else '0.0'
                contract = int(rec[14]) if rec[14] else 0
                gtd = rec[15]
                condition = int(rec[20]) if rec[20] else 0
                state = int(rec[21]) if rec[21] else 0
                contract_trade = int(rec[34]) if rec[34] else 0
                owner = int(rec[35]) if rec[35] else 0

                ret = (key, rwbill, rec[2], cargo_net, contract, gtd, condition, state, contract_trade, owner,)

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_rwbreceiptcar (key_number, rwbill_no, car_no, cargo_net, contract, gtd,
                condition, state, contract_trade, owner, md5hash, date_import)
                VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_rwbreceiptcar SET rwbill_no=?, car_no=?, cargo_net=?, contract=?, gtd=?,
                condition=?, state=?, contract_trade=?, owner=?, md5hash=?, date_import=?  
                WHERE key_number=?"""
        return query


class Gtd(BaseTable):
    external_name = 'B65'

    cargo = models.IntegerField(default=0)
    cargo_net = models.CharField(max_length=50)
    doc_date = models.CharField(max_length=10)

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None
        if len(rec) < 4:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        elif rec[0] != '':
            cargo = int(rec[1]) if rec[1] else 0
            getcontext().prec = 4
            cargo_net = str(Decimal(rec[2])) if rec[2] else '0.0'
            doc_date = utils.date_format(rec[3])
            ret = (rec[0], cargo, cargo_net, doc_date,)

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_gtd (key_number, cargo, cargo_net, doc_date,
                md5hash, date_import) VALUES (?, ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_gtd SET cargo=?, cargo_net=?, doc_date=?,
                md5hash=?, date_import=? WHERE key_number=?"""
        return query


class MemoTime(BaseTable):
    external_name = 'B49S1'

    route = models.CharField(max_length=50)
    memo = models.CharField(max_length=50)
    notice = models.IntegerField(default=0)
    path_no = models.IntegerField(default=0)

    in_date = models.CharField(max_length=50)
    in_time = models.CharField(max_length=5)
    end_date = models.CharField(max_length=50)
    end_time = models.CharField(max_length=5)

    @staticmethod
    def replace(key, default):
        dict = {'memo': (['0', '00', '000', '0000', '00000', '0000000', '000000000', '0000000000', 'юююю', 'ююю', 'юю',
                          'жжж', '99999999', '999999', '99999', '+', '+++', '+++++', '..', '....', '.........', '////'],
                         'б/н', )}
        if key in dict.keys() and default in dict[key][0]:
            return dict[key][1]

        return default

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None
        if len(rec) < 6:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        else:
            if ':'.join([rec[0], rec[1], rec[2]]) != '::':
                if rec[0] == '' or rec[1] == '' or rec[2] == '':
                    err = ';'.join(rec + ['плохой ключ'])
                else:
                    memo = MemoTime.replace('memo', rec[1])
                    notice = rec[2] if rec[2].isdigit() and rec[2] != '0' else '1'
                    key = ':'.join([rec[0], memo, notice])

                    path_no = int(rec[3]) if rec[3] else 1
                    in_date = utils.date_format(rec[4])
                    in_time = utils.time_format(rec[5])
                    end_date = utils.date_format(rec[6])
                    end_time = utils.time_format(rec[7])

                    ret = (key, rec[0], memo, notice, path_no, in_date, in_time, end_date, end_time,)

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_memotime (key_number, route, memo, notice, path_no, 
                in_date, in_time, end_date, end_time, md5hash, date_import)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_memotime SET route=?, memo=?, notice=?, path_no=?, 
                in_date=?, in_time=?, end_date=?, end_time=?, md5hash=?, date_import=? WHERE key_number=?"""
        return query


class MemoCar(BaseTable):
    external_name = 'B49S2'

    route = models.CharField(max_length=50)
    memo = models.CharField(max_length=50)
    notice = models.IntegerField(default=0)
    rwbill_no = models.CharField(max_length=50)
    car_no = models.CharField(max_length=10)

    @staticmethod
    def trash():
        return ['353941.', 'Б/Н', 'Б/Н1', 'Б/Н.', 'б/н']

    @staticmethod
    def replace(key, default):
        dict = {'memo': (['0', '00', '000', '0000', '00000', '0000000', '000000000', '0000000000', 'юююю', 'ююю',
                          'юю', 'жжж', '99999999', '999999', '99999', '+', '+++', '+++++', '..', '....', '.........',
                          '////'], 'б/н', )}
        if key in dict.keys() and default in dict[key][0]:
            return dict[key][1]

        return default

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None
        if len(rec) < 5:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        else:
            if rec[4] not in MemoCar.trash() and rec[3] not in MemoCar.trash():
                if '' in rec:
                    err = f'плохой ключ'
                elif not rec[4].isdigit():
                    err = f'плохой вагон: {rec[4]}'
                else:
                    rwbill = rec[3].replace('.', '*')
                    memo = MemoCar.replace('memo', rec[1])
                    notice = rec[2] if rec[2].isdigit() and rec[2] != '0' else '1'
                    key = ':'.join([rec[0], memo, notice, rwbill, rec[4]])
                    ret = (key, rec[0], memo, notice, rwbill, rec[4], )

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_memocar (key_number, route, memo, notice, rwbill_no, 
                car_no, md5hash, date_import) VALUES (?, ?, ?, ?, ?,  ?, ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_memocar SET route=?, memo=?, notice=?, rwbill_no=?, 
                car_no=?, md5hash=?, date_import=? WHERE key_number=?"""
        return query


class RwbReceiptCarTime(BaseTable):
    external_name = 'B59'

    rwbill_no = models.CharField(max_length=50)
    car_no = models.CharField(max_length=10)
    doc_date = models.CharField(max_length=50)
    doc_time = models.CharField(max_length=5)
    get_date = models.CharField(max_length=50)
    get_time = models.CharField(max_length=5)

    route = models.CharField(max_length=50, default='')
    memo = models.CharField(max_length=50, default='')
    notice = models.IntegerField(default=0)

    def __str__(self):
        return f'{self.key_number} от {self.doc_date}'

    @staticmethod
    def replace(key, default):
        dict = {'memo': (['', '0', '00', '000', '0000', '00000', '0000000', '000000000', '0000000000', 'юююю', 'ююю',
                          'юю', 'жжж', '99999999', '999999', '99999', '+', '+++', '+++++', '..', '....', '.........',
                          '////'], 'б/н', )}
        if key in dict.keys() and default in dict[key][0]:
            return dict[key][1]

        return default


    @staticmethod
    def trash():
        return ['301199', '301200', '415767', '00083568', '00083569', '00126763', '00126789', '00902912', '00916454',
                '00917958', '072508', 'Е902888-', 'Е916370', 'Б/Н', 'Б/Н1', 'Б/Н.', 'б/н']

    @staticmethod
    def large_trash():
        return []

    @staticmethod
    def csv_ext(rec):
        ret = None
        err = None

        if len(rec) < 22:
            err = ';'.join(rec) + f': мало данных ({len(rec)})'
        else:
            key = ':'.join([rec[0], rec[1]])
            if key == ':':
                return ret, err

            if rec[0] in RwbReceiptCarTime.trash() or rec[1] in RwbReceiptCarTime.trash():
                return ret, err

            if key in RwbReceiptCarTime.large_trash():
                return ret, err

            if ''.join(rec[2:35]) == '':
                return ret, err

            if rec[0] == '' or rec[1] == '':
                err = ';'.join(rec + [f'плохой ключ: {key}'])
            elif not rec[1].isdigit():
                err = f'плохой вагон: {rec[1]}'
            else:
                while len(rec) < 35:
                    rec.append('')

                rwbill = rec[0].replace('.', '*')
                key = ':'.join([rwbill, rec[1]])

                doc_date = utils.date_format(rec[4])
                doc_time = utils.time_format(rec[5])
                get_date = utils.date_format(rec[6])
                get_time = utils.time_format(rec[7])

                if rwbill == '916436' and rec[32] == '':
                    rec[32] = '59014.4'
                    doc_date = '2002-07-29'
                    doc_time = '21:00'
                    get_date = '2002-07-29'
                    get_time = '21:00'
                if rwbill == '916448' and rec[32] == '':
                    rec[32] = '59038.13'
                    doc_date = '2002-08-22'
                    doc_time = '4:00'
                    get_date = '2002-08-23'
                    get_time = '14:35'
                if rwbill == 'Е916413' and rec[32] == '':
                    rec[32] = '58981.1'
                    doc_date = '2002-06-25'
                    doc_time = '13:00'
                    get_date = '2002-06-25'
                    get_time = '13:00'
                memo = RwbReceiptCarTime.replace('memo', rec[33])
                notice = rec[34] if rec[34].isdigit() and rec[34] != '0' else '1'

                if doc_date == utils.t2000() and get_date != utils.t2000():
                    doc_date = get_date
                    doc_time = get_time
                if get_date == utils.t2000() and doc_date != utils.t2000():
                    get_date = doc_date
                    get_time = doc_time

                ret = (key, rwbill, rec[1], doc_date, doc_time, get_date, get_time, rec[32], memo, notice, )

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_rwbreceiptcartime (key_number, rwbill_no, car_no, 
                doc_date, doc_time, get_date, get_time, route, memo, notice, md5hash, date_import)
                VALUES (?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_rwbreceiptcartime SET rwbill_no=?, car_no=?, doc_date=?, doc_time=?,  
            get_date=?, get_time=?, route=?, memo=?, notice=?, md5hash=?, date_import=? WHERE key_number=?"""
        return query


class RcwError(models.Model):

    date_import = models.IntegerField(default=0)
    data = models.TextField()
    file_name = models.CharField(max_length=50)

    def __str__(self):
        import time

        t = time.gmtime(self.date_import)
        return time.strftime('%Y-%m-%d', t)


def all_tables():
    from sys import modules
    from inspect import getmembers, isclass

    current_module = modules[__name__]
    x = [obj for _, obj in getmembers(current_module, isclass)]
    y = [obj for obj in x if hasattr(obj, 'external_name')]
    return y


def crud_directly(query, values):
    import sqlite3
    from os.path import abspath, dirname, join
    from .apps import ReceiptConfig
    from impex import utils

    BASE_DIR = dirname(dirname(abspath(__file__)))
    base_name = f'{ReceiptConfig.name}.sqlite3'
    with sqlite3.connect(join(BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        utils.execute_many(values, conn, cursor, query)
