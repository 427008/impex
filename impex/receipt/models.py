from django.db import models
from impex import utils
from decimal import Decimal, getcontext


class BaseTable(models.Model):
    class Meta:
        abstract = True

    separator_pos = 0
    separator_set = set()
    key_number = models.CharField(max_length=255, unique=True)
    date_import = models.IntegerField(default=0)
    md5hash = models.CharField(max_length=32, default='')

    def __str__(self):
        return self.key_number

    @staticmethod
    def query_delete(name):
        return f'DELETE FROM {name} WHERE key_number=?'

    @classmethod
    def add_directly(cls: object, ext_dict: dict, current_dict: dict, only_old_data: bool):
        import time

        inserted_count = 0
        updated_count = 0
        separator_pos = cls.separator_pos
        diff = set(ext_dict) - set(current_dict)
        if diff:
            insert_list = []
            update_list = []

            t = int(time.time())

            for md5hash in diff:
                key = ext_dict[md5hash][0]
                data = ext_dict[md5hash]
                if separator_pos is None or \
                    (only_old_data and data[separator_pos] in BaseTable.separator_set) or \
                    (not only_old_data and data[separator_pos] not in BaseTable.separator_set):

                    if key in current_dict.values():
                        update_list.append(data[1:] + (md5hash, t, data[0],))
                    else:
                        insert_list.append(data + (md5hash, t,))

            crud_directly(cls.query_update(), update_list)
            updated_count = len(update_list)
            crud_directly(cls.query_insert(), insert_list)
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
    large_trash = set()

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
    def csv_ext(rec, only_old_data):
        ret = None
        err = None
        if len(rec) < 11:
            return ret, ';'.join(rec) + f': мало данных ({len(rec)})'

        if rec[0] != '':
            key = rec[0]
            if only_old_data is False and key in BaseTable.separator_set:
                return ret, err
            if key in RwbReceipt.large_trash:
                return ret, f'{key} исключается(мусор)'

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
    # 13	? Отправлено (всего)
    external_name = 'B2'
    separator_pos = 1
    large_trash = set()

    rwbill_no = models.CharField(max_length=50)         # 0	    Номер основной жд накладной
    act_no = models.CharField(max_length=50)            # 1	    Номер приходного акта
    car_no = models.CharField(max_length=10)            # 2	    Номер жд вагона
    get_date = models.CharField(max_length=50)          # 5	    Дата окончания выгрузки
    get_time = models.CharField(max_length=5)           # 6	    Время окончания выгрузки

    store_section = models.IntegerField(default=0)      # 8	    Номер секции/склада выгрузки
    section_no = models.CharField(max_length=50)        # 22	Номер секции
    cargo_net = models.CharField(max_length=50)         # 9	    Вес нетто вагона по документам принято
    cargo_net_fact = models.CharField(max_length=50)    # 10	Вес нетто вагона по факту
    npp = models.IntegerField(default=0)                # 19	Порядковый номер гр ваг в составе фактически

    contract = models.IntegerField(default=0)           # 14	Номер договора/контракта
    gtd = models.CharField(max_length=50)               # 15	Номер груз там декларации
    sert_to = models.CharField(max_length=50)           # 17	Номера сертификатов качества и происхождения
    condition = models.IntegerField(default=0)          # 20	Состояние вагона

    custom_no = models.CharField(max_length=50)         # 31	Номер таможенного учета
    custom_net = models.CharField(max_length=50)        # 32	Передекларированный вес
    custom_gtd = models.CharField(max_length=50)        # 33	Номер декларации
    contract_new = models.IntegerField(default=0)       # 34	Номер договора - новый
    owner = models.IntegerField(default=0)              # 35	Владелец

    @staticmethod
    def trash():
        return ["021087:59596064", "021087:59597062", "021087:59806570", "021087:59806877", "021087:59807008",
                "021087:59807271", "021087:59813881", "021087:59814053", "021087:59814525", "021087:59831388",
                "021087:59831743"]

    @staticmethod
    def csv_ext(rec, only_old_data):
        ret = None
        err = None
        if len(rec) < 10:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        else:
            key = ':'.join([rec[0], rec[2]])
            if key == ':':
                return ret, err
            if rec[0] == '' or rec[2] == '':
                err = f'плохой ключ: {key}'
            if only_old_data is False and rec[0] in BaseTable.separator_set:
                return ret, err
            if key in RwbReceiptCar.trash() and rec[1] == 'б/н':
                return ret, f'накладная+вагон {key} имеют дубль'
            if not rec[2].isdigit():
                return ret, f'вагон {key} - дефектный номер вагона'
            if rec[0] in RwbReceiptCar.large_trash:
                return ret, f'{key} исключается(мусор)'
            while len(rec) < 36:
                rec.append('')
            if rec[21].isdigit() and int(rec[21]) == 2:
                return ret, f'{key} вагон не принят'

            # if rec[0]=='ЭГ337863':
            #     err = ';'.join(rec)
            #     return ret, f'{err} XXX'

            rwbill = rec[0].replace('.', '*')
            key = ':'.join([rwbill, rec[2]])
            get_date = utils.date_format(rec[5])
            get_time = utils.time_format(rec[6])

            store_section = int(rec[8]) if rec[8] else 0
            section_no = rec[22]
            getcontext().prec = 4
            cargo_net = str(Decimal(rec[9])) if rec[9] else '0.0'
            cargo_net_fact = str(Decimal(rec[10])) if rec[10] else '0.0'
            npp = int(rec[19]) if rec[19] and rec[19].isdigit() else 0

            contract = int(rec[14]) if rec[14] else 0
            gtd = rec[15]
            sert_to = rec[17]
            condition = int(rec[20]) if rec[20] else 0  # dicts_defects (5 == 'ПЕРЕАДРЕСОВАН', ....)

            custom_no = rec[31]
            custom_net = str(Decimal(rec[32])) if rec[32] else '0.0'
            custom_gtd = rec[33]
            contract_trade = int(rec[34]) if rec[34] else 0
            owner = int(rec[35]) if rec[35] else 0

            ret = (key, rwbill, rec[1], rec[2], get_date, get_time,
                   store_section, section_no, cargo_net, cargo_net_fact, npp,
                   contract, gtd, sert_to, condition, custom_no, custom_net, custom_gtd, contract_trade, owner, )

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_rwbreceiptcar (key_number, rwbill_no, act_no, car_no, get_date, get_time, 
                   store_section, section_no, cargo_net, cargo_net_fact, npp, 
                   contract, gtd, sert_to, condition, custom_no, custom_net, custom_gtd, contract_new, owner, 
                   md5hash, date_import)
                VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_rwbreceiptcar SET rwbill_no=?, act_no=?, car_no=?, get_date=?, get_time=?, 
                   store_section=?, section_no=?, cargo_net=?, cargo_net_fact=?, npp=?, 
                   contract=?, gtd=?, sert_to=?, condition=?, custom_no=?, custom_net=?, custom_gtd=?, contract_new=?, 
                   owner=?, md5hash=?, date_import=?   
                WHERE key_number=?"""
        return query

    @staticmethod
    def check_data(only_old_data):
        query1 = """select count(*) from receipt_rwbreceipt A 
                      left join receipt_rwbreceiptcar B 
                      on A.key_number=B.rwbill_no where B.rwbill_no is null;"""

        query2 = """select count(*) from receipt_rwbreceiptcar A 
                      left join receipt_rwbreceipt B
                      on B.key_number=A.rwbill_no where B.key_number is null;"""

        query3 = """select A.rwbill_no, count(*) 
                    from (select distinct rwbill_no, contract from receipt_rwbreceiptcar) A
                    group by rwbill_no having count(*)>1"""

        # -- 1. receipt_rwbreceipt -> receipt_rwbreceiptcar - д.б. 0 записей
        # --    для всех вагонов должна быть накладная (и наоборот)
        # -- 2. receipt_rwbreceiptcar -> receipt_rwbreceipt - д.б. 0 записей
        # --    для всех вагонов должна быть накладная (и наоборот)

        result = execute_check(query1, query2, query3)
        if result[0][0][0] != 0 or result[1][0][0] != 0:
            ret = []
            if result[0][0][0] != 0:
                ret.append(f'Есть накладные ({result[0][0][0]}шт) RwbReceipt(B1) без вагонов RwbReceiptCar(B2).')
            if result[1][0][0] != 0:
                ret.append(f'Есть вагоны ({result[1][0][0]}шт) RwbReceiptCar(B2) без накладной RwbReceipt(B1)!')
        else:
            ret = ['В RwbReceipt(B1) и в RwbReceiptCar(B2) записей без подчиненных(без главной) нет.']
            if len(result[2]) > 0:
                ret.append(f'Есть накладные с контрактами 1:2 ({len(result[2])}шт) RwbReceiptCar(B2) (норма 1:1)')

        return ret


class Gtd(BaseTable):
    external_name = 'B65'

    separator_pos = None
    cargo = models.IntegerField(default=0)
    cargo_net = models.CharField(max_length=50)
    doc_date = models.CharField(max_length=10)

    @staticmethod
    def csv_ext(rec, only_old_data):
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

    @staticmethod
    def check_data(only_old_data):
        query1 = """select distinct A.gtd, C.doc_date from receipt_rwbreceiptcar A
                    inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
                    left join receipt_gtd B on B.key_number=A.gtd
                    where B.key_number is null order by C.doc_date desc;"""

        #-- 7. receipt_rwbreceiptcar -> receipt_gtd - д.б. 0 записей
        # --   для всех вагонов должна быть гтд

        result = execute_check(query1)
        errors_count = len(result[0])
        if errors_count > 0:
            return [f'Есть вагоны RwbReceiptCar(B2) с гтд без записи в справочнике Gtd(B65). Всего {errors_count}.']

        return ['В Gtd(B65) и в RwbReceiptCar(B2) записей без подчиненных(без главной) нет.']


class MemoTime(BaseTable):
    external_name = 'B49S1'
    separator_pos = None
    skip_for_old_data = True

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
    def csv_ext(rec, only_old_data):
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
    separator_pos = 4
    skip_for_old_data = True

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
    def csv_ext(rec, only_old_data):
        ret = None
        err = None
        if len(rec) < 5:
            err = ';'.join(rec + [f'мало данных: {len(rec)}'])
        else:
            if only_old_data is False and rec[3] in BaseTable.separator_set:
                return ret, err
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

    @staticmethod
    def check_data(only_old_data):
        query1 = 'select count(*) from receipt_memocar where route is null;'
        query2 = """select count(*) from receipt_rwbreceiptcartime A inner join receipt_memocar B 
                    on A.rwbill_no=B.rwbill_no and A.car_no=B.car_no 
                    where A.route<>B.route;"""
        query3 = """select A.get_date from receipt_rwbreceiptcartime A left join receipt_memocar B
                    on A.rwbill_no=B.rwbill_no and A.car_no=B.car_no
                    where B.route is null order by A.get_date desc"""
        #-- 1. receipt_memocar
        #--    ненормально - для пары вагон+накладная нет значения маршрута, д.б. 0 записей
        #-- 2. receipt_rwbreceiptcartime
        #--    в receipt_memocar и receipt_rwbreceiptcartime должны совпадать маршруты A.route == B.route, д.б. 0 записей
        #-- 3. receipt_rwbreceiptcartime -> receipt_memocar
        #--    в receipt_memocar можно найти маршруты для receipt_rwbreceiptcartime , д.б. 0 записей

        result = execute_check(query1, query2, query3)
        errors_count = len(result[2])
        if result[0][0][0] != 0 or result[1][0][0] != 0 or errors_count > 0:
            ret = []
            if result[0][0][0] != 0:
                ret.append(f'Есть вагоны ({result[0][0][0]}шт) MemoCar(B49S2) без маршрута!.')
            if result[1][0][0] != 0:
                ret.append(f'Есть вагоны ({result[1][0][0]}шт) RwbReceiptCarTime(B59) '
                           f'маршрут которых отличается от MemoCar(B49S2).')
            if errors_count > 0:
                errors_18 = len([r for r in result[2][0] if r[0] > '2015-12-31'])
                ret.append(f'Есть вагоны RwbReceiptCarTime(B59) без подчиненных MemoCar(B49S2). ' \
                           f'Всего {errors_count}, новых {errors_18}.')
            return ret

        return ['В RwbReceiptCarTime(B59) и в MemoCar(B49S2) записей без подчиненных(без главной) нет.']


class RwbReceiptCarTime(BaseTable):
    external_name = 'B59'
    separator_creator = 1
    separator_pos = 1
    large_trash = set()
    trash = {'301199', '301200', '415767', '00083568', '00083569', '00126763', '00126789', '00902912', '00916454',
            '00917958', '072508', 'Е902888-', 'Е916370', '301198', '311701', '75443266', '75443546', '75448506',
            '75448517', '78120300', '78120813', '800913', 'М191960', 'М191961'}

    rwbill_no = models.CharField(max_length=50)
    car_no = models.CharField(max_length=10)
    doc_date = models.CharField(max_length=50)                  # Дата вручения памятки
    doc_time = models.CharField(max_length=5)                   # Время вручения памятки
    get_date = models.CharField(max_length=50)                  # Дата факт подачи с Автово
    get_time = models.CharField(max_length=5)                   # Время факт подачи

    put_date = models.CharField(max_length=50, default='')      # Дата фактич выгрузки
    put_time = models.CharField(max_length=5, default='')       # Время фактич выгрузки
    doc_out_date = models.CharField(max_length=50, default='')  # Дата факт. уборки вагонов с путей ББТ
    doc_out_time = models.CharField(max_length=5, default='')   # Время фактического вывода
    out_date = models.CharField(max_length=50, default='')      # Дата факт. уборки вагонов с путей ББТ
    out_time = models.CharField(max_length=5, default='')       # Время фактического вывода

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
    def csv_ext(rec, only_old_data):
        ret = None
        err = None

        if len(rec) < 22:
            err = ';'.join(rec) + f': мало данных ({len(rec)})'
        else:
            if ''.join(rec[2:35]) == '':
                return ret, err
            if rec[0] in BaseTable.separator_set:
                return ret, err

            key = ':'.join([rec[0], rec[1]])
            if rec[0] == '' or rec[1] == '':
                err = ';'.join(rec + [f'плохой ключ: {key}'])
            if rec[0] in RwbReceiptCarTime.trash or rec[1] in RwbReceiptCarTime.trash:
                return ret, f'{key} содержит мусор'
            if not rec[1].isdigit():
                return ret, f'{key} - ошибка в номере вагона'
            if key in RwbReceiptCarTime.large_trash:
                return ret, f'{key} ошибки в данных'

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

            put_date = utils.date_format(rec[12])
            put_time = utils.time_format(rec[13])

            doc_out_date = utils.date_format(rec[24])
            doc_out_time = utils.time_format(rec[25])
            out_date = utils.date_format(rec[26])
            out_time = utils.time_format(rec[27])
            if out_date == utils.t2000() and doc_out_date != utils.t2000():
                out_date = doc_out_date
                out_time = doc_out_time

            ret = (key, rwbill, rec[1], doc_date, doc_time, get_date, get_time, rec[32], memo, notice,
                   put_date, put_time, doc_out_date, doc_out_time, out_date, out_time, )

        return ret, err

    @staticmethod
    def query_insert():
        query = """INSERT INTO receipt_rwbreceiptcartime (key_number, rwbill_no, car_no, 
                doc_date, doc_time, get_date, get_time, route, memo, notice, 
                put_date, put_time, doc_out_date, doc_out_time, out_date, out_time, 
                md5hash, date_import) VALUES (?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?, ?, ?,  ?, ?)"""
        return query

    @staticmethod
    def query_update():
        query = """UPDATE receipt_rwbreceiptcartime SET rwbill_no=?, car_no=?, doc_date=?, doc_time=?,  
            get_date=?, get_time=?, route=?, memo=?, notice=?, put_date=?, put_time=?, doc_out_date=?, doc_out_time=?,  
            out_date=?, out_time=?, md5hash=?, date_import=? WHERE key_number=?"""
        return query

    @staticmethod
    def check_data(only_old_data):
        if(only_old_data):
            query1 = """select A.rwbill_no from receipt_rwbreceiptcar A
                        left join receipt_rwbreceiptcartime B on A.key_number=B.key_number
                        where B.key_number is null;"""

        else:
            query1 = """select distinct A.rwbill_no, C.doc_date from receipt_rwbreceiptcar A
                        inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
                        left join receipt_rwbreceiptcartime B on A.key_number=B.key_number
                        where C.doc_date<'2019-01-01' and B.key_number is null;"""

        query2 = """select A.rwbill_no from receipt_rwbreceiptcartime A
                    left join receipt_rwbreceiptcar B on A.key_number=B.key_number 
                    where B.key_number is null;"""

        query3 = "select count(*) from receipt_rwbreceiptcartime where out_date='2000-01-01'"

        #-- 3. receipt_rwbreceiptcar -> receipt_rwbreceiptcartime
        #--    для всех вагонов(+накладная) должно быть соответствие.
        #--    ненормально - есть вагон+накладная, нет записи о прибытии, д.б. 0 записей

        #-- 4. receipt_rwbreceiptcartime -> receipt_rwbreceiptcar
        #--    ненормально - есть время нет пары вагон+накладная, д.б. 0 записей

        result = execute_check(query1, query2, query3)
        ret = []
        if len(result[0]) != 0 or result[2][0][0] != 0:
            if len(result[0]) != 0:
                ret.append(f'Есть вагоны ({len(result[0])}шт) RwbReceiptCar(B2) ' \
                      f'без записи о времени обработки RwbReceiptCarTime(B59).')
            if len(result[1]) != 0:
                ret.append(f'Есть обработанные вагоны ({len(result[1])}шт) RwbReceiptCarTime(B59) ' \
                               f'без записи в накладной RwbReceiptCar(B2).')
            if result[2][0][0] != 0:
                ret.append(f'Есть вагоны без даты и времени возврата ({result[2][0][0]}шт) RwbReceiptCarTime(B59).')
            return ret

        if result[2][0][0] != 0:
            ret.append(f'Есть вагоны без даты и времени возврата ({result[2][0][0]}шт) RwbReceiptCarTime(B59).')
        ret.append(['В RwbReceiptCarTime(B59) и в RwbReceiptCar(B2) записей без подчиненных(без главной) нет.'])
        return ret


class RcwError(models.Model):

    date_import = models.IntegerField(default=0)
    data = models.TextField()
    file_name = models.CharField(max_length=50)

    def __str__(self):
        import time

        t = time.gmtime(self.date_import)
        return time.strftime('%Y-%m-%d', t)


class Separator(models.Model):
    key_number = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.key_number


def all_tables():
    from sys import modules
    from inspect import getmembers, isclass

    current_module = modules[__name__]
    x = [obj for _, obj in getmembers(current_module, isclass)]
    y = [obj for obj in x if hasattr(obj, 'external_name')]
    z = sorted(y, key=lambda obj: not hasattr(obj, 'separator_creator'))
    return z


def crud_directly(query, values):
    import sqlite3
    from os.path import join
    from .apps import ReceiptConfig
    from impex import utils
    from impex import settings

    base_name = f'{ReceiptConfig.name}.sqlite3'
    with sqlite3.connect(join(settings.BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        utils.execute_many(values, conn, cursor, query)


def execute_check(*queries):
    import sqlite3
    from os.path import join
    from .apps import ReceiptConfig
    from impex import settings

    result = []
    base_name = f'{ReceiptConfig.name}.sqlite3'
    with sqlite3.connect(join(settings.BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
            result.append(cursor.fetchall())

    return result
