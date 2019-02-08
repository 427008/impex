from django.db import models
from hashlib import md5


class BaseTable(models.Model):
    class Meta:
        abstract = True

    key_number = models.CharField(max_length=255, unique=True)
    date_import = models.IntegerField(default=0)
    date_export = models.IntegerField(default=0)
    md5hash = models.CharField(max_length=32, default='')

    def __str__(self):
        return self.key_number

    @staticmethod
    def delete_directly(obj, new_keys, cur_keys, query=None):
        deleted_count = 0
        deleted = set(cur_keys.values()) - set(new_keys.values())
        if deleted:
            table_name = obj._meta.db_table
            if query is None:
                query = f'UPDATE {table_name} SET date_import=0, date_import=0 WHERE key_number=?'
            delete_list = [(key,) for key in deleted]
            crud_directly(query, delete_list)
            deleted_count = len(delete_list)

        return deleted_count

    @staticmethod
    def add_directly(obj, new_keys, cur_keys, table):
        inserted_count = 0
        updated_count = 0
        diff = set(new_keys) - set(cur_keys)
        if diff:
            insert_list = []
            update_list = []

            for md5hash in diff:
                key = new_keys[md5hash]
                data = table[key]

                if key in cur_keys.values():
                    update_list.append(obj.prepare_update(data, md5hash))
                else:
                    insert_list.append(obj.prepare_insert(data, md5hash))

            crud_directly(obj.query_update(), update_list)
            updated_count = len(update_list)

            crud_directly(obj.query_insert(), insert_list)
            inserted_count = len(insert_list)

        return inserted_count, updated_count


class RwbReceipt(BaseTable):
    source_base = 'receipt'

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

    gtd = models.TextField(default='')

    def __str__(self):
        return f'{self.key_number} от {self.doc_date}'

    @staticmethod
    def update_from_source():
        import sqlite3

        with sqlite3.connect(connection_str()) as conn:
            cursor = conn.cursor()
            max_date_import1 = max_date_import('adapter_rwbreceipt', cursor)
            max_date_import2 = max_date_import('adapter_rwbreceiptcar', cursor)
            current_adapter = current_hash('adapter_rwbreceipt', cursor)

            # start with receipt from receipt_rwbreceipt
            source_name = f'{RwbReceipt.source_base}.sqlite3'
            cursor.execute(f"ATTACH DATABASE '{connection_str(source_name)}' AS S")
            query = f"""SELECT key_number, doc_date, sender, recipient, cargo,
                qty, cargo_net, contract, customer, recipient2, station, date_import FROM S.receipt_rwbreceipt
                WHERE date_import>{max_date_import1}"""
            cursor.execute(query)
            receipt0 = {row[0]: (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                 row[10], row[11], ) for row in cursor.fetchall()}

            # add to receipt_rwbreceipt gtd from receipt_rwbreceiptcar:
            #   get gtd
            query = f"""select key_number, B.gtd, count(B.gtd), B.date_import from S.receipt_rwbreceipt A
                inner join (select distinct rwbill_no, gtd, date_import 
                from S.receipt_rwbreceiptcar where date_import>{max_date_import2}) B 
                on A.key_number = B.rwbill_no group by key_number"""
            cursor.execute(query)
            gtd_rows = [(row[0], row[1], row[2], row[3], ) for row in cursor.fetchall()]
            gtd_rows1 = {t[0]: (t[1], t[3], ) for t in gtd_rows if t[2] == 1}       # 1 rwbill - 1 gtd
            gtd_rows2 = {t[0] for t in gtd_rows if t[2] != 1}                       # 1 rwbill - n gtd
            gtd_rowsn = {}
            for key in gtd_rows2:
                query = f"select distinct gtd, date_import from S.receipt_rwbreceiptcar where rwbill_no=?"
                cursor.execute(query, (key, ))
                rows = cursor.fetchall()
                gtd_rows = [row[0] for row in rows]
                date_import = max([row[1] for row in rows])
                gtd_rowsn[key] = (';'.join(gtd_rows), date_import, )

            #   add gtd to receipt_rwbreceipt
            gtd_rows2 = None
            gtd_rows = {**gtd_rows1, **gtd_rowsn}
            receipt = {key: receipt0.get(key) + gtd_rows.get(key, ('', 0, ))
                       for key in set(receipt0)}
            hashed_receipt = {md5(str(rec[:-3] + (rec[-2], )).encode('utf-8')).hexdigest(): key for key, rec in receipt.items()}

            # check hash for deleted
            deleted_count = 0 # BaseTable.delete_directly(RwbReceipt, hashed_receipt, current_adapter)

            # check hash for new
            inserted_count, updated_count = BaseTable.add_directly(RwbReceipt, hashed_receipt, current_adapter, receipt)

        return deleted_count, inserted_count, updated_count

    @staticmethod
    def query_insert():
        query = f"""INSERT INTO adapter_rwbreceipt (key_number, doc_date, sender, recipient, cargo, 
            qty, cargo_net, contract, customer, recipient2, station, gtd,  
            md5hash, date_import, date_export)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?,  ?, ?, 0)"""
        return query

    @staticmethod
    def prepare_insert(data, md5hash):
        # .. (from receipt_rwbreceipt) date_import[-3], (from receipt_rwbreceiptcar) gtd[-2], date_import[-1]
        return data[:-3] + (data[-2], md5hash, max(data[-3], data[-1]),)

    @staticmethod
    def query_update():
        query = f"""UPDATE adapter_rwbreceipt SET doc_date=?, sender=?, recipient=?, cargo=?,  
            qty=?, cargo_net=?, contract=?, customer=?, recipient2=?, station=?, gtd=?,  
            md5hash=?, date_import=?, date_export=0 
            WHERE key_number=?"""
        return query

    @staticmethod
    def prepare_update(data, md5hash):
        # .. (from receipt_rwbreceipt) date_import[-3], (from receipt_rwbreceiptcar) gtd[-2], date_import[-1]
        return data[1:-3] + (data[-2], md5hash, max(data[-3], data[-1]), data[0],)


class RwbReceiptCar(BaseTable):
    source_base = 'receipt'

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
    def update_from_source():
        import sqlite3

        with sqlite3.connect(connection_str()) as conn:
            cursor = conn.cursor()
            max_date_import1 = max_date_import('adapter_rwbreceiptcar', cursor)
            current_adapter = current_hash('adapter_rwbreceiptcar', cursor)

            # start with receipt from receipt_rwbreceipt
            source_name = f'{RwbReceiptCar.source_base}.sqlite3'
            cursor.execute(f"ATTACH DATABASE '{connection_str(source_name)}' AS S")
            query = f"""SELECT key_number, rwbill_no, car_no, cargo_net, contract, gtd,
            condition, state, contract_trade, owner, date_import FROM S.receipt_rwbreceiptcar 
            WHERE date_import>{max_date_import1}"""
            cursor.execute(query)
            receipt = {row[0]: (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],)
                       for row in cursor.fetchall()}

            hashed_receipt = {md5(str(rec[:-1]).encode('utf-8')).hexdigest(): key for key, rec in receipt.items()}

            # check hash for deleted
            deleted_count = 0 # BaseTable.delete_directly(RwbReceiptCar, hashed_receipt, current_adapter)

            # check hash for new
            inserted_count, updated_count = BaseTable.add_directly(RwbReceiptCar, hashed_receipt, current_adapter, receipt)

        return deleted_count, inserted_count, updated_count

    @staticmethod
    def query_insert():
        query = f"""INSERT INTO adapter_rwbreceiptcar (key_number, rwbill_no, car_no, cargo_net, contract, gtd,
            condition, state, contract_trade, owner, date_import, md5hash, date_export)
            VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, 0)"""
        return query

    @staticmethod
    def prepare_insert(data, md5hash):
        return data + (md5hash, )

    @staticmethod
    def query_update():
        query = f"""UPDATE adapter_rwbreceiptcar SET rwbill_no=?, car_no=?, cargo_net=?, contract=?, gtd=?,  
            condition=?, state=?, contract_trade=?, owner=?, date_import=?, md5hash=?, date_export=0 
            WHERE key_number=?"""
        return query

    @staticmethod
    def prepare_update(data, md5hash):
        return data[1:] + (md5hash, data[0],)


class RwbReceiptMemo(BaseTable):
    source_base = 'receipt'

    route = models.CharField(max_length=50)
    memo = models.CharField(max_length=50)
    notice = models.IntegerField(default=0)
    path_no = models.IntegerField(default=0)

    in_date = models.CharField(max_length=10)
    in_time = models.CharField(max_length=5)
    end_date = models.CharField(max_length=10)
    end_time = models.CharField(max_length=5)

    @staticmethod
    def update_from_source():
        import sqlite3

        with sqlite3.connect(connection_str()) as conn:
            cursor = conn.cursor()
            max_date_import1 = max_date_import('adapter_rwbreceiptmemo', cursor)
            current_adapter = current_hash('adapter_rwbreceiptmemo', cursor)

            # start with receipt from receipt_rwbreceipt
            source_name = f'{RwbReceiptMemo.source_base}.sqlite3'
            cursor.execute(f"ATTACH DATABASE '{connection_str(source_name)}' AS S")
            query = f"""SELECT key_number, route, memo, notice, path_no, in_date, in_time, end_date, end_time, 
                md5hash, date_import FROM S.receipt_memotime
                WHERE date_import>{max_date_import1}"""
            cursor.execute(query)
            receiptmemo = {row[0]: (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                    row[10], ) for row in cursor.fetchall()}
            # не считаем  [-2]= md5hash  (источник все колонки таблицы источника = получателя)
            hashed_receipt = {rec[-2]: key for key, rec in receiptmemo.items()}

            # check hash for deleted
            deleted_count = 0  # BaseTable.delete_directly(RwbReceiptMemo, hashed_receipt, current_adapter)

            # check hash for new
            inserted_count, updated_count = BaseTable.add_directly(RwbReceiptMemo, hashed_receipt, current_adapter,
                                                                   receiptmemo)

        return deleted_count, inserted_count, updated_count

    @staticmethod
    def query_insert():
        query = f"""INSERT INTO adapter_rwbreceiptmemo (key_number, route, memo, notice, path_no, in_date, in_time,   
            end_date, end_time, md5hash, date_import, date_export)
            VALUES (?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, 0)"""
        return query

    @staticmethod
    def prepare_insert(data, md5hash):
        return data

    @staticmethod
    def query_update():
        query = f"""UPDATE adapter_rwbreceiptmemo SET route=?, memo=?, notice=?, path_no=?, in_date=?, in_time=?,  
            end_date=?, end_time=?, md5hash=?, date_import=?, date_export=0  
            WHERE key_number=?"""
        return query

    @staticmethod
    def prepare_update(data, md5hash):
        return data[1:] + (data[0],)


class RwbReceiptMemoCar(BaseTable):
    source_base = 'receipt'

    rwbill_no = models.CharField(max_length=50)
    car_no = models.CharField(max_length=10)
    date_in = models.CharField(max_length=10)
    time_in = models.CharField(max_length=5)
    route_memo_notice = models.CharField(max_length=255, default='')

    @staticmethod
    def update_from_source():
        import sqlite3

        with sqlite3.connect(connection_str()) as conn:
            cursor = conn.cursor()
            max_date_import1 = max_date_import('adapter_rwbreceiptmemocar', cursor)
            current_adapter = current_hash('adapter_rwbreceiptmemocar', cursor)

            source_name = f'{RwbReceiptMemoCar.source_base}.sqlite3'
            cursor.execute(f"ATTACH DATABASE '{connection_str(source_name)}' AS S")
            query = f"""SELECT key_number, rwbill_no, car_no, get_date, get_time, 
                        route, memo, notice, date_import FROM S.receipt_rwbreceiptcartime 
                WHERE date_import>{max_date_import1}"""
            cursor.execute(query)
            receipt_memo_car = {row[0]: (row[0], row[1], row[2], row[3], row[4], ':'.join(row[5:7] + (str(row[7]),)),
                                row[8],) for row in cursor.fetchall()}

            hashed_receipt = {md5(str(rec[:-1]).encode('utf-8')).hexdigest(): key
                              for key, rec in receipt_memo_car.items()}

            # check hash for deleted
            deleted_count = 0  # BaseTable.delete_directly(RwbReceiptMemoCar, hashed_receipt, current_adapter)

            # check hash for new
            inserted_count, updated_count = BaseTable.add_directly(RwbReceiptMemoCar, hashed_receipt, current_adapter,
                                                                   receipt_memo_car)

        # select errors.key_number, memo.route, memo.memo, memo.notice
        # from (select A.key_number, A.rwbill_no, A.car_no
        # from receipt_rwbreceiptcartime A
        # left join receipt_memotime B
        # on A.route=B.route and A.memo=B.memo and A.notice=B.notice
        # where B.key_number is null) errors
        # left join receipt_memocar memo
        # on errors.rwbill_no=memo.rwbill_no and errors.car_no=memo.car_no
        # where memo.route is not null



        return deleted_count, inserted_count, updated_count

    @staticmethod
    def query_insert():
        query = f"""INSERT INTO adapter_rwbreceiptmemocar (key_number, rwbill_no, car_no, date_in, time_in, 
            route_memo_notice, date_import, md5hash, date_export)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?, 0)"""
        return query

    @staticmethod
    def prepare_insert(data, md5hash):
        return data + (md5hash, )

    @staticmethod
    def query_update():
        query = f"""UPDATE adapter_rwbreceiptmemocar SET rwbill_no=?, car_no=?, date_in=?, time_in=?, 
            route_memo_notice=?, date_import=?, md5hash=?, date_export=0 WHERE key_number=?"""
        return query

    @staticmethod
    def prepare_update(data, md5hash):
        return data[1:] + (md5hash, data[0], )


class RwbGtd(BaseTable):
    source_base = 'receipt'

    cargo = models.IntegerField(default=0)
    cargo_net = models.CharField(max_length=50)
    doc_date = models.CharField(max_length=10)

    @staticmethod
    def update_from_source():
        import sqlite3

        with sqlite3.connect(connection_str()) as conn:
            cursor = conn.cursor()
            max_date_import1 = max_date_import('adapter_rwbgtd', cursor)
            current_adapter = current_hash('adapter_rwbgtd', cursor)

            source_name = f'{RwbGtd.source_base}.sqlite3'
            cursor.execute(f"ATTACH DATABASE '{connection_str(source_name)}' AS S")
            query = f"""SELECT key_number, cargo, cargo_net, doc_date, md5hash, date_import FROM S.receipt_gtd 
                WHERE date_import>{max_date_import1}"""
            cursor.execute(query)
            receipt_gtd = {row[0]: (row[0], row[1], row[2], row[3], row[4], row[5],) for row in cursor.fetchall()}
            # не считаем  [-2]= md5hash  (источник все колонки таблицы источника = получателя)
            hashed_receipt = {rec[-2]: key for key, rec in receipt_gtd.items()}

            # check hash for deleted
            deleted_count = 0  # BaseTable.delete_directly(RwbGtd, hashed_receipt, current_adapter)

            # check hash for new
            inserted_count, updated_count = BaseTable.add_directly(RwbGtd, hashed_receipt, current_adapter, receipt_gtd)

        return deleted_count, inserted_count, updated_count

    @staticmethod
    def query_insert():
        query = """INSERT INTO adapter_rwbgtd (key_number, cargo, cargo_net, doc_date,
                md5hash, date_import, date_export) VALUES (?, ?, ?, ?,  ?, ?, 0)"""
        return query

    @staticmethod
    def prepare_insert(data, md5hash):
        return data

    @staticmethod
    def query_update():
        query = """UPDATE adapter_rwbgtd SET cargo=?, cargo_net=?, doc_date=?,
                md5hash=?, date_import=?, date_export=0 WHERE key_number=?"""
        return query

    @staticmethod
    def prepare_update(data, md5hash):
        return data[1:] + (data[0], )


def all_tables():
    from sys import modules
    from inspect import getmembers, isclass

    current_module = modules[__name__]
    x = [obj for _, obj in getmembers(current_module, isclass)]
    y = [obj for obj in x if hasattr(obj, 'update_from_source')]
    return y


def crud_directly(query, values):
    import sqlite3
    from os.path import abspath, dirname, join
    from .apps import AdapterConfig
    from impex import utils
    from impex import settings

    base_name = f'{AdapterConfig.name}.sqlite3'
    with sqlite3.connect(join(settings.BASE_DIR, base_name)) as conn:
        cursor = conn.cursor()
        utils.execute_many(values, conn, cursor, query)


def connection_str(source_name=None):
    from os.path import join
    from .apps import AdapterConfig
    from impex import settings

    if source_name is None:
        base_name = f'{AdapterConfig.name}.sqlite3'
    else:
        base_name = source_name

    return join(settings.BASE_DIR, base_name)


def max_date_import(table_name, cursor):
    query = f'SELECT max(date_import) FROM {table_name}'
    cursor.execute(query)
    return cursor.fetchone()[0] or 0


def current_hash(table_name, cursor):
    query = f'SELECT key_number, md5hash FROM {table_name}'
    cursor.execute(query)
    return {row[1]: row[0] for row in cursor.fetchall()}
