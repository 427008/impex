from datetime import datetime


def execute_many(values, cn, cur, command):
    for i in range(int(len(values) / 1000) + 1):
        cur.executemany(command, values[i * 1000:(i + 1) * 1000])
        cn.commit()


def date_format(data):
    ret = datetime.fromordinal(int(data)+672046).strftime('%Y-%m-%d') if data.isdigit() else t2000()
    return ret


def time_format(data):
    ret = '{}:{:0>2}'.format(int(int(data) / 60 / 60),
                                        int((int(data) - int(int(data) / 60 / 60) * 60 * 60) / 60)) \
        if data.isdigit() else '0:00'
    return ret


def t2000():
    return datetime(2000, 1, 1).strftime('%Y-%m-%d')
