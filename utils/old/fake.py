import pymysql
import datetime
import random
from functools import reduce

DBCONF = {'host': '127.0.0.1', 'port': 3306,
          'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}



def t(s):
    print(s)
    return s

with pymysql.connect(**DBCONF) as cursor:
    current_time = datetime.datetime(2016, 8, 23, 0, 0, 0)
    end_time = datetime.datetime(2016, 10, 24, 0, 0, 0)
    interval = datetime.timedelta(minutes=10)

    cursor.execute((
        'select query from procdata.ten_minute_count '
        'where ip="all" '
        'order by time '
    ))

    datarows = cursor.fetchall()
    for datarow in datarows:
        count = datarow[0]
        catched = int(count * (random.randint(60, 95)/100))
        cursor.execute((
            'insert procdata.catchedcount '
            '(time, querycount) '
            'values ("{stime}", {querycount})'
            ).format(
            stime=current_time,
            querycount=catched
        ))

        current_time += interval

