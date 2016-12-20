# from utils.cacherun import Cache
# import multiprocessing as mp
# import datetime
# from show.mysql import Mysqldb
# import time
#
# db = Mysqldb()
#
# cache = Cache('2016-08-23')
#
# cache.start()
#
# print(mp.active_children()[0].name)
#
# time.sleep(10)
#
# print(mp.active_children())
#
# if mp.active_children():
#     print('a')
#     data = list()
#     sdate = datetime.datetime.strptime(mp.active_children()[0].name, '%Y-%m-%d')
#     edate = sdate+datetime.timedelta(days=1)
#     interval = datetime.timedelta(minutes=10)
#
#     while True:
#         if sdate != edate:
#             db.cursor.execute('''
#                               select '{stime}', count(ip)
#                               from procdata._ipcatched
#                               where `time` >= '{stime}'
#                               and `time` < '{etime}' '''.format(stime=sdate, etime=sdate + interval))
#             row = db.cursor.fetchone()
#             data.append({'time': row[0], 'querycount': row[1]})
#             sdate += interval
#         else:
#             break
#     print(data)
#

from utils.cacheten import Cache

cache = Cache()

cache.run()

# res = cache.getsvm(cache.Datacache(ip='114.80.10.1', order=1, query=100,
#                                    stime=1471910400.0, ltime=1471996800.0))
#
# pro = cache.svmprodict(res)
#
# print(pro)



# from datetime import datetime
# def tp(strtime):
#     timeymd, timehms = strtime.split(' ')
#     year, mon, day = timeymd.split('-')
#     hour, min, sec = timehms.split('.')[0].split(':')
#     return datetime(int(year), int(mon), int(day), int(hour), int(min), int(sec))
#
# print(tp('2016-08-23 00:00:52').timestamp()-tp('2016-08-23 00:00:00').timestamp())



# import pymysql
# from utils.const import const
#
#
# class Mysqldb(object):
#     def __init__(self):
#         self.connect = pymysql.connect(
#             host=const.DBIP, port=const.DBPORT,
#             user=const.DBUSER, passwd=const.DBPASSWD)
#         self.cursor = self.connect.cursor()
#         self.cursor.execute('USE {dbname}'.format(dbname=const.DBNAME))
#
# mdb = Mysqldb()
#
# mdb.cursor.execute('select * from cmd_cache '
#                    'where querytime < "2016-08-24"'
#                    'order by querytime')
#
# print(mdb.cursor.fetchone())
# print(mdb.cursor.fetchone())
# print(mdb.cursor.fetchone())