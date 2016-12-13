from utils.cacheten import Cache

cache = Cache()

cache.run()



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