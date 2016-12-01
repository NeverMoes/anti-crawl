import pymysql as mdb
import datetime


class Mysqldb(object):
    def __init__(self):
        try:
            self.connect = mdb.connect(
                host='127.0.0.1', port=3306, user='root', passwd='yi')
            self.cursor = self.connect.cursor()
            self.cursor.execute('USE zhx')
        except Exception as e:
            print(e)


db = Mysqldb()



# 创建表

"""


db.cursor.execute('''
CREATE TABLE IF NOT EXISTS `cmd_cache` (
  `ip` varchar(50) NOT NULL,
  `querytime` datetime NOT NULL,
  `command` varchar(50) NOT NULL,
  KEY `querytime` (`querytime`),
  KEY `ip` (`ip`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk
''')



db.cursor.execute('''
CREATE TABLE IF NOT EXISTS `ft_day` (
  `ip` varchar(50) NOT NULL,
  `querycount` int NOT NULL,
  `ordercount` int NOT NULL,
  `querytime` datetime NOT NULL,
  KEY `ip` (`ip`),
  KEY `querycount` (`querycount`),
  KEY `ordercount` (`ordercount`),
  KEY `querytime` (`querytime`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk
''')



db.cursor.execute('''
CREATE TABLE IF NOT EXISTS `ss_cache` (
  `ip` varchar(50) NOT NULL,
  `querycount` int NOT NULL,
  `querytime` datetime NOT NULL,
  KEY `ip` (`ip`),
  KEY `querytime` (`querytime`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk
''')

"""

#插入数据


# db.cursor.execute('''
# insert cmd_cache
# select ip, querytime, command
# from totalcmd_dzairb2c_szx411 as cmd
# where cmd.command='FlightShopping'
# or cmd.command='SellSeat'
# ''')
#

# 分时间插入



begin = datetime.date(2016, 9, 23)
end = datetime.date(2016, 9, 30)

for i in range((end - begin).days+1):
    day = begin + datetime.timedelta(days=i)
    day_1 = begin + datetime.timedelta(days=i+1)

    db.cursor.execute('''
    insert ft_day
    select ip, count(ip), 0, '{date}'
    from cmd_cache
    where querytime >= '{date}'
    and querytime < '{date_1}'
    and command = 'FlightShopping'
    group by ip
    '''.format(date=str(day), date_1=str(day_1)))


    db.cursor.execute('''
    insert ss_cache
    select ip, count(ip), '{date}'
    from cmd_cache
    where querytime >= '{date}'
    and querytime < '{date_1}'
    and command = 'SellSeat'
    group by ip
    '''.format(date=str(day), date_1=str(day_1)))


# 连接数据
db.cursor.execute('''
update ft_day
inner join ss_cache
on ft_day.ip = ss_cache.ip
AND ft_day.querytime = ss_cache.querytime
set ft_day.ordercount = ss_cache.querycount
''')

# 删除缓存表
db.cursor.execute('''
drop table ss_cache
''')

db.connect.commit()
db.connect.close()

