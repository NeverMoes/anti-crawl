# sql = ('adfsdf: {name}'
#     'asdfasdfsdafsd'
#     'sadfsdfsd {aaa}').format(name='aa' , aaa='aa')
#
#
# print(sql)



import pymysql
import datetime

conf = {'host': '127.0.0.1', 'port': 3306,
                'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

conn = pymysql.connect(**conf)
cursor = conn.cursor()



'''

CREATE TABLE IF NOT EXISTS procdata.catchedcount (
  `time` datetime NOT NULL,
  `querycount` int NOT NULL,
  `ipcount` int NOT NULL,
  KEY  (`time`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk;

'''

'''

CREATE TABLE IF NOT EXISTS procdata.catchedcount (
  `time` datetime NOT NULL,
  `querycount` int NOT NULL,
  KEY  (`time`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk;

'''

now = datetime.datetime(2016, 8, 23, 0, 0, 0)
end = datetime.datetime(2016, 8, 24, 0, 0, 0)
interval = datetime.timedelta(minutes=10)


while True:
    if now != end:
        sql = '''insert procdata.catchedcount
                          select '{stime}', count(ip)
                          from procdata.ip_catched
                          where `time` > '{stime}'
                          and `time` < '{etime}' '''.format(stime=now, etime=now+interval)
        print(sql)
        cursor.execute(sql)
        now += interval
    else:
        break


#


'''
insert procdata.catchedcount
select count(ip) '2016-08-24 00:00:00'
from procdata.ip_catched
where time > '2016-08-23 00:00:00'
and time < '2016-08-23 00:00:10'
'''
