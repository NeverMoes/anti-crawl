import pymysql
import redis
import datetime

from collections import namedtuple
from .const import const


class Mysqldb(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()


class Cache(object):
    def __init__(self, speed=False, warnpro=200, svmpro=50, timeout=7200, mlen=30000):
        self.speed = speed
        self.warnpro = warnpro
        self.spro = svmpro
        self.timeout = timeout
        self.mlen = mlen

        self.redis_init()
        self.mdb_init()

    # 连接redis并,定义cache的数据包
    def redis_init(self):
        self.Datacache = namedtuple('Datacache', ['ip', 'query', 'order', 'stime', 'ltime'])
        self.Datawarn = namedtuple('Datacache', ['ip', 'day1', 'day2', 'day3'])
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        return

    # 持久化数据
    def mdb_init(self):
        self.mdb = Mysqldb()
        # self.mdb.cursor.execute('drop table {table}'.format(table=const.IPCATCHED))

        self.mdb.cursor.execute('''
        CREATE TABLE IF NOT EXISTS {table} (
        `ip` varchar(50) NOT NULL,
        `time` datetime NOT NULL,
        KEY  (`time`,`ip`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk;
        '''.format(table=const.IPCATCHED))
        return

    def run(self):
        self.produce(sdate=datetime.date(2016, 8, 23))
        self.produce(sdate=datetime.date(2016, 8, 24))
        self.produce(sdate=datetime.date(2016, 8, 25))

    # 生产数据
    def produce(self, sdate):
        edate = sdate + datetime.timedelta(days=1)
        # 确定数据总数
        self.mdb.cursor.execute('''select count(*)
                                   from {table}
                                   WHERE querytime >= '{sdate}'
                                   AND querytime < '{edate}'
                                   '''.format(sdate=sdate,
                                              edate=edate, table=const.PROCCMD))
        datacount = self.mdb.cursor.fetchone()[0]

        # 确定索引起始位置
        self.mdb.cursor.execute('''select index_id
                                   from {table}
                                   WHERE querytime >= '{sdate}'
                                   limit 1
                                   '''.format(sdate=sdate, edate=edate, table=const.PROCCMD))
        indexloc = self.mdb.cursor.fetchone()[0]

        # 建立数据包结构
        # ip, querytime, command, index_id
        self.mdb.cursor.execute('select * from {table} limit 1'.format(table=const.PROCCMD))
        Datasent = namedtuple('Datasent', [x[0] for x in self.mdb.cursor.description])

        # 开始读取数据
        for i in range(indexloc, indexloc+datacount, 10000):
            # 一次读取10000条
            print(i)
            self.mdb.cursor.execute('''select ip, querytime, command, index_id
                                       from {table} where index_id >= {index}
                                       limit 10000
                                       '''.format(table=const.PROCCMD, index=i))
            rows = self.mdb.cursor.fetchall()
            for row in rows:
                self.rec(Datasent(*row))
        return

    # datasent接受到一个命名元组
    # ip, querytime, command
    # 注意cache中的时间都为timestamp

    def rec(self, datasent):
        # 判断是否超出缓存
        # if self.rd.dbsize() > self.mlen:
        #     self.clear(now=datasent.querytime)

        if self.rd.exists('warn:'+datasent.ip):
            if datasent.command == 'FlightShopping':
                datawarn = self.getwarn(datasent.ip)
                if self.calcuwarn(datawarn):
                    self.catchip(datasent)
                    return
                else:
                    self.refreshwarn(datasent.ip)
            else:
                self.clearwarn(datasent.ip)
                self.setip(datasent)
        else:
            # 判断是否已存在
            if self.rd.exists(datasent.ip):
                # 取出数据
                datacache = self.getip(datasent.ip)
                # 判断是否超时
                if datasent.querytime.timestamp() - datacache.stime > self.timeout:
                    self.clearip(datasent)
                    self.setip(datasent)
                else:
                    # 判断是否为极端爬虫行为
                    if datacache.query > self.warnpro and datacache.order == 0:
                        self.setwarn(datasent.ip)
                        self.catchip(datasent)
                    else:
                        # 刷新缓存
                        self.refreship(datasent)
                        return
            else:
                self.setip(datasent)
                return

    def catchip(self, datasent):
        print('ip: ', datasent.ip, 'time: ', datasent.querytime)
        self.mdb.cursor.execute('''insert into {table}
                                   (`ip`, `time`)
                                    VALUES ('{ip}', '{time}')
                                    '''.format(table=const.IPCATCHED,
                                               ip=datasent.ip, time=datasent.querytime))

    # 刷新数据
    def calcuwarn(self, datawarn):
        if 0.3 * datawarn.day3 + 0.6*datawarn.day2 + datawarn.day1 > self.warnpro:
            return True
        else:
            return False

    def dayrefresh(self, date):
        # 清理超时IP
        ips = self.rd.keys(r'\d+\.\d+\.\d+\.\d+')
        nowtimestamp = date.timestamp()
        for ip in ips:
            datacache = self.getip(ip)
            if nowtimestamp - datacache.ltime > self.timeout:
                self.clearip(ip)

        # 刷新历史表
        warnips = self.rd.keys(r'warn:\d+\.\d+\.\d+\.\d+')
        for warnip in warnips:
            datawarn = self.getwarn(warnip, prefix=False)
            self.rd.hmset(warnip, {'day1': 0, 'day2': datawarn.day1, 'day3':datawarn.day2})
        return

    def setwarn(self, ip):
        self.rd.hmset('warn:'+ip, {'day1': 1, 'day2': 0, 'day3': 0})
        self.rd.delete(ip)
        return

    def getwarn(self, ip, prefix=True):
        if prefix:
            datadic = {key.decode('utf-8'): value.decode('utf-8') for key, value
                       in self.rd.hgetall('warn:'+ip).items()}
        else:
            datadic = {key.decode('utf-8'): value.decode('utf-8') for key, value
                       in self.rd.hgetall(ip).items()}

        return self.Datawarn(ip, int(datadic['day1']),
                                 int(datadic['day2']), int(datadic['day3']))

    def refreshwarn(self, ip):
        self.rd.hincrby('warn:'+ip, 'day1')
        return

    def clearwarn(self, ip):
        self.rd.delete('warn:'+ip)
        return

    def getip(self, ip):
        datadic = {key.decode('utf-8'): value.decode('utf-8') for key, value in self.rd.hgetall(ip).items()}
        return self.Datacache(ip, int(datadic['query']), int(datadic['order']),
                              float(datadic['stime']), float(datadic['ltime']))

    def setip(self, datasent):
        self.rd.hmset(datasent.ip, {'query': 1, 'stime': datasent.querytime.timestamp(),
                                    'ltime': datasent.querytime.timestamp(), 'order': 0})
        return

    def refreship(self, datasent):
        if datasent.command == 'FlightShopping':
            self.rd.hincrby(datasent.ip, 'query')
        else:
            self.rd.hincrby(datasent.ip, 'order')

        self.rd.hset(datasent.ip, 'ltime', datasent.querytime.timestamp())
        return

    def clearip(self, datasent):
        self.rd.delete(datasent.ip)
        return

    @staticmethod
    def strtimepar(strtime):
        timeymd, timehms = strtime.split(' ')
        year, mon, day = timeymd.split('-')
        hour, min, sec = timehms.split('.')[0].split(':')
        return datetime.datetime(int(year), int(mon), int(day), int(hour), int(min), int(sec))

    @staticmethod
    def log(datasent):
        print('ip: ', datasent.ip, 'time:', datasent.querytime)
        return

    @staticmethod
    def test(s):
        print(s)
        return s



