import pymysql
import redis
import datetime

from collections import namedtuple
from .const import const


class Mysqldb(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()


class Cache():
    def __init__(self, speed=False, wpro=50, spro=50, timeout=7200, mlen=30000):
        super().__init__()
        self.mdb = Mysqldb()
        self.speed = speed
        self.wpro = wpro
        self.spro = spro
        self.timeout = timeout
        self.mlen= mlen

        self.rd = redis.StrictRedis()
        self.rd.flushall()

        # self.mdb.cursor.execute('drop table {table}'.format(table=const.IPCATCHED))

        self.mdb.cursor.execute('''
        CREATE TABLE IF NOT EXISTS {table} (
        `ip` varchar(50) NOT NULL,
        `time` datetime NOT NULL,
        KEY  (`time`,`ip`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk;
        '''.format(table=const.IPCATCHED))


    def log(self, msg):
        print('ip: ', msg.ip, 'time:', msg.querytime)
        self.mdb.cursor.execute('''
                                insert {table}
                                (ip, time)
                                value('{ip}', '{time}')
                                '''.format(table=const.IPCATCHED, ip=msg.ip, time=msg.querytime))
        return


    def produce(self):
        self.mdb.cursor.execute('''select count(*)
                                   from {table}'''.format(table=const.PROCCMD))

        datacount = self.mdb.cursor.fetchone()[0]

        # ip, querytime, command, index_id
        self.mdb.cursor.execute('select * from {table} limit 1'.format(table=const.PROCCMD))
        Data = namedtuple('Data', [x[0] for x in self.mdb.cursor.description])

        for i in range(datacount):
            if i % 10000 == 0:
                print(i)

            self.mdb.cursor.execute('select * from {table} where index_id = {index}'.format(
                table=const.PROCCMD, index=i + 1))

            self.rec(Data(*(self.mdb.cursor.fetchone())))
        return

    # msg接受到一个命名元组
    # ip, querytime, command
    def rec(self, msg):
        # 判断是否超出缓存
        # if self.rd.dbsize() > self.mlen:
        #     self.clear(now=msg.querytime)

        # 判断是否已存在
        if self.rd.exists(msg.ip):
            # 判断之前的数据是否要废弃
            if msg.querytime.timestamp() - self.tp(self.rd.hget(msg.ip, 'ltime').decode('utf8')).timestamp() > self.timeout:
                # 清除数据
                self.rd.delete(msg.ip)
                # 初始化数据
                self.rd.hmset(msg.ip, {'query': 1, 'stime': msg.querytime, 'ltime': msg.querytime, 'order': 0})
                return

            else:
                # 将哈希表解析成字典
                data = self.dp(msg.ip)

                # 判断是否为极端爬虫行为
                if int(data['query']) > self.wpro and int(data['order']) == 0:
                    if not self.rd.sismember('warn', data['ip']):
                        self.rd.sadd('warn', data['ip'])
                        return
                    else:
                        self.log(msg)
                        return
                else:
                    # 刷新缓存
                    if msg.command == 'FlightShopping':
                        self.rd.hincrby(msg.ip, 'query')
                    else:
                        self.rd.hincrby(msg.ip, 'order')
                    self.rd.hset(msg.ip, 'ltime', msg.querytime)
                    return

        else:
            self.rd.hmset(msg.ip, {'query': 1, 'stime': msg.querytime, 'ltime': msg.querytime, 'order': 0})
            return


    @staticmethod
    def tp(strtime):
        timeymd, timehms = strtime.split(' ')
        year, mon, day = timeymd.split('-')
        hour, min, sec = timehms.split('.')[0].split(':')
        return datetime.datetime(int(year), int(mon), int(day), int(hour), int(min), int(sec))

    def dp(self, ip):
        data = {key.decode('utf-8'): value.decode('utf-8') for key, value in self.rd.hgetall(ip).items()}
        data['ip'] = ip
        return data


