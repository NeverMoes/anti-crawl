import pymysql
import redis
from collections import namedtuple
import multiprocessing as mp
from .const import const


class Mysqldb(object):
    def __init__(self):
        self.connect = pymysql.connect(
            host=const.DBIP, port=const.DBPORT,
            user=const.DBUSER, passwd=const.DBPASSWD)
        self.cursor = self.connect.cursor()
        self.cursor.execute('USE {dbname}'.format(dbname=const.DBNAME))


class Cache(mp.Process):
    def __init__(self, queue, speed=False, propor=100, timeout=7200):
        super().__init__()
        self.queue = queue
        self.speed = speed
        self.propor = propor
        self.timeout = timeout

        self.rd = redis.StrictRedis()
        self.rd.flushall()

    def run(self):
        self.re()


    def re(self):
        mdb = Mysqldb()
        mdb.cursor.execute('select count(*) from cmd_cache')
        datacount = mdb.cursor.fetchone()[0]

        mdb.cursor.execute('select * from cmd_cache where index_id = 1')
        Data = namedtuple('Data', [x[0] for x in mdb.cursor.description])

        for i in range(datacount):
            mdb.cursor.execute('select * from cmd_cache where index_id = {index}'.format(index=i+1))
            self.rec(Data(*(mdb.cursor.fetchone())))


    # msg接受到一个命名元组
    def rec(self, msg):
        if self.rd.exists(msg.ip):
            self.rd.hincrby(msg.ip, 'query')
            if msg.command == 'SellSeat':
                self.rd.hincrby(msg.ip, 'order')
            else:
                pass
            data = self.rd.hgetall(msg.ip)
            data = {key.decode('utf-8'): value.decode('utf-8') for key, value in data.items()}

            if int(data['query']) + 1 / int(data['order']) > self.propor:
                self.warn(data)
            else:
                pass

        else:
            self.rd.hmset(msg.ip, {'query': 1, 'time': msg.querytime, 'order': 1})


    def warn(self, msg):
        if self.rd.sismember('warned', msg['ip']):
            pass
        else:
            self.rd.sadd('warned', msg['ip'])
            self.queue.put(msg)


