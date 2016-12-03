import pymysql
import redis


class Mysqldb(object):
    def __init__(self,database):
        self.connect = pymysql.connect(
            host='127.0.0.1', port=3306, user='root', passwd='yi')
        self.cursor = self.connect.cursor()
        self.cursor.execute('USE {database}'.format(database=database))


class Cache(object):
    def __init__(self):
        self.rd = redis.StrictRedis()

    # mes应该接收到一个字典
    def rec(self, mes):
        result = self.rd.pipeline().hmset(mes['ip'], {'time': mes['querytime'], 'count': 1, 'command': mes['command']})

    def warn(self):
        pass


mdb = Mysqldb()
mdb.cursor.execute('select count(*) from cmd_cache')
datacount = mdb.cursor.fetchone()[0]
cache = Cache()
for i in range(100):
    mdb.cursor.execute('select * from cmd_cache where index_id = {index}'.format(index=i+1))
    cache.rec(mdb.cursor.fetchone())





