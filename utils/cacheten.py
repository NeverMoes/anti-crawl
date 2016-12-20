import pymysql
import redis
import datetime
import os
import numpy as np
from sklearn.externals import joblib
from collections import namedtuple
from .const import const


class Mysqldb(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()


class Cache(object):
    def __init__(self, speed=False, warnpro=100, svmpro=30, timeout=7200, mlen=30000):
        self.speed = speed
        self.warnpro = warnpro
        self.svmpro = svmpro
        self.timeout = timeout
        self.mlen = mlen

        self.redis_init()
        self.mdb_init()
        self.svm_init()
        self.load_history()

    # 连接redis并,定义cache的数据包
    def redis_init(self):
        self.Datacache = namedtuple('Datacache', ['ip', 'query', 'order', 'stime', 'ltime'])
        self.Datawarn = namedtuple('Datawarn', ['ip', 'now', 'day1', 'day2', 'day3'])

        self.warnprefix = 'warn:'
        self.svmset = 'svmset'

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
        `type` varchar(50) NOT NULL,
        KEY  (`time`,`ip`, `type`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk;
        '''.format(table=const.IPCATCHED))
        return

    def svm_init(self):
        # 定义数据包
        self.Datasvm = namedtuple('Datasvm', ['duration', 'querycount', 'depcount', 'arrcount', 'std', 'mean', 'errpro'])
        self.Dataraw = namedtuple('Dataraw', ['ip', 'depature', 'arrival', 'querytime', 'result'])

        # svm的黑白名单
        self.svmwhitelis = 'svmwhite'
        self.svmblacklis = 'svmblack'

        # 导入svm模型
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        svmpath = os.path.join(BASE_DIR, 'svmmodel', 'svmmodel.pkl')
        self.svm_model = joblib.load(svmpath)

        # 定义结果类型
        self.Svmres = namedtuple('Svmres', ['iscra', 'notcra'])
        self.svmres = self.Svmres(iscra=2, notcra=1)

        return

    def load_history(self):
        self.whitelist = 'whiteset'
        self.blacklist = 'blackset'

        whitelist = ['114.80.10.1', '61.155.159.41', '103.37.138.14', '103.37.138.110']
        blacklist = []

        [self.rd.sadd(self.whitelist, x) for x in whitelist]
        [self.rd.sadd(self.blacklist, x) for x in blacklist]

        return

    def run(self):
        self.produce(sdate=datetime.date(2016, 8, 23))
        self.produce(sdate=datetime.date(2016, 8, 24))
        self.produce(sdate=datetime.date(2016, 8, 25))
        self.produce(sdate=datetime.date(2016, 8, 26))
        self.produce(sdate=datetime.date(2016, 8, 27))

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
                self.cache(Datasent(*row))
        return

    # datasent接受到一个命名元组
    # ip, querytime, command
    # 注意cache中的时间都为timestamp

    def cache(self, datasent):
        # 判断是否超出缓存
        # if self.rd.dbsize() > self.mlen:
        #     self.clear(now=datasent.querytime)

        # 查询黑白名单
        if self.rd.sismember(self.whitelist, datasent.ip):
            return
        if self.rd.sismember(self.blacklist, datasent.ip):
            self.catchquery(datasent.ip, datasent.querytime, 'cache')
            return

        # 查询svm黑名单
        if self.rd.sismember(self.svmblacklis, datasent.ip):
            if datasent.command == 'FlightShopping':
                self.catchquery(datasent.ip, datasent.querytime, 'svm')
                return
            else:
                self.rd.srem(self.svmblacklis, datasent.ip)
            return

        # 查询svm白名单
        if self.rd.hexists(self.svmwhitelis, datasent.ip):
            # 判断是否满足再次进入svm的条件
            if int(self.rd.hget(self.svmwhitelis, datasent.ip).decode('utf8')) > self.svmpro*2:
                datacache = self.getip(datasent.ip)
                datasvm = self.getsvm(datacache)
                # 判断是否满足svm
                if self.svmprodict(datasvm):
                    self.setsvmblis(datasent.ip, True)
                    return
                else:
                    self.clearsvmwlis(datasent.ip)
                    return
            else:
                # 刷新svm白名单缓存
                self.refreshsvmwlis(datasent.ip)
                self.refreship(datasent)
                return

        # 查询cache历史记录
        if self.rd.exists(self.warnprefix+datasent.ip):
            if datasent.command == 'FlightShopping':
                datawarn = self.getwarn(datasent.ip)
                if self.calcuwarn(datawarn):
                    self.catchquery(datasent.ip, datasent.querytime, 'cache')
                    return
                else:
                    self.refreshwarn(datasent.ip)
                    return
            else:
                self.clearwarn(datasent.ip)
                return

        # 判断cache表是否已存在数据
        if self.rd.exists(datasent.ip):
            # 取出数据
            datacache = self.getip(datasent.ip)
            # 判断是否超时
            if datasent.querytime.timestamp() - datacache.stime > self.timeout:
                self.clearip(datasent)
                self.setip(datasent)
                return
            else:
                # 判断是否为极端爬虫行为
                if datacache.query > self.warnpro and datacache.order == 0:
                    self.setwarn(datasent.ip)
                    self.clearip(datasent)
                    self.catchquery(datasent.ip, datasent.querytime, 'cache')
                    return
                else:
                    # 判断是否需要svm
                    if datacache.query > self.svmpro:
                        datasvm = self.getsvm(datacache)
                        if self.svmprodict(datasvm):
                            # 加入svm缓存
                            self.setsvmblis(datasent.ip, False)
                            self.clearip(datasent)
                            return
                        else:
                            self.setsvmwlis(datasent.ip)
                            self.refreship(datasent)
                    else:
                        # 刷新缓存
                        self.refreship(datasent)
                    return
        else:
            self.setip(datasent)
            return

    def clearsvmwlis(self, ip):
        self.rd.hdel(self.svmwhitelis, ip)

    def refreshsvmwlis(self, ip):
        self.rd.hincrby(self.svmwhitelis, ip)
        return

    def setsvmblis(self, ip, isfromwlis):
        # 先删除cache和白名单中的缓存,
        # 再加入黑名单
        if isfromwlis:
            self.rd.hdel(self.svmwhitelis, ip)

        self.rd.delete(ip)
        self.rd.sadd(self.svmblacklis, ip)
        return

    def setsvmwlis(self, ip):
        self.rd.hset(self.svmwhitelis, ip, 1)

    def getsvm(self, datacache):
        self.mdb.cursor.execute('''SELECT ip, depature, arrival, querytime, result
                                   FROM {table}
                                   WHERE ip = '{ip}'
                                   AND querytime
                                   BETWEEN '{stime}'
                                   AND '{etime}'
                                   ORDER BY querytime
                                '''.format(table=const.RAWFF, ip=datacache.ip,
                                           stime=datetime.datetime.fromtimestamp(datacache.stime),
                                           etime=datetime.datetime.fromtimestamp(datacache.ltime)))
        dataraws = [self.Dataraw(*row) for row in self.mdb.cursor.fetchall()]
        querytime = list()
        depature = list()
        arrival = list()
        interval = list()
        errcount = 0
        for dataraw in dataraws:
            querytime.append(dataraw.querytime)
            depature.append(dataraw.depature)
            arrival.append(dataraw.arrival)
            if dataraw.result != 'ok.':
                errcount += 1

        for i in range(len(dataraws)-1):
            interval.append(querytime[i+1].timestamp()-querytime[i].timestamp())

        return self.Datasvm(duration=datacache.ltime-datacache.stime,
                            querycount=len(dataraws), depcount=len(set(depature)),
                            arrcount=len(set(arrival)), errpro=errcount/(len(dataraws)+1),
                            std=np.array(interval).std(),
                            mean=np.array(interval).mean())

    def svmprodict(self, datasvm):
        resultpro = self.svm_model.predict_proba([datasvm])[0][1]
        if resultpro > 0.90:
            return True
        else:
            return False

    # 将捕获到的ip放入数据库
    def catchquery(self, ip, time, type):
        print('ip: ', ip, 'time: ', time, 'type: ', type)
        self.mdb.cursor.execute('''insert into {table}
                                   (`ip`, `time`, `type`)
                                    VALUES ('{ip}', '{time}', '{type}')
                                    '''.format(table=const.IPCATCHED,
                                               ip=ip, time=time, type=type))
        return

    # 计算
    def calcuwarn(self, datawarn):
        if 0.25*datawarn.day3 + 0.5*datawarn.day2 + 0.75*datawarn.day1 + datawarn.now > self.warnpro:
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
        warnips = self.rd.keys(r'{prefix}\d+\.\d+\.\d+\.\d+'.format(prefix=self.warnprefix))
        for warnip in warnips:
            datawarn = self.getwarn(warnip, prefix=False)
            self.rd.hmset(warnip, {'now': 0, 'day1': datawarn.now+self.warnpro,
                                   'day2': datawarn.day1, 'day3': datawarn.day2})
        return

    def setwarn(self, ip):
        self.rd.hmset(self.warnprefix+ip, {'now': 1, 'day1': 0, 'day2': 0, 'day3': 0})
        return

    def getwarn(self, ip, prefix=True):
        if prefix:
            datadic = {key.decode('utf-8'): value.decode('utf-8') for key, value
                       in self.rd.hgetall(self.warnprefix+ip).items()}
        else:
            datadic = {key.decode('utf-8'): value.decode('utf-8') for key, value
                       in self.rd.hgetall(ip).items()}

        return self.Datawarn(ip, int(datadic['now']), int(datadic['day1']),
                             int(datadic['day2']), int(datadic['day3']))

    def refreshwarn(self, ip):
        self.rd.hincrby(self.warnprefix+ip, 'now')
        return

    def clearwarn(self, ip):
        self.rd.delete(self.warnprefix+ip)
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


