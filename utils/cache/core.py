import multiprocessing.dummy as threading
import redis
import logging
import os
import datetime
import pymysql
from sklearn.externals import joblib
import numpy as np
from utils.consts import const
from .pak import *
import sys


class Algorithm(object):
    """
    算法具体实现
    """

    def __init__(self):
        self.init_redis()
        self.kname_whitelist = 'whiteset'
        self.kname_blacklist = 'blackset'
        self.svmjudger = Svmjudger()

    def init_redis(self):
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        return

    def judge(self, rawpak):

        if self.query_whitelist(rawpak):
            return False

        if self.query_blacklist(rawpak):
            return True

        if self.query_exist(rawpak):
            if self.query_timeout(rawpak):
                self.drop(rawpak)
                self.init_rdpak(rawpak)
                return False
            else:
                if self.time_switch(rawpak):
                    return True
                else:
                    self.update(rawpak)
                    return False
        else:
            self.init_rdpak(rawpak)
            return False

    def time_switch(self, rawpak):
        """
        根据查询的次数进行分流
        """
        querytime = self.fetch_rdpak(rawpak).query

        if querytime < 30:
            return False
        elif querytime == 50:
            if self.svmjudger.judge(self.fetch_rdpak(rawpak)):
                return True
            else:
                return False
        elif querytime > 200:
            return True

    def update(self, rawpak):
        if rawpak.command == 'FlightShopping':
            self.rd.hincrby(rawpak.ip, 'query')
        else:
            self.rd.hincrby(rawpak.ip, 'order')
        self.rd.hset(rawpak.ip, 'ltime', rawpak.querytime.timestamp())
        return

    def drop(self, rawpak):
        self.rd.delete(rawpak.ip)
        return

    def query_whitelist(self, rawpak):
        if self.rd.sismember(self.kname_whitelist, rawpak.ip):
            return True
        else:
            return False

    def query_blacklist(self, rawpak):
        if self.rd.sismember(self.kname_blacklist, rawpak.ip):
            return True
        else:
            return False

    def query_exist(self, rawpak):
        if self.rd.exists(rawpak):
            return True
        else:
            return False

    def query_timeout(self, rawpak):
        if rawpak.querytime.timestamp() - self.fetch_rdpak(rawpak).stime > cacheconf.TIMEOUT:
            return True
        else:
            return False

    def fetch_rdpak(self, rawpak):
        dic = {key.decode('utf-8'): value.decode('utf-8') for key, value in self.rd.hgetall(rawpak.ip).items()}
        return Rdpak(ip=rawpak.ip, query=int(dic['query']), order=int(dic['order']),
                     stime=float(dic['stime']), ltime=float(dic['ltime']))

    def init_rdpak(self, rawpak):
        self.rd.hmset(rawpak.ip, {'query': 1, 'stime': rawpak.querytime.timestamp(),
                                  'ltime': rawpak.querytime.timestamp(), 'order': 0})
        return


class Core(threading.Process):
    """
    算法的多线程及输出封装层
    """

    def __init__(self, conf, queue):
        super().__init__()
        self.queue = queue
        self.init_conf(conf)
        self.algorithm = Algorithm()
        return

    def run(self):
        rawpak = self.queue.get()
        self.rec(rawpak)

    def init_conf(self, conf):
        self.outputs = list()
        if conf.db:
            self.outputs.append(Database())
        if conf.log:
            self.outputs.append(Logger())
        if conf.file:
            self.outputs.append(FileLogger())
        return

    def output(self, rawpak):
        catchedpak = Catchedpak(
            ip=rawpak.ip,
            time=rawpak.querytime,
            type='cache'
        )
        [output.output(catchedpak) for output in self.outputs]
        return

    def rec(self, rawpak):
        if self.algorithm.judge(rawpak):
            self.output(rawpak)
            return True
        else:
            return False


class Svmjudger(object):
    def __init__(self):
        self.init_model()
        self.init_db()

    def init_db(self):
        self.connection = pymysql.connect(**const.DBCONF)
        self.cursor = self.connection.cursor()

    def init_model(self):
        # utils/目录下
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        svmpath = os.path.join(BASE_DIR, 'svmmodel', 'svmmodel.pkl')

        self.svm_model = joblib.load(svmpath)

        svmpath = os.path.join(BASE_DIR, 'model20', 'svmmodel.pkl')
        self.svm_model20 = joblib.load(svmpath)

        svmpath = os.path.join(BASE_DIR, 'model50', 'svmmodel.pkl')
        self.svm_model50 = joblib.load(svmpath)

        svmpath = os.path.join(BASE_DIR, 'model100', 'svmmodel.pkl')
        self.svm_model100 = joblib.load(svmpath)
        return

    def fetch_svmpak(self, rdpak):
        self.cursor.execute('''SELECT ip, depature, arrival, querytime, result
                                   FROM {table}
                                   WHERE ip = '{ip}'
                                   AND querytime
                                   BETWEEN '{stime}'
                                   AND '{etime}'
                                   ORDER BY querytime
                                '''.format(table=cacheconf.BACKUPTABLE, ip=rdpak.ip,
                                           stime=datetime.datetime.fromtimestamp(rdpak.stime),
                                           etime=datetime.datetime.fromtimestamp(rdpak.ltime)))

        dataraws = [Rawpak(*row) for row in self.cursor.fetchall()]
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

        for i in range(len(dataraws) - 1):
            interval.append(querytime[i + 1].timestamp() - querytime[i].timestamp())

        return Svmpak(duration=rdpak.ltime - rdpak.stime,
                      querycount=len(dataraws), depcount=len(set(depature)),
                      arrcount=len(set(arrival)), errpro=errcount / (len(dataraws) + 1),
                      std=np.array(interval).std(),
                      mean=np.array(interval).mean())

    def judge(self, rdpak):
        svmpak = self.fetch_svmpak(rdpak)
        resultpro = self.svm_model.predict_proba([svmpak])[0][1]

        if resultpro > cacheconf.SVMPROBABILITY:
            return True
        else:
            return False


class Output(object):
    """
    输出的抽象类
    """

    def output(self, catchedpak):
        raise NotImplementedError


class Database(Output):
    """
    数据库的实例
    """

    def __init__(self):
        self.connection = pymysql.connect(**const.DBCONF)
        self.cursor = self.connection.cursor()

        self.cursor.execute(
            'CREATE TABLE cachedata.catchedinfo (\n'
            '`ip` varchar(30) NOT NULL,\n'
            '`querytime` datetime NOT NULL,\n'
            '`type` varchar(30) DEFAULT NULL,\n'
            ') ENGINE=MyISAM DEFAULT CHARSET=utf8\n'
        )

    def output(self, catchedpak):
        self.cursor.execute((
            'insert into {table}\n'
            '(`ip`, `time`, `type`)\n'
            'VALUES (\'{ip}\', \'{time}\', \'{type}\')'
        ).format(
            table=cacheconf.CATCHEDTABLE,
            ip=catchedpak.ip,
            time=catchedpak.time,
            type='cache')
        )


class Logger(Output):
    """
    日志的实例
    """

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.setLevel(logging.INFO)

    def output(self, catchedpak):
        self.logger.info('ip: ' + str(catchedpak.ip) + ', time: ' + str(catchedpak.time))


class FileLogger(Output):
    """
    文件日志的实例
    """

    def __init__(self):
        self.logger = logging.getLogger('filelogger')
        self.logger.addHandler(logging.FileHandler(cacheconf.FILELOG_PATH))
        self.logger.setLevel(logging.INFO)

    def output(self, catchedpak):
        self.logger.info('ip: ' + str(catchedpak.ip) + ', time: ' + str(catchedpak.time))
