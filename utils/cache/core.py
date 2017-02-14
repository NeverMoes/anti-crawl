import redis
import datetime
import pymysql
from sklearn.externals import joblib
import numpy as np
from .pkg import *


class Core(object):
    """
    算法具体实现
    """

    def __init__(self):
        self.init_redis()
        self.kname_whitelist = 'whiteset'
        self.kname_blacklist = 'blackset'
        self.svmpre = Svmpredictor()
        return

    def init_redis(self):
        """
        redis的客户端是线程和进程都安全的
        """
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        return

    def predict(self, rawpkg):

        if self.query_whitelist(rawpkg):
            return False

        if self.query_blacklist(rawpkg):
            return True

        if self.query_exist(rawpkg):
            if self.query_timeout(rawpkg):
                self.drop(rawpkg)
                self.init_rdpkg(rawpkg)
                return False
            else:
                if self.time_switch(rawpkg):
                    return True
                else:
                    self.update(rawpkg)
                    return False
        else:
            self.init_rdpkg(rawpkg)
            return False

    def time_switch(self, rawpkg):
        """
        根据查询的次数进行分流
        """
        querytime = self.fetch_rdpkg(rawpkg).query

        if querytime < 30:
            return False
        elif querytime == 50:
            if self.svmpre.predict(self.fetch_rdpkg(rawpkg)):
                return True
            else:
                return False
        elif querytime > 200:
            return True

    def update(self, rawpkg):
        if rawpkg.command == 'FlightShopping':
            self.rd.hincrby(rawpkg.ip, 'query')
        else:
            self.rd.hincrby(rawpkg.ip, 'order')
        self.rd.hset(rawpkg.ip, 'ltime', rawpkg.querytime.timestamp())
        return

    def drop(self, rawpkg):
        self.rd.delete(rawpkg.ip)
        return

    def query_whitelist(self, rawpkg):
        if self.rd.sismember(self.kname_whitelist, rawpkg.ip):
            return True
        else:
            return False

    def query_blacklist(self, rawpkg):
        if self.rd.sismember(self.kname_blacklist, rawpkg.ip):
            return True
        else:
            return False

    def query_exist(self, rawpkg):
        if self.rd.exists(rawpkg):
            return True
        else:
            return False

    def query_timeout(self, rawpkg):
        if rawpkg.querytime.timestamp() - self.fetch_rdpkg(rawpkg).stime > cacheconf.TIMEOUT:
            return True
        else:
            return False

    def fetch_rdpkg(self, rawpkg):
        dic = {key.decode('utf-8'): value.decode('utf-8') for key, value in self.rd.hgetall(rawpkg.ip).items()}
        return Rdpkg(ip=rawpkg.ip, query=int(dic['query']), order=int(dic['order']),
                     stime=float(dic['stime']), ltime=float(dic['ltime']))

    def init_rdpkg(self, rawpkg):
        self.rd.hmset(rawpkg.ip, {'query': 1, 'stime': rawpkg.querytime.timestamp(),
                                  'ltime': rawpkg.querytime.timestamp(), 'order': 0})
        return


class Svmpredictor(object):
    def __init__(self):
        self.connection = pymysql.connect(**cacheconf.DBCONF)
        self.cursor = self.connection.cursor()

    def init_model(self):
        # utils/目录下
        svmpath = cacheconf.SVM_PATH
        self.svm_model = joblib.load(svmpath)
        return

    def fetch_svmpkg(self, rdpkg):
        cursor = self.cursor
        cursor.execute(
            ('SELECT ip, depature, arrival, querytime, result\n'
             'FROM {table}\n'
             'WHERE ip = \'{ip}\'\n'
             'AND querytime\n'
             'BETWEEN \'{stime}\'\n'
             'AND \'{etime}\'\n'
             'ORDER BY querytime\n'
             ).format(
                table=cacheconf.BACKUPTABLE,
                ip=rdpkg.ip,
                stime=datetime.datetime.fromtimestamp(rdpkg.stime),
                etime=datetime.datetime.fromtimestamp(rdpkg.ltime)
            )
        )
        dataraws = [Rawpkg(*row) for row in cursor.fetchall()]

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

        return Svmpkg(
            duration=rdpkg.ltime - rdpkg.stime,
            querycount=len(dataraws),
            depcount=len(set(depature)),
            arrcount=len(set(arrival)),
            errpro=errcount / (len(dataraws) + 1),
            std=np.array(interval).std(),
            mean=np.array(interval).mean()
        )

    def predict(self, rdpkg):
        svmpkg = self.fetch_svmpkg(rdpkg)
        resultpro = self.svm_model.predict_proba([svmpkg])[0][1]

        if resultpro > cacheconf.SVMPROBABILITY:
            return True
        else:
            return False
