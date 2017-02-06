import redis
import datetime
import pymysql
from sklearn.externals import joblib
import numpy as np
from .pak import *


class Core(object):
    """
    算法具体实现
    """

    def __init__(self, connpool):
        super().__init__()
        self.init_redis()
        self.kname_whitelist = 'whiteset'
        self.kname_blacklist = 'blackset'
        self.svmpre = Svmpredictor(connpool)
        return

    def init_redis(self):
        """
        redis的客户端是线程和进程都安全的
        """
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        return

    def predict(self, rawpak):

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
            if self.svmpre.predict(self.fetch_rdpak(rawpak)):
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


class Svmpredictor(object):
    def __init__(self, connpool):
        self.connpool = connpool
        self.init_model()

    def init_model(self):
        # utils/目录下
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        svmpath = os.path.join(BASE_DIR, 'svmmodels', 'svmmodel', 'svmmodel.pkl')

        self.svm_model = joblib.load(svmpath)

        return

    def fetch_svmpak(self, rdpak):
        conn = self.connpool.connect()
        cursor = conn.cursor()
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
                ip=rdpak.ip,
                stime=datetime.datetime.fromtimestamp(rdpak.stime),
                etime=datetime.datetime.fromtimestamp(rdpak.ltime)
            )
        )
        dataraws = [Rawpak(*row) for row in cursor.fetchall()]
        conn.close()

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

        return Svmpak(
            duration=rdpak.ltime - rdpak.stime,
            querycount=len(dataraws),
            depcount=len(set(depature)),
            arrcount=len(set(arrival)),
            errpro=errcount / (len(dataraws) + 1),
            std=np.array(interval).std(),
            mean=np.array(interval).mean()
        )

    def predict(self, rdpak):
        svmpak = self.fetch_svmpak(rdpak)
        resultpro = self.svm_model.predict_proba([svmpak])[0][1]

        if resultpro > cacheconf.SVMPROBABILITY:
            return True
        else:
            return False
