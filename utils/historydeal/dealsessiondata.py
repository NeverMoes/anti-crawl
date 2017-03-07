# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 14:04:12 2017

@author: mao133132
"""


import os
import sys
sys.path.insert(0, os.path.abspath('..'))


import pymysql
import datetime
import numpy as np
from sklearn.externals import joblib
import time
from sklearn import svm
from utils.consts import const

'''
CREATE TABLE IF NOT EXISTS `sessiondiv` (
  `ip` varchar(50) NOT NULL,
  `starttime` datetime NOT NULL,
  `endtime` datetime NOT NULL,
  `duration` int NOT NULL,
  `query` int DEFAULT 1,
  `buy` int DEFAULT 0,
  `depature` int DEFAULT 0,
  `arrival` int DEFAULT 0,
  `variance` float DEFAULT 0,
  `mean` float DEFAULT 0,
  `error` float DEFAULT 0,
  `hotcity` float DEFAULT 0,
  `normalcity` float DEFAULT 0,
  `class` int NULL,
  `svmclass` float NULL,
  PRIMARY KEY  (`ip`,`starttime`)
  KEY (`endtime`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk;

'''

#该函数主要用于历史数据处理
class MakeSession(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()
        self.svmpath = const.SVM_PATH
        self.svmmodel = joblib.load(self.svmpath)

    def main(self, d1, d2):
        self.MakeSession(d1, d2)    #将查询与订票数据转化为session的格式
        self.svmjudge(d1, d2)      #用默认使用的svm模型判断session的爬虫概率

    def MakeSession(self, d1, d2):
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(d1, timeformat)
        delta = datetime.timedelta(seconds=2)
        sessiondelta = datetime.timedelta(hours=1)
        date2 = date1 + delta
        dateend = datetime.datetime.strptime(d2, timeformat)
        cache = {}
        readcount = 360

        while (date1 < dateend):
            self.cursor.execute('''
            SELECT {ip},{querytime},{de},{ar},{error}
            from {table}
            WHERE {querytime} >= '{date}'
            AND {querytime} < '{date2}'
            '''.format(error=const.TABLE_ER, ip=const.TABLE_IP, de=const.TABLE_DE, ar=const.TABLE_AR, table=const.RAWFF,
                       querytime=const.TABLE_QUERYTIME, date=date1.strftime(timeformat),
                       date2=date2.strftime(timeformat)))
            result = self.cursor.fetchall()
            for i in range(len(result)):
                if result[i][0] == None:
                    continue
                if result[i][0] in cache:
                    temp1 = result[i][1]
                    temp2 = cache[result[i][0]]['time'][-1]
                    if temp1 - temp2 > sessiondelta:
                        self.clear_cache(result[i][0], cache[result[i][0]])
                        cache.pop(result[i][0])
                        if result[i][4] == 'ok.':
                            cache[result[i][0]] = {'count': 1, 'buy': 0, 'time': [result[i][1]],
                                                   'depature': [result[i][2]], 'arrival': [result[i][3]], 'error': 0}
                        else:
                            cache[result[i][0]] = {'count': 1, 'buy': 0, 'time': [result[i][1]],
                                                   'depature': [result[i][2]], 'arrival': [result[i][3]], 'error': 1}
                    else:
                        cache[result[i][0]]['count'] += 1
                        cache[result[i][0]]['time'].append(result[i][1])
                        cache[result[i][0]]['depature'].append(result[i][2])
                        cache[result[i][0]]['arrival'].append(result[i][3])
                        if result[i][4] != 'ok.':
                            cache[result[i][0]]['error'] += 1
                else:
                    if result[i][4] == 'ok.':
                        cache[result[i][0]] = {'count': 1, 'buy': 0, 'time': [result[i][1]], 'depature': [result[i][2]],
                                               'arrival': [result[i][3]], 'error': 0}
                    else:
                        cache[result[i][0]] = {'count': 1, 'buy': 0, 'time': [result[i][1]], 'depature': [result[i][2]],
                                               'arrival': [result[i][3]], 'error': 1}
            self.cursor.execute('''
            SELECT {ip}
            from {table}
            WHERE {querytime} >= '{date}'
            AND {querytime} < '{date2}'
            '''.format(ip=const.TABLE_IP, table=const.RAWSS, querytime=const.TABLE_QUERYTIME,
                       date=date1.strftime(timeformat), date2=date2.strftime(timeformat)))
            result = self.cursor.fetchall()

            for j in range(len(result)):
                if result[j][0] == None:
                    continue
                if result[j][0] in cache:
                    cache[result[j][0]]['buy'] = 1
                    self.clear_cache(result[j][0], cache[result[j][0]])
                    cache.pop(result[j][0])

            if readcount > 720:  # 过了1个小时
                readcount = 0
                tempkeylist = []
                print(datetime.datetime.strftime(date1, timeformat))
                for key in cache:
                    lasttime = cache[key]['time'][-1]
                    if date1 - lasttime > sessiondelta or cache[key]['count'] > 1000:
                        self.clear_cache(key, cache[key])
                        tempkeylist.append(key)
                for k in range(len(tempkeylist)):
                    cache.pop(tempkeylist[k])
            date1 += delta
            date2 += delta
            readcount += 1
            # =============================while循环结束===========================================
        for key in cache:  # 结尾将所有的数据都处理，然后放进数据库中
            self.clear_cache(key, cache[key])
        cache = 0
        return 1
    #将指定ip一段时间的数据转化为session格式，存入数据库
    def clear_cache(self, ip, data):
        class1list = ['PEK', 'PVG', 'CAN', 'CTU', 'SZX', 'SHA']
        class2list = ['KMG', 'XIY', 'CKG', 'HGH', 'XMN', 'NKG', 'WUH', 'CSX', 'URC', 'TAO', 'CGO', 'SYX', 'HAK', 'TSN',
                      'DLC', 'HRB', 'KWE', 'SHE', 'FOC', 'NNG']

        w_ip = ip
        w_starttime = datetime.datetime.strftime(data['time'][0], '%Y-%m-%d %H:%M:%S')
        w_endtime = datetime.datetime.strftime(data['time'][-1], '%Y-%m-%d %H:%M:%S')
        tempt1 = time.strptime(w_starttime, '%Y-%m-%d %H:%M:%S')
        tempt2 = time.strptime(w_endtime, '%Y-%m-%d %H:%M:%S')
        w_duration = time.mktime(tempt2) - time.mktime(tempt1)
        w_query = data['count']
        w_buy = data['buy']
        w_depature = len(np.unique(data['depature']))
        w_arrival = len(np.unique(data['arrival']))
        if w_query >= 2:
            # w_variance = round(np.array([x.timestamp() for x in data['time']]).std(), 2)
            templist = []
            for i in range(w_query - 1):
                t1 = datetime.datetime.strftime(data['time'][i], '%Y-%m-%d %H:%M:%S')
                t2 = datetime.datetime.strftime(data['time'][i + 1], '%Y-%m-%d %H:%M:%S')
                t1 = time.strptime(t1, '%Y-%m-%d %H:%M:%S')
                t2 = time.strptime(t2, '%Y-%m-%d %H:%M:%S')
                t3 = time.mktime(t2) - time.mktime(t1)
                templist.append(t3)
            w_variance = float(np.array(templist).std())
            w_mean = float(np.array(templist).mean())
        else:
            w_mean = 0
            w_variance = 0

        w_error = 1.0 * data['error'] / data['count']
        w_hotcity = 0.0
        w_normalcity = 0.0
        for i in range(len(data['depature'])):
            if data['depature'][i] in class1list:
                w_hotcity += 1
            elif data['depature'][i] in class2list:
                w_normalcity += 1
        for i in range(len(data['arrival'])):
            if data['arrival'][i] in class1list:
                w_hotcity += 1
            elif data['arrival'][i] in class2list:
                w_normalcity += 1

        w_hotcity = 0.5 * w_hotcity / data['count']
        w_normalcity = 0.5 * w_normalcity / data['count']
        sql = 'INSERT INTO procdata.sessiondiv ( ip, starttime, endtime, duration, query, buy, depature, arrival, variance, mean, error,hotcity,normalcity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        self.cursor.execute(sql, (
        w_ip, w_starttime, w_endtime, w_duration, w_query, w_buy, w_depature, w_arrival, w_variance, w_mean, w_error,
        w_hotcity, w_normalcity))
        return 1

    def svmjudge(self, d1, d2):
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(d1, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta
        dateend = datetime.datetime.strptime(d2, timeformat)
        flagshoplist = ['114.80.10.1', '61.155.159.41', '103.37.138.14', '103.37.138.10']

        while (date1 < dateend):
            sql = 'SELECT ip,starttime,duration,query,depature,arrival,variance,mean,error,hotcity,normalcity FROM procdata.sessiondiv where `starttime`>= %s AND `starttime` < %s'
            self.cursor.execute(sql, (date1.strftime(timeformat), date2.strftime(timeformat)))
            tempresult = self.cursor.fetchall()
            sql1 = 'UPDATE procdata.sessiondiv SET svmclass = 0 WHERE ip = %s AND starttime = %s'
            sql2 = 'UPDATE procdata.sessiondiv SET svmclass = %s WHERE ip = %s AND starttime = %s'
            for i in range(len(tempresult)):
                if tempresult[i][0] in flagshoplist:
                    self.cursor.execute(sql1, (tempresult[i][0], tempresult[i][1].strftime(timeformat)))
                    continue
                if tempresult[i][3] <= 5:
                    self.cursor.execute(sql1, (tempresult[i][0], tempresult[i][1].strftime(timeformat)))
                    continue
                svmlist = []
                for j in range(2, 11):#数据归一化
                    svmlist.append(tempresult[i][j])
                svmlist[0] = 1.0 * svmlist[0] / 10000
                if svmlist[0] > 1:
                    svmlist[0] = 1.0
                svmlist[1] = 1.0 * svmlist[1] / 200
                if svmlist[1] > 1:
                    svmlist[1] = 1.0
                svmlist[2] = 1.0 * svmlist[2] / 50
                if svmlist[2] > 1:
                    svmlist[2] = 1.0
                svmlist[3] = 1.0 * svmlist[3] / 50
                if svmlist[3] > 1:
                    svmlist[3] = 1.0
                svmlist[4] = 1.0 * svmlist[4] / 1000
                if svmlist[4] > 1:
                    svmlist[4] = 1.0
                svmlist[5] = 1.0 * svmlist[5] / 1000
                if svmlist[5] > 1:
                    svmlist[5] = 1.0
                svmlist = np.array(svmlist).reshape(1, -1)
                result = self.svmmodel.predict_proba(svmlist)
                self.cursor.execute(sql2, (str(result[0][1]), tempresult[i][0], tempresult[i][1].strftime(timeformat)))
            print(datetime.datetime.strftime(date1, timeformat) + ' judge')
            date1 = date1 + delta
            date2 = date1 + delta
        return 1


if __name__ == '__main__':
    db = MakeSession()
    db.svmjudge('2016-08-23 00:00:00', '2016-10-24 00:00:00')