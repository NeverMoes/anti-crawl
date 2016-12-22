#!/usr/bin/python3
# -*- coding: <encoding name> -*-

import pymysql as mdb
from sklearn import svm
import datetime
import random
import os
import time
from sklearn.externals import joblib
import shutil

from utils.const import const



class Mysqldb(object):
    def __init__(self):
        pass

    def __del__(self):
        self.cursor.close()
        self.connect.close()

    def get_connetc(self):
        try:
            self.connect = mdb.connect(**const.DBCONF)
            self.cursor = self.connect.cursor()
        except Exception as e:
            print(e)
        return

    def close_connenct(self):
        self.cursor.close()
        self.connect.close()
        return

    ################################################

    def deletesvm(self, name):
        self.get_connetc()

        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        svmpathdir = os.path.join(BASE_DIR, 'untils', 'SVMmodel', name)
        shutil.rmtree(svmpathdir)
        sql = 'delete FROM procdata.svmmessage where name = %s'
        self.cursor.execute(sql, (name))

        self.close_connenct()
        return

    def showsvmmessage(self):
        self.get_connetc()

        sql = 'SELECT * FROM procdata.svmmessage'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        returnlist = []
        for i in range(len(result)):
            tempdict = {'name': result[i][0], 'time': datetime.datetime.strftime(result[i][1], '%Y-%m-%d %H:%M:%S'),
                        'message': result[i][2], 'accuracy': result[i][3]}
            returnlist.append(tempdict)

        self.close_connenct()
        return returnlist

    def makesvm(self, name, datea, dateb, properties, datas):

        self.get_connetc()

        delta = datetime.timedelta(days=1)
        timeformat = '%Y-%m-%d'
        tempdate = datetime.datetime.strptime(dateb, timeformat)
        tempdate += delta
        dateb = tempdate.strftime(timeformat)
        propertylist = properties.split('@')
        propertystr = ''

        trainx = []
        trainy = []
        testx = []
        class1index = 0
        class2index = 0
        '''  临时用条件搜索设定的训练样本处理
        indata = datas.split('')
        tempclassindex = len(indata[0]-1)
        for i in range(len(indata)):
            trainx.append()
            trainy.append()
            if indata[i][tempclassindex] == 1:
                class1index += 1
            else:
                class2index +=1
        '''

        for i in range(len(propertylist)):
            if i != len(propertylist) - 1:
                propertystr += propertylist[i] + ','
            else:
                propertystr += propertylist[i]

        for i in range(len(datas)):
            traintempliat = []
            for j in range(len(propertylist)):
                traintempliat.append(datas[i][propertylist[j]])
            if datas[i]['class'] == 1:
                class1index += 1
                trainy.append(1)
            else:
                class2index += 1
                trainy.append(2)
            trainx.append(traintempliat)

        self.cursor.execute('''SELECT {properties}
    FROM procdata.trainsessiondiv
    WHERE class = 1 AND `starttime` BETWEEN '{time1}' AND '{time2}'
    '''.format(properties=propertystr, time1=datea, time2=dateb))
        result = self.cursor.fetchall()
        testall = []
        propertycount = len(propertylist)
        for i in range(len(result)):
            templist = []
            for j in range(propertycount):
                templist.append(result[i][j])
            testall.append(templist)
        for i in range(len(testall)):
            trainx.append(testall[i])
            trainy.append(1)
        class1index += len(testall)
        testall = []
        self.cursor.execute(
            "SELECT {properties} FROM procdata.trainsessiondiv WHERE class = 2 AND `starttime` BETWEEN '{time1}' AND '{time2}'".format(
                properties=propertystr, time1=datea, time2=dateb))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            templist = []
            for j in range(propertycount):
                templist.append(result[i][j])
            testall.append(templist)
        for i in range(len(testall)):
            trainx.append(testall[i])
            trainy.append(2)
        class2index += len(testall)

        self.cursor.execute('''SELECT {properties}
    FROM procdata.sessiondiv
    WHERE class = 1 AND `starttime` BETWEEN '{time1}' AND '{time2}'
    '''.format(properties=propertystr, time1=datea, time2=dateb))
        result = self.cursor.fetchall()

        if (len(result) + class1index) < 2000:
            delta2 = datetime.timedelta(days=15)
            tempdate2 = tempdate + delta2
            dateb2 = tempdate2.strftime(timeformat)
            self.cursor.execute('''SELECT {properties}
    FROM procdata.sessiondiv
    WHERE class = 1 AND `starttime` BETWEEN '{time1}' AND '{time2}'
    '''.format(properties=propertystr, time1=datea, time2=dateb2))
            result = self.cursor.fetchall()

        testall = []
        propertycount = len(result[0])
        for i in range(len(result)):
            templist = []
            for j in range(propertycount):
                templist.append(result[i][j])
            testall.append(templist)

        random.shuffle(testall)
        for i in range(2000 - class1index):
            trainx.append(testall[i])
            trainy.append(1)
        testall = testall[(2000 - class1index):]
        for i in range(100):
            testx.append(testall[i])
        self.cursor.execute('''SELECT {properties}
    FROM procdata.sessiondiv
    WHERE class = 2 AND `starttime` BETWEEN '{time1}' AND '{time2}'
    '''.format(properties=propertystr, time1=datea, time2=dateb))
        result = self.cursor.fetchall()

        testall = []
        propertycount = len(result[0])
        for i in range(len(result)):
            templist = []
            for j in range(propertycount):
                templist.append(result[i][j])
            testall.append(templist)

        random.shuffle(testall)
        for i in range(2000 - class2index):
            trainx.append(testall[i])
            trainy.append(2)
        testall = testall[(2000 - class1index):]
        for i in range(100):
            testx.append(testall[i])

        model = svm.SVC(C=0.98, kernel='linear', probability=True)
        model.fit(trainx, trainy)

        accuracy = 0
        testresult = model.predict(testx[:100])
        for i in range(100):
            if testresult[i] == 1:
                accuracy += 1
        testresult = model.predict(testx[100:])
        for i in range(100):
            if testresult[i] == 2:
                accuracy += 1
        accuracy = 1.0 * accuracy / 200

        # 项目目录
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        svmpathdir = os.path.join(BASE_DIR, 'untils', 'SVMmodel', name)
        svmpath = os.path.join(svmpathdir, 'svmmodel.pkl')

        sql = 'INSERT INTO procdata.svmmessage ( name, createtime, includekind, accuracy) VALUES (%s, %s, %s, %s)'

        self.cursor.execute(sql, (
        name, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), properties.replace("&", ","), accuracy))

        os.makedirs(svmpathdir)
        joblib.dump(model, svmpath)

        self.close_connenct()

        return

    def selecttrainsession(self, datea, dateb, properties, details):

        self.get_connetc()

        propertylist = properties.split('@')
        selectstr = ''
        wherestr = ''
        alllist = details.split('@')
        for i in range(len(alllist)):
            tempstr = alllist[i]
            templist = tempstr.split('$')
            if templist[1] == templist[2]:
                wherestr += " AND " + templist[0] + " >= " + templist[1]
            else:
                wherestr += " AND " + templist[0] + " >= " + templist[1] + " AND " + templist[0] + " < " + templist[2]

        for i in range(len(propertylist)):
            if i != len(propertylist) - 1:
                selectstr += propertylist[i] + ','
            else:
                selectstr += propertylist[i]
        # --------------------------------------select与where语句完成--------------------

        delta = datetime.timedelta(days=1)
        temptime2 = datetime.datetime.strptime(dateb, '%Y-%m-%d')
        temptime2 += delta
        dateb = datetime.datetime.strftime(temptime2, '%Y-%m-%d')

        self.cursor.execute('''SELECT ip,starttime,{properties}
    FROM procdata.sessiondiv
    WHERE `starttime` >= '{time1}' AND `starttime` < '{time2}'{where2}
    '''.format(properties=selectstr, time1=datea, time2=dateb, where2=wherestr))
        result = self.cursor.fetchall()

        returnlist = []
        for i in range(len(result)):
            tempdict = {}
            tempdict['ip'] = result[i][0]
            tempdict['starttime'] = datetime.datetime.strftime(result[i][1], '%Y-%m-%d %H:%M:%S')
            for j in range(len(propertylist)):
                tempdict[propertylist[j]] = result[i][j + 2]

            returnlist.append(tempdict)

        self.close_connenct()

        return returnlist

    def addtrainsession(self, inip, instarttime, inclass):

        instarttime = instarttime.replace('_', ' ')

        self.get_connetc()

        sql = 'SELECT * FROM procdata.sessiondiv WHERE ip = %s and starttime = %s'
        self.cursor.execute(sql, (inip, instarttime))
        result = self.cursor.fetchall()

        if len(result[0]) > 0:
            sql = 'INSERT INTO procdata.trainsessiondiv ( ip, starttime, endtime, duration, query, buy, depature, arrival, variance, mean, error, class) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            self.cursor.execute(sql, (
            result[0][0], result[0][1], result[0][2], result[0][3], result[0][4], result[0][5], result[0][6],
            result[0][7], result[0][8], result[0][9], result[0][10], inclass))

        self.close_connenct()
        return

    def getsessiontabledate(self, startdate, enddate):

        self.get_connetc()

        kindlist = ['duration', 'query', 'depature', 'arrival', 'variance', 'mean', 'error']
        connection = self.connect
        cursor = self.cursor

        timeformat = '%Y-%m-%d'
        date1 = datetime.datetime.strptime(startdate, timeformat)
        date2 = datetime.datetime.strptime(enddate, timeformat)
        delta = datetime.timedelta(days=1)

        # sql = 'SELECT * FROM %s WHERE date = %s'

        returnlist = []
        allclass1 = 0
        allclass2 = 0
        for i in range(7):
            tempdict = {}
            col = kindlist[i]
            date1 = datetime.datetime.strptime(startdate, timeformat)
            while date1 <= date2:
                cursor.execute('''SELECT *
        FROM procdata.allsessiontable
        WHERE `kinds` = '{kind}' AND `date` = '{time}'
        '''.format(kind=col, time=datetime.datetime.strftime(date1, timeformat)))
                result = cursor.fetchall()

                for i in range(len(result)):
                    allclass2 += result[i][3]
                    allclass1 += result[i][4]
                    if result[i][2] in tempdict:
                        tempdict[result[i][2]]['class2'] += result[i][3]
                        tempdict[result[i][2]]['class1'] += result[i][4]
                    else:
                        tempdict[result[i][2]] = {'class2': result[i][3], 'class1': result[i][4]}
                date1 += delta
            showlist = []
            # tempdict = sorted(tempdict,key=lambda x:(x['range']))
            # tempdict['all'] = {'class1':allclass1,'class2':allclass2}
            # showlist.append({'range':'all','class2':allclass2,'class1':allclass1})
            for key in tempdict:
                showlist.append({'range': key, 'class2': tempdict[key]['class2'], 'class1': tempdict[key]['class1']})
            showlist = sorted(showlist, key=lambda x: (x['range']))
            showlist.append({'range': 'all', 'class2': allclass2, 'class1': allclass1})
            temp2dict = {'kinds': col, 'data': showlist}
            returnlist.append(temp2dict)

        self.close_connenct()
        return returnlist

    def setiplist(self, ip=None, type=None, time=None, istrash=None, label=None, isdel=False):

        self.get_connetc()

        if isdel:
            self.cursor.execute('''delete from procdata.iplist
                                   where ip = '{ip}' '''.format(ip=ip))
        else:
            self.cursor.execute('''insert procdata.iplist
                                   (ip, type, time, istrash, label)
                                   VALUES ('{ip}', '{type}', '{time}', '{istrash}', '{label}')
                                   '''.format(ip=ip, type=type, time=time.replace('_', ' '),
                                              istrash=istrash, label=label))

        self.close_connenct()
        return {'result': 'ok'}

    def getiplist(self):
        self.get_connetc()
        data = list()
        self.cursor.execute('''select * from procdata.iplist''')
        for row in self.cursor.fetchall():
            data.append({'ip': row[0], 'type': row[1],
                         'time': row[2], 'istrash': row[3], 'label': row[4]})
        self.close_connenct()
        return data
    # count, time

    def getcatchedcount(self, date):
        self.get_connetc()
        data = list()
        date = datetime.datetime.strptime(date+' 00:00:00', '%Y-%m-%d %H:%M:%S')
        interval = datetime.timedelta(days=1)
        self.cursor.execute('''
                            select * from procdata.catchedcount
                            where time >= '{stime}'
                            and time < '{etime}'
        '''.format(stime=date, etime=date+interval))
        for row in self.cursor.fetchall():
            data.append({'querycount': row[1], 'time': row[0]})
        self.close_connenct()
        return data

    def getippiedata(self, date):
        self.get_connetc()

        sql = "SELECT * FROM procdata.piedata WHERE time = %s"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()

        templist = []
        tempdict = {}
        tempdict['0-10'] = result[0][1]
        tempdict['10-30'] = result[0][2]
        tempdict['30-50'] = result[0][3]
        tempdict['50-100'] = result[0][4]
        tempdict['over100'] = result[0][5]
        tempdict['all'] = result[0][6]

        templist.append(tempdict)
        self.close_connenct()
        return templist

    def gettenminutecount(self, date, ip):

        self.get_connetc()

        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = date + ' 00:00:00'
        tempdate1 = datetime.datetime.strptime(date1, timeformat)
        delta = datetime.timedelta(minutes=10)
        deltaend = datetime.timedelta(days=1)
        tempdate = tempdate1
        tempdate2 = tempdate1 + deltaend

        sql = 'SELECT time,query,buy FROM {table} WHERE ip = %s AND time BETWEEN %s AND %s ORDER BY time'.format(table=const.APITENMINUTECOUNT)
        self.cursor.execute(sql, (ip, tempdate1.strftime(timeformat), tempdate2.strftime(timeformat)))
        result = self.cursor.fetchall()

        templist = []
        i = 0
        while tempdate < tempdate2:
            # temptime = datetime.datetime.strptime(result[i][0],timeformat)
            temptime = result[i][0]
            tempdict = {}
            if tempdate < temptime:
                # templist.append([tempdate.strftime(timeformat),0,0])
                tempdict['datetime'] = tempdate.strftime(timeformat)
                tempdict['query'] = 0
                tempdict['buy'] = 0
            elif tempdate == temptime:
                # templist.append([tempdate.strftime(timeformat),result[i][1],result[i][2]])
                tempdict['datetime'] = tempdate.strftime(timeformat)
                tempdict['query'] = result[i][1]
                tempdict['buy'] = result[i][2]
                if i + 1 < len(result):
                    i += 1
            else:
                # templist.append([tempdate.strftime(timeformat),0,0])
                tempdict['datetime'] = tempdate.strftime(timeformat)
                tempdict['query'] = 0
                tempdict['buy'] = 0
            templist.append(tempdict)
            tempdate += delta

        self.close_connenct()
        return templist

    def ipwhere(self, ip, isref=False):

        if isref:
            tempip = ip.split('.')
            ipnum = 0
            ipnum += int(tempip[3]) + int(tempip[2]) * 256 + int(tempip[1]) * 256 * 256 + int(
                tempip[0]) * 256 * 256 * 256
            sql = '''SELECT Country, Local
                  FROM {tbname}
                  WHERE StartIPNum <= {ipnum}
                  AND EndIPNum+1 > {ipnum}'''.format(tbname=const.IPWHERE, ipnum=ipnum)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            temp = result[0][0] + result[0][1]
            return {'ipwhere': temp}

        else:
            self.get_connetc()
            tempip = ip.split('.')
            ipnum = 0
            ipnum += int(tempip[3]) + int(tempip[2]) * 256 + int(tempip[1]) * 256 * 256 + int(tempip[0]) * 256 * 256 * 256
            sql = '''SELECT Country, Local
                  FROM {tbname}
                  WHERE StartIPNum <= {ipnum}
                  AND EndIPNum+1 > {ipnum}'''.format(tbname=const.IPWHERE, ipnum=ipnum)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            temp = result[0][0] + result[0][1]
            self.close_connenct()
            return {'ipwhere': temp}

    def route(self, ip=None, date=None):

        self.get_connetc()

        data = list()
        if not ip:
            self.cursor.execute('''
            SELECT  route, query, buy, ip
            from {table}
            WHERE time = '{date}'
            order by query desc
            '''.format(table=const.APIROUTE, date=date, ip=ip))
            for row in self.cursor.fetchall():
                data.append({'route': row[0], 'querycount': row[1],
                             'ordercount': row[2], 'ip': row[3]})
            return data
        else:
            self.cursor.execute('''
            SELECT  route, query, buy
            from {table}
            WHERE time = '{date}'
            and ip = '{ip}'
            order by query desc
            '''.format(table=const.APIROUTE, date=date, ip=ip))
            for row in self.cursor.fetchall():
                data.append({'route': row[0], 'querycount': row[1], 'ordercount': row[2]})

            self.close_connenct()
            return data

    def top(self, date, type, limit):

        self.get_connetc()

        data = list()
        if type == 'query':
            self.cursor.execute('''
            SELECT ip, querycount, ordercount
            from {table}
            where querytime = '{date}'
            order by querycount desc
            limit {limit}
            '''.format(table=const.APITOP, date=date, limit=limit))
            for index, row in enumerate(self.cursor.fetchall()):
                data.append({'ip': row[0], 'querycount': row[1], 'ordercount': row[2]})

        elif type == 'order':
            self.cursor.execute('''
            SELECT ip, querycount, ordercount
            from {table}
            where querytime = '{date}'
            order by ordercount desc
            limit {limit}
            '''.format(table=const.APITOP, date=date, limit=limit))
            for index, row in enumerate(self.cursor.fetchall()):
                data.append({'ip': row[0], 'querycount': row[1], 'ordercount': row[2]})

        elif type == 'onlyquery':
            self.cursor.execute('''
            SELECT ip, querycount, ordercount
            from {table}
            where querytime = '{date}'
            and ordercount = 0
            order by querycount desc
            limit {limit}
            '''.format(table=const.APITOP, date=date, limit=limit))
            for index, row in enumerate(self.cursor.fetchall()):
                data.append({'ip': row[0], 'querycount': row[1], 'ordercount': row[2]})

        if data:
            for row in data:
                row['iploc'] = self.ipwhere(row['ip'], isref=True)['ipwhere']

        self.close_connenct()

        return data

