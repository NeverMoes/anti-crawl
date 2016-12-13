#!/usr/bin/python3
# -*- coding: <encoding name> -*-

import pymysql as mdb
import datetime

from utils.const import const


class Mysqldb(object):
    def __init__(self):
        try:
            self.connect = mdb.connect(**const.DBCONF)
            self.cursor = self.connect.cursor()
        except Exception as e:
            print(e)

    def setiplist(self, ip=None, type=None, time=None, istrash=None, label=None, isdel=False):
        if isdel:
            self.cursor.execute('''delete from procdata.iplist
                                   where ip = '{ip}' '''.format(ip=ip))
        else:
            self.cursor.execute('''insert procdata.iplist
                                   (ip, type, time, istrash, label)
                                   VALUES ('{ip}', '{type}', '{time}', '{istrash}', '{label}')
                                   '''.format(ip=ip, type=type, time=time.replace('_', ' '),
                                              istrash=istrash, label=label))
        return {'result': 'ok'}

    def getiplist(self):
        data = list()
        self.cursor.execute('''select * from procdata.iplist''')
        for row in self.cursor.fetchall():
            data.append({'ip': row[0], 'type': row[1],
                         'time': row[2], 'istrash': row[3], 'label': row[4]})
        return data
    # count, time

    def getcatchedcount(self, date):
        data = list()
        date = datetime.datetime.strptime(date+' 00:00:00', '%Y-%m-%d %H:%M:%S')
        interval = datetime.timedelta(days=1)
        self.cursor.execute('''
                            select * from procdata.catchedcount
                            where time > '{stime}'
                            and time < '{etime}'
        '''.format(stime=date, etime=date+interval))
        for row in self.cursor.fetchall():
            data.append({'querycount': row[1], 'time': row[0]})
        return data

    def getippiedata(self, date):
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
        return templist

    def gettenminutecount(self, date, ip):

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

        return templist

    def ipwhere(self, ip):
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
        return {'ipwhere': temp}

    def route(self, ip=None, date=None):
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
            return data

    def top(self, date, type, limit):
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
                row['iploc'] = self.ipwhere(row['ip'])['ipwhere']

        return data

