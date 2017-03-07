# -*- coding: utf-8 -*-
"""
Created on Mon Feb 06 14:12:21 2017

@author: mao133132

"""

import pymysql
import datetime
import numpy as np
from utils.consts import const
from .dealsessionhistory import MakeSessionTable
import time
import math


class MakeTable(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()
        self.ffdata = []  # 查询数据
        self.ssdata = []  # 订票数据
        self.tenminuteindex = [0]  # 每十分钟的查询数据index
        self.tenminuteindex_buy = [0]  # 每十分钟的订票数据index
        self.hourindex = [0]  # 每小时的查询数据index
        self.hourbuyindex_buy = [0]  # 每小时的订票数据index
        self.crawlerip = []
        self.class1list = ['PEK', 'PVG', 'CAN', 'CTU', 'SZX', 'SHA']
        self.class2list = ['KMG', 'XIY', 'CKG', 'HGH', 'XMN', 'NKG', 'WUH', 'CSX', 'URC', 'TAO', 'CGO', 'SYX', 'HAK',
                           'TSN', 'DLC', 'HRB', 'KWE', 'SHE', 'FOC', 'NNG']
        self.flagshoplist = ['114.80.10.1', '61.155.159.41', '103.37.138.14', '103.37.138.10']

    def maindeal(self, datea):
        self.ffdata = []  # 查询数据
        self.ssdata = []  # 订票数据
        self.tenminuteindex = [0]  # 每十分钟的查询数据index
        self.tenminuteindex_buy = [0]  # 每十分钟的订票数据index
        self.hourindex = [0]  # 每小时的查询数据index
        self.hourbuyindex_buy = [0]  # 每小时的订票数据index
        self.crawlerip = []

        print(datea + 'begin!')
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        self.getdata(datea)
        '''
        self.Day_Count(datea)
        self.MakeThreeIp(datea)
        self.MakeTwoIp(datea)
        self.TenMinuteCount(datea)
        self.makecrawlerip(datea)
        self.MakeIpPieData(datea)
        self.route_count_daily(datea)
        '''
        self.makeipmessage(datea)
        '''
        self.MakePositiveSample(datea)
        self.MakeNegativeSample(datea)
        self.forfaster(datea)
        sessiontable = MakeSessionTable()  # 制作session属性分布表，用于第三，第四页面。
        sessiontable.maindeal(datea)
        sessiontable = 0
        '''
        print('all over!')
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

        return 0

    def getdata(self, a):  # 先把数据取出来，便于提高效率
        self.ffdata = []
        self.tenminuteindex = [0]
        self.hourindex = [0]
        tempdate = a + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(tempdate, timeformat)
        delta = datetime.timedelta(days=1)
        tenminutedelta = datetime.timedelta(minutes=10)
        date2 = date1 + tenminutedelta
        dateend = date1 + delta
        tempflag = 1
        querycount = 0
        buycount = 0
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
                    querycount -= 1
                    continue
                self.ffdata.append(result[i])
            querycount += len(result)
            self.tenminuteindex.append(querycount)

            self.cursor.execute('''
            SELECT {ip},{ssmessage}
            from {table}
            WHERE {querytime} >= '{date}'
            AND {querytime} < '{date2}'
            '''.format(ssmessage=const.TABLE2_ROUTE, ip=const.TABLE_IP, table=const.RAWSS,
                       querytime=const.TABLE_QUERYTIME, date=date1.strftime(timeformat),
                       date2=date2.strftime(timeformat)))
            result = self.cursor.fetchall()
            for i in range(len(result)):
                if result[i][0] == None:
                    buycount -= 1
                    continue
                de = result[i][1][9:12]
                ar = result[i][1][13:16]
                self.ssdata.append([result[i][0], de, ar])
            buycount += len(result)
            self.tenminuteindex_buy.append(buycount)

            if tempflag == 6:
                tempflag = 0
                self.hourindex.append(querycount)
                self.hourbuyindex_buy.append(buycount)
            tempflag += 1
            date1 = date1 + tenminutedelta
            date2 = date1 + tenminutedelta
        return 0

    def Day_Count(self, a):  # 制作ft_day表，即一天四位ip查询订票数据统计表
        ipdict = {}
        for i in range(len(self.ffdata)):
            if self.ffdata[i][0] in ipdict:
                ipdict[self.ffdata[i][0]] += 1
            else:
                ipdict[self.ffdata[i][0]] = 1
        sql = 'INSERT INTO procdata.ft_day (date, ip, querycount) VALUES (%s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]))
        '''
        到这里为止，查询统计完成
        '''
        buydict = {}
        for i in range(len(self.ssdata)):
            if self.ssdata[i][0] in buydict:
                buydict[self.ssdata[i][0]] += 1
            else:
                buydict[self.ssdata[i][0]] = 1

        sql = "UPDATE procdata.ft_day SET ordercount = %s WHERE `date` = %s AND `ip` = %s"
        for i in buydict.keys():
            self.cursor.execute(sql, (buydict[i], a, i))
        return 0

    # 制作ip头三位的块表ft_three_day，三位ip查询订票数据统计表
    def MakeThreeIp(self, a):

        sql = 'SELECT * FROM procdata.ft_day where date = %s'
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()

        ipdict = {}
        for i in range(len(result)):
            tempip = result[i][1]
            templist = tempip.split('.')
            tempip = templist[0] + '.' + templist[1] + '.' + templist[2]
            if tempip in ipdict:
                ipdict[tempip]['querycount'] += result[i][2]
                ipdict[tempip]['ordercount'] += result[i][3]
            else:
                ipdict[tempip] = {'querycount': result[i][2], 'ordercount': result[i][3]}
        sql = 'INSERT INTO procdata.ft_three_day (date, ip, querycount, ordercount) VALUES (%s, %s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]['querycount'], ipdict[i]['ordercount']))
        return 0

    # 制作ip头二位的块表ft_two_day, 二位ip查询订票数据统计表
    def MakeTwoIp(self, a):
        sql = 'SELECT * FROM procdata.ft_three_day where `date` = %s'
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()

        ipdict = {}
        for i in range(len(result)):
            tempip = result[i][1]
            templist = tempip.split('.')
            tempip = templist[0] + '.' + templist[1]
            if tempip in ipdict:
                ipdict[tempip]['querycount'] += result[i][2]
                ipdict[tempip]['ordercount'] += result[i][3]
            else:
                ipdict[tempip] = {'querycount': result[i][2], 'ordercount': result[i][3]}
        sql = 'INSERT INTO procdata.ft_two_day (date, ip, querycount, ordercount) VALUES (%s, %s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]['querycount'], ipdict[i]['ordercount']))
        return 0

    # 获取头两位ip有问题的ip头（查询量大而不订票），不是建表函数，在session的正负样本自动选取中有用。
    def GetTopTwoIp(self, date, num):
        sql = 'SELECT ip FROM procdata.ft_two_day where `date` = %s AND ordercount = 0 order by querycount desc limit 0,%s'
        self.cursor.execute(sql, (date, num))
        result = self.cursor.fetchall()

        templist = []
        for i in range(len(result)):
            templist.append(result[i][0])
        return templist

    # 制作ip航线的快表ip_route_count，记载一天所有以及特定ip的航线查询订票情况
    def route_count_daily(self, a):
        class1list = self.class1list
        class2list = self.class2list
        topiplist = []
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND ordercount = 0 ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY ordercount desc LIMIT 0 , 20"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        topiplist = np.unique(topiplist)  # 四位ip的list

        threetoplist = []  # 三位ip的list
        sql = "SELECT ip FROM procdata.ft_three_day WHERE `date` = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            threetoplist.append(result[i][0])
        twotoplist = []  # 二位ip的list
        sql = "SELECT ip FROM procdata.ft_two_day WHERE `date` = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            twotoplist.append(result[i][0])

        # 到这里为止保存了top50的ip
        routedict = {}
        for i in range(len(self.ffdata)):
            alltemp = self.ffdata[i][2] + self.ffdata[i][3] + 'all'
            if alltemp in routedict:
                routedict[alltemp]['query'] += 1
            else:
                routedict[alltemp] = {'query': 1, 'cquery': 0}
            tempiplist = self.ffdata[i][0].split('.')
            threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
            twoip = tempiplist[0] + '.' + tempiplist[1]
            if self.ffdata[i][0] in self.crawlerip:
                routedict[alltemp]['cquery'] += 1
                if self.ffdata[i][0] in topiplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + self.ffdata[i][0]
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                        routedict[temp]['cquery'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 1}
                if threeip in threetoplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + threeip
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                        routedict[temp]['cquery'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 1}
                if twoip in twotoplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + twoip
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                        routedict[temp]['cquery'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 1}
            else:
                if self.ffdata[i][0] in topiplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + self.ffdata[i][0]
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 0}
                if threeip in threetoplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + threeip
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 0}
                if twoip in twotoplist:
                    temp = self.ffdata[i][2] + self.ffdata[i][3] + twoip
                    if temp in routedict:
                        routedict[temp]['query'] += 1
                    else:
                        routedict[temp] = {'query': 1, 'cquery': 0}

        sql = 'INSERT INTO procdata.ip_route_count (time, ip, route, query,crawlerquery,kinds) VALUES (%s, %s, %s, %s, %s, %s)'
        for i in routedict.keys():#航线分类
            tempde = i[0:3]
            tempar = i[3:6]
            if tempde in class1list:
                if tempar in class1list:
                    routeclass = 1
                elif tempar in class2list:
                    routeclass = 2
                else:
                    routeclass = 4
            elif tempar in class2list:
                if tempar in class1list:
                    routeclass = 2
                elif tempar in class2list:
                    routeclass = 3
                else:
                    routeclass = 5
            else:
                if tempar in class1list:
                    routeclass = 4
                else:
                    routeclass = 5
            self.cursor.execute(sql, (a, i[6:], i[0:6], routedict[i]['query'], routedict[i]['cquery'], routeclass))
        # 到这里为止统计了航线的查询数量
        buydict = {}
        for i in range(len(self.ssdata)):
            alltemp = self.ssdata[i][1] + self.ssdata[i][2] + 'all'
            if alltemp in buydict:
                buydict[alltemp] += 1
            else:
                buydict[alltemp] = 1
            tempiplist = self.ssdata[i][0].split('.')
            threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
            twoip = tempiplist[0] + '.' + tempiplist[1]
            if self.ssdata[i][0] in topiplist:
                temp = self.ssdata[i][1] + self.ssdata[i][2] + self.ssdata[i][0]
                if temp in buydict:
                    buydict[temp] += 1
                else:
                    buydict[temp] = 1
            if threeip in threetoplist:
                temp = self.ssdata[i][1] + self.ssdata[i][2] + threeip
                if temp in buydict:
                    buydict[temp] += 1
                else:
                    buydict[temp] = 1
            if twoip in twotoplist:
                temp = self.ssdata[i][1] + self.ssdata[i][2] + twoip
                if temp in buydict:
                    buydict[temp] += 1
                else:
                    buydict[temp] = 1
        for i in buydict.keys():
            sql = "UPDATE procdata.ip_route_count SET buy = %s WHERE `time` = %s AND ip = %s AND route = %s"
            self.cursor.execute(sql, (buydict[i], a, i[6:], i[0:6]))
        return 0

    # 制作第一张饼图数据与第二张饼图数据
    def MakeIpPieData(self, a):
        alllist = []
        for i in range(5):
            templist = []
            for j in range(5):
                templist.append({'all': [], 'crawler': [], 'buy': []})
            alllist.append(templist)
        rangelist = [[], [], [], [], []]
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND querycount > 0 AND querycount < 10"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            rangelist[0].append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND querycount >= 10 AND querycount < 30"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            rangelist[1].append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND querycount >= 30 AND querycount < 50"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            rangelist[2].append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND querycount >= 50 AND querycount < 100"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            rangelist[3].append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND querycount >= 100"
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            rangelist[4].append(result[i][0])

        for i in range(len(self.ffdata)):
            de = self.ffdata[i][2]
            ar = self.ffdata[i][3]
            if de in self.class1list:
                if ar in self.class1list:
                    routeclass = 1
                elif ar in self.class2list:
                    routeclass = 2
                else:
                    routeclass = 4
            elif de in self.class2list:
                if ar in self.class1list:
                    routeclass = 2
                elif ar in self.class2list:
                    routeclass = 3
                else:
                    routeclass = 5
            else:
                if ar in self.class1list:
                    routeclass = 4
                else:
                    routeclass = 5
            rangeclass = 0
            for j in range(5):
                if self.ffdata[i][0] in rangelist[j]:
                    rangeclass = j
                    break
            routeclass = routeclass - 1
            if self.ffdata[i][0] in self.crawlerip:
                alllist[routeclass][rangeclass]['all'].append(self.ffdata[i][0])
                alllist[routeclass][rangeclass]['crawler'].append(self.ffdata[i][0])
            else:
                alllist[routeclass][rangeclass]['all'].append(self.ffdata[i][0])

        for i in range(len(self.ssdata)):
            de = self.ssdata[i][1]
            ar = self.ssdata[i][2]
            if de in self.class1list:
                if ar in self.class1list:
                    routeclass = 1
                elif ar in self.class2list:
                    routeclass = 2
                else:
                    routeclass = 4
            elif de in self.class2list:
                if ar in self.class1list:
                    routeclass = 2
                elif ar in self.class2list:
                    routeclass = 3
                else:
                    routeclass = 5
            else:
                if ar in self.class1list:
                    routeclass = 4
                else:
                    routeclass = 5
            rangeclass = 0
            for j in range(5):
                if self.ssdata[i][0] in rangelist[j]:
                    rangeclass = j
                    break
            routeclass = routeclass - 1
            alllist[routeclass][rangeclass]['buy'].append(self.ssdata[i][0])

        rangenamelist = ['0_9', '10_29', '30_49', '50_99', '100_']
        for i in range(5):
            for j in range(5):
                sql1 = 'INSERT INTO procdata.piedata (`time`, `range`, `queryip`, `orderip`, `crawlerip`, `kinds`) VALUES (%s, %s, %s,%s, %s, %s)'
                sql2 = 'INSERT INTO procdata.piedata2 (`time`, `range`, `querycount`, `ordercount`, `crawlercount`, `kinds`) VALUES (%s, %s, %s,%s, %s, %s)'
                self.cursor.execute(sql1, (
                a, rangenamelist[j], len(np.unique(alllist[i][j]['all'])), len(np.unique(alllist[i][j]['buy'])),
                len(np.unique(alllist[i][j]['crawler'])), i + 1))
                self.cursor.execute(sql2, (a, rangenamelist[j], len(alllist[i][j]['all']), len(alllist[i][j]['buy']),
                                           len(alllist[i][j]['crawler']), i + 1))
        for i in range(5):
            querylist = []
            orderlist = []
            crawlerlist = []
            for j in range(5):
                for k in range(len(alllist[j][i]['all'])):
                    querylist.append(alllist[j][i]['all'][k])
                for k in range(len(alllist[j][i]['buy'])):
                    orderlist.append(alllist[j][i]['buy'][k])
                for k in range(len(alllist[j][i]['crawler'])):
                    crawlerlist.append(alllist[j][i]['crawler'][k])
            self.cursor.execute(sql1, (
            a, rangenamelist[i], len(np.unique(querylist)), len(np.unique(orderlist)), len(np.unique(crawlerlist)), 6))
        alllist = []
        return 0

    def makeipmessage(self, dataa):  # 处理部分ip的信息表，用于第四个页面，日数据于小时数据都在这了，小时只统计查询，订票
        class1list = self.class1list
        class2list = self.class2list
        temp_date = dataa + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(temp_date, timeformat)
        delta = datetime.timedelta(days=1)
        delta2 = datetime.timedelta(days=5)
        sessiondelta = datetime.timedelta(hours=1)
        dateend = date1 + delta
        datab = date1 - delta2
        datab = datetime.datetime.strftime(datab, timeformat)
        all_delist = []
        all_arlist = []
        all_er = 0
        all_hot = 0
        all_normal = 0
        iplist = []  # 存放需要处理的四位ip
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 300"
        self.cursor.execute(sql, (dataa))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        sql = "SELECT distinct ip FROM procdata.ft_day WHERE date < %s AND date >= %s ORDER BY querycount desc LIMIT 0 , 200"
        self.cursor.execute(sql, (dataa,datab))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_three_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 100"
        self.cursor.execute(sql, (dataa))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_three_day WHERE date < %s AND date >= %s ORDER BY querycount desc LIMIT 0 , 70"
        self.cursor.execute(sql, (dataa,datab))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_two_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (dataa))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_two_day WHERE date < %s AND date >= %s ORDER BY querycount desc LIMIT 0 , 20"
        self.cursor.execute(sql, (dataa,datab))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            iplist.append(result[i][0])
        iplist = np.unique(iplist)
        # iplist = ['114.80.10.1','61.155.159.41','103.37.138.14','103.37.138.10']
        cache = {}
        hourcache = {}
        all_buy = 0
        index = 0
        while date1 < dateend:
            hourcache = {}
            for i in range(self.hourindex[index], self.hourindex[index + 1]):
                all_delist.append(self.ffdata[i][2])
                all_arlist.append(self.ffdata[i][3])
                if self.ffdata[i][2] in class1list:
                    all_hot += 1
                elif self.ffdata[i][2] in class2list:
                    all_normal += 1
                if self.ffdata[i][3] in class1list:
                    all_hot += 1
                elif self.ffdata[i][3] in class2list:
                    all_normal += 1
                if self.ffdata[i][4] != 'ok.':
                    all_er += 1
                if self.ffdata[i][0] == None:
                    continue
                tempiplist = self.ffdata[i][0].split('.')
                threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
                twoip = tempiplist[0] + '.' + tempiplist[1]
                if self.ffdata[i][0] in iplist:
                    if self.ffdata[i][0] in cache:
                        cache[self.ffdata[i][0]]['data'].append(self.ffdata[i])
                    else:
                        cache[self.ffdata[i][0]] = {'data': [self.ffdata[i]]}
                    if self.ffdata[i][0] in hourcache:
                        hourcache[self.ffdata[i][0]]['data'] += 1
                    else:
                        hourcache[self.ffdata[i][0]] = {'data': 1}
                if threeip in iplist:
                    if threeip in cache:
                        cache[threeip]['data'].append(self.ffdata[i])
                    else:
                        cache[threeip] = {'data': [self.ffdata[i]]}
                    if threeip in hourcache:
                        hourcache[threeip]['data'] += 1
                    else:
                        hourcache[threeip] = {'data': 1}
                if twoip in iplist:
                    if twoip in cache:
                        cache[twoip]['data'].append(self.ffdata[i])
                    else:
                        cache[twoip] = {'data': [self.ffdata[i]]}
                    if twoip in hourcache:
                        hourcache[twoip]['data'] += 1
                    else:
                        hourcache[twoip] = {'data': 1}
            for i in range(len(iplist)):
                if iplist[i] not in hourcache:
                    sql = 'INSERT INTO procdata.iphourmessage ( ip,time, query,buy) VALUES ( %s, %s, %s,%s)'
                    self.cursor.execute(sql, (iplist[i], date1.strftime(timeformat), 0, 0))
                    continue
                hw_count = hourcache[iplist[i]]['data']
                hw_buy = 0   #alliplist[i] 的订票量
                for k in range(self.hourbuyindex_buy[index], self.hourbuyindex_buy[index + 1]):
                    if self.ssdata[k][0] == iplist[i]:
                        hw_buy += 1
                        continue
                    tempiplist = self.ssdata[k][0].split('.')
                    threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
                    twoip = tempiplist[0] + '.' + tempiplist[1]
                    if threeip == iplist[i]:
                        hw_buy += 1
                        continue
                    if twoip == iplist[i]:
                        hw_buy += 1
                sql = 'INSERT INTO procdata.iphourmessage ( ip,time, query,buy) VALUES ( %s, %s, %s,%s)'
                self.cursor.execute(sql, (iplist[i], date1.strftime(timeformat), hw_count, hw_buy))
            sql = 'INSERT INTO procdata.iphourmessage ( ip,time, query,buy) VALUES ( %s, %s, %s,%s)'
            self.cursor.execute(sql, (
            'all', date1.strftime(timeformat), self.hourindex[index + 1] - self.hourindex[index],
            self.hourbuyindex_buy[index + 1] - self.hourbuyindex_buy[index]))
            hourcache = {}
            date1 = date1 + sessiondelta
            index += 1
        for i in range(len(iplist)):
            if iplist[i] not in cache:
                sql = 'INSERT INTO procdata.ipdaymessage ( ip,time, duration, query, buy, depature, arrival, variance, mean, error,hotcity,normalcity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)'
                self.cursor.execute(sql, (iplist[i], dataa, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
                continue
            thisiptemplist = cache[iplist[i]]['data']
            delist = []
            arlist = []
            w_hotcity = 0
            w_normalcity = 0
            for j in range(len(thisiptemplist)):
                delist.append(thisiptemplist[j][2])
                arlist.append(thisiptemplist[j][3])
                if thisiptemplist[j][2] in class1list:
                    w_hotcity += 1
                elif thisiptemplist[j][2] in class2list:
                    w_normalcity += 1
                if thisiptemplist[j][3] in class1list:
                    w_hotcity += 1
                elif thisiptemplist[j][3] in class2list:
                    w_normalcity += 1
            w_de = len(np.unique(delist))
            w_ar = len(np.unique(arlist))
            ipcache = {}
            tempdata = []  # 存放当天的session,便于计算
            for j in range(len(thisiptemplist)):
                if iplist[i] in ipcache:
                    temp1 = thisiptemplist[j][1]
                    temp2 = ipcache[iplist[i]]['time'][-1]
                    if temp1 - temp2 > sessiondelta:
                        tempdata.append(self.clear_cache(ipcache[iplist[i]]))
                        ipcache.pop(iplist[i])
                        if thisiptemplist[j][4] == 'ok.':
                            ipcache[iplist[i]] = {'count': 1, 'time': [thisiptemplist[j][1]], 'error': 0}
                        else:
                            ipcache[iplist[i]] = {'count': 1, 'time': [thisiptemplist[j][1]], 'error': 1}
                    else:
                        ipcache[iplist[i]]['count'] += 1
                        ipcache[iplist[i]]['time'].append(thisiptemplist[j][1])
                        if thisiptemplist[j][4] != 'ok.':
                            ipcache[iplist[i]]['error'] += 1
                else:
                    if thisiptemplist[j][4] == 'ok.':
                        ipcache[iplist[i]] = {'count': 1, 'time': [thisiptemplist[j][1]], 'error': 0}
                    else:
                        ipcache[iplist[i]] = {'count': 1, 'time': [thisiptemplist[j][1]], 'error': 1}
            if iplist[i] in ipcache:
                tempdata.append(self.clear_cache(ipcache[iplist[i]]))
                ipcache.pop(iplist[i])

            w_durantion = 0
            w_count = 0
            w_variance = 0.0
            w_mean = 0.0
            w_error = 0.0

            for k in range(len(tempdata)):
                w_count += tempdata[k]['count']
                w_error += tempdata[k]['error']
                w_mean += tempdata[k]['mean'] * tempdata[k]['count']
                w_durantion += tempdata[k]['duration']
                w_variance += tempdata[k]['variance'] * tempdata[k]['variance'] * tempdata[k]['count']
            if w_count > 0:
                w_error = 1.0 * w_error / w_count
                w_mean = 1.0 * w_mean / w_count
                w_variance = math.sqrt(1.0 * w_variance / w_count)
                w_hotcity = 0.5 * w_hotcity / w_count
                w_normalcity = 0.5 * w_normalcity / w_count
            else:
                w_error = 0
                w_mean = 0
                w_variance = 0
                w_hotcity = 0.0
                w_normalcity = 0.0

            iplen = len(iplist[i].split('.'))
            if iplen == 4:
                sql = 'SELECT ordercount FROM procdata.ft_day where ip = %s AND `date` = %s'
                self.cursor.execute(sql, (iplist[i], dataa))
                buyresult = self.cursor.fetchall()
            elif iplen == 3:
                sql = 'SELECT ordercount FROM procdata.ft_three_day where ip = %s AND `date` = %s'
                self.cursor.execute(sql, (iplist[i], dataa))
                buyresult = self.cursor.fetchall()
            else:
                sql = 'SELECT ordercount FROM procdata.ft_two_day where ip = %s AND `date` = %s'
                self.cursor.execute(sql, (iplist[i], dataa))
                buyresult = self.cursor.fetchall()
            if len(buyresult) > 0:
                w_buy = buyresult[0][0]
            else:
                w_buy = 0

            sql = 'INSERT INTO procdata.ipdaymessage ( ip,time, duration, query, buy, depature, arrival, variance, mean, error,hotcity,normalcity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)'
            self.cursor.execute(sql, (
            iplist[i], dataa, w_durantion, w_count, w_buy, w_de, w_ar, w_variance, w_mean, w_error, w_hotcity,
            w_normalcity))

        all_count = len(self.ffdata)
        all_buy += len(self.ssdata)
        all_de = len(np.unique(all_delist))
        all_ar = len(np.unique(all_arlist))
        if all_count > 0:
            all_er = 1.0 * all_er / all_count
            all_hot = 0.5 * all_hot / all_count
            all_normal = 0.5 * all_normal / all_count
        else:
            all_er = 0
            all_hot = 0
            all_normal = 0
        sql = 'INSERT INTO procdata.ipdaymessage ( ip,time, duration, query, buy, depature, arrival, variance, mean, error,hotcity,normalcity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)'
        if all_count > 0:
            self.cursor.execute(sql, (
            'all', dataa, 0, all_count, all_buy, all_de, all_ar, 0.0, 0.0, all_er, all_hot, all_normal))
        else:
            self.cursor.execute(sql, ('all', dataa, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
        return 0

    # 配合上一个函数使用，返回一个ip的一个session数据的统计
    def clear_cache(self, data):
        w_starttime = datetime.datetime.strftime(data['time'][0], '%Y-%m-%d %H:%M:%S')
        w_endtime = datetime.datetime.strftime(data['time'][-1], '%Y-%m-%d %H:%M:%S')
        tempt1 = time.strptime(w_starttime, '%Y-%m-%d %H:%M:%S')
        tempt2 = time.strptime(w_endtime, '%Y-%m-%d %H:%M:%S')
        w_duration = time.mktime(tempt2) - time.mktime(tempt1)
        w_query = data['count']
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
        returndict = {'count': w_query, 'duration': w_duration, 'mean': w_mean, 'variance': w_variance,
                      'error': data['error']}
        return returndict
    #返回当天svm判断为爬虫的ip
    def makecrawlerip(self, data):
        tempdate = data + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(tempdate, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta
        sql = 'SELECT ip FROM procdata.sessiondiv where `starttime`>= %s AND `starttime` < %s AND `svmclass` > 0.9'
        self.cursor.execute(sql, (date1.strftime(timeformat), date2.strftime(timeformat)))
        result = self.cursor.fetchall()
        tempiplist = []
        for i in range(len(result)):
            tempiplist.append(result[i][0])
        self.crawlerip = np.unique(tempiplist)
    #制作当天的十分钟数据快表,只统计所有以及特定ip的十分钟查询订票情况
    def TenMinuteCount(self, a):  # a的格式为 '2016-08-23'   统计每10分钟的数据.
        date = a
        topiplist = []
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND ordercount = 0 ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY ordercount desc LIMIT 0 , 20"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        topiplist = np.unique(topiplist)

        twotopiplist = []
        sql = "SELECT ip FROM procdata.ft_two_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            twotopiplist.append(result[i][0])

        threetopiplist = []
        sql = "SELECT ip FROM procdata.ft_three_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            threetopiplist.append(result[i][0])

        date = date + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date = datetime.datetime.strptime(date, timeformat)
        delta = datetime.timedelta(minutes=10)
        deltaend = datetime.timedelta(days=1)
        date2 = date + delta
        dateend = date + deltaend
        index = 0
        while date < dateend:
            ipdict = {}
            ipdict['all'] = 0
            for i in range(len(topiplist)):
                ipdict[topiplist[i]] = 0
            for i in range(len(twotopiplist)):
                ipdict[twotopiplist[i]] = 0
            for i in range(len(threetopiplist)):
                ipdict[threetopiplist[i]] = 0
            for i in range(self.tenminuteindex[index], self.tenminuteindex[index + 1]):
                ipdict['all'] += 1
                tempiplist = self.ffdata[i][0].split('.')
                threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
                twoip = tempiplist[0] + '.' + tempiplist[1]
                if self.ffdata[i][0] in topiplist:
                    ipdict[self.ffdata[i][0]] += 1
                if threeip in threetopiplist:
                    ipdict[threeip] += 1
                if twoip in twotopiplist:
                    ipdict[twoip] += 1
            sql2 = 'INSERT INTO procdata.ten_minute_count (time, ip, query) VALUES (%s, %s, %s)'
            for key in ipdict:
                self.cursor.execute(sql2, (date.strftime(timeformat), key, ipdict[key]))
            buydict = {}
            buydict['all'] = 0

            for i in range(self.tenminuteindex_buy[index], self.tenminuteindex_buy[index + 1]):
                buydict['all'] += 1
                tempiplist = self.ssdata[i][0].split('.')
                threeip = tempiplist[0] + '.' + tempiplist[1] + '.' + tempiplist[2]
                twoip = tempiplist[0] + '.' + tempiplist[1]
                if self.ssdata[i][0] in topiplist:
                    if self.ssdata[i][0] in buydict:
                        buydict[self.ssdata[i][0]] += 1
                    else:
                        buydict[self.ssdata[i][0]] = 1
                if threeip in threetopiplist:
                    if threeip in buydict:
                        buydict[threeip] += 1
                    else:
                        buydict[threeip] = 1
                if twoip in twotopiplist:
                    if twoip in buydict:
                        buydict[twoip] += 1
                    else:
                        buydict[twoip] = 1
            sql2 = "UPDATE procdata.ten_minute_count SET buy = %s WHERE `time` = %s AND ip = %s"
            for key in buydict:
                self.cursor.execute(sql2, (buydict[key], date.strftime(timeformat), key))
            date += delta
            date2 += delta
            index += 1
        return 0
    #日用ip近几天的查询情况与ip头三位ip近几天查询情况判断ip是否为正样本，作为svm的填充正样本
    def MakePositiveSample(self, a):
        positiveiplist = []
        flagshoplist = self.flagshoplist
        sql = 'SELECT ip FROM procdata.ft_day WHERE ordercount > 0 AND `date` = %s'
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            if result[i][0] not in flagshoplist:  # 剔除旗舰店数据
                positiveiplist.append(result[i][0])
        positiveiplist = np.unique(positiveiplist)
        timeformat = '%Y-%m-%d %H:%M:%S'
        timeformat2 = '%Y-%m-%d'
        tempdate1 = a
        a = a + ' 00:00:00'
        date1 = datetime.datetime.strptime(a, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta
        tempdate2 = datetime.datetime.strptime(tempdate1, timeformat2) + delta
        tempdate1 = datetime.datetime.strptime(tempdate1, timeformat2) - delta

        sql2 = 'SELECT ip,starttime FROM procdata.sessiondiv WHERE starttime >= %s and starttime < %s'
        self.cursor.execute(sql2, (a, datetime.datetime.strftime(date2, timeformat)))
        result = self.cursor.fetchall()

        sql3 = 'UPDATE procdata.sessiondiv SET class = 1 WHERE ip = %s AND starttime = %s'
        for i in range(len(result)):
            if result[i][0] in positiveiplist:
                self.cursor.execute(sql3, (result[i][0], datetime.datetime.strftime(result[i][1], timeformat)))

        sql = 'SELECT ip FROM procdata.sessiondiv WHERE class is null AND query between 10 AND 200 AND starttime >= %s and starttime < %s'
        self.cursor.execute(sql, (a, datetime.datetime.strftime(date2, timeformat)))
        result = self.cursor.fetchall()
        iplist = []
        for i in range(len(result)):
            if result[i][0] not in flagshoplist:
                iplist.append(result[i][0])
        iplist = np.unique(iplist)

        for i in range(len(iplist)):
            tempipl = iplist[i].split('.')
            tempip = tempipl[0] + '.' + tempipl[1] + '.' + tempipl[2]
            sql = 'SELECT querycount FROM procdata.ft_three_day WHERE `date` >= %s and `date` <= %s and ip = %s'
            self.cursor.execute(sql, (
            datetime.datetime.strftime(tempdate1, timeformat2), datetime.datetime.strftime(tempdate2, timeformat2),
            tempip))
            result2 = self.cursor.fetchall()
            threedaycount = 0
            for j in range(len(result2)):
                threedaycount += result2[j][0]
            if threedaycount < 500.0 * len(result2):
                sql = 'SELECT querycount FROM procdata.ft_day WHERE `date` >= %s and `date` <= %s and ip = %s'
                self.cursor.execute(sql, (
                datetime.datetime.strftime(tempdate1, timeformat2), datetime.datetime.strftime(tempdate2, timeformat2),
                iplist[i]))
                result3 = self.cursor.fetchall()
                threedaycount = 0
                for j in range(len(result3)):
                    threedaycount += result3[j][0]
                if threedaycount < 180 * len(result3):
                    sql3 = 'UPDATE procdata.sessiondiv SET class = 1 WHERE ip = %s AND starttime >= %s AND starttime < %s'
                    self.cursor.execute(sql3, (iplist[i], a, datetime.datetime.strftime(tempdate2, timeformat2)))
        return 1
    #利用四位ip，三位ip，二位ip数据发现动态ip的爬虫ip以及 单个的爬虫ip ，作为svm训练的填充负样本
    def MakeNegativeSample(self, a):
        negativeiplist = self.GetTopTwoIp(a, 5)
        toplist = []
        sql = 'SELECT ip FROM procdata.ft_day WHERE ordercount = 0 AND querycount > 300 AND `date` = %s order by querycount desc limit 0,30'
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            toplist.append(result[i][0])
        toplist = np.unique(toplist)

        timeformat = '%Y-%m-%d %H:%M:%S'
        a = a + ' 00:00:00'
        date1 = datetime.datetime.strptime(a, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta

        sql2 = 'SELECT ip,starttime FROM procdata.sessiondiv WHERE class is null AND starttime >= %s and starttime < %s'
        self.cursor.execute(sql2, (a, datetime.datetime.strftime(date2, timeformat)))
        result = self.cursor.fetchall()
        sql3 = 'UPDATE procdata.sessiondiv SET class = 2 WHERE ip = %s AND starttime = %s'
        for i in range(len(result)):
            temp = result[i][0]
            templist = temp.split('.')
            temp = templist[0] + '.' + templist[1]
            if (temp in negativeiplist) or (result[i][0] in toplist):
                self.cursor.execute(sql3, (result[i][0], datetime.datetime.strftime(result[i][1], timeformat)))
        return 0
    #给一个ip，返回ip的具体地址，速度并不快，在用于与客户端交流时不建议多次调用
    def ipwhere(self, ip):
        tempip = ip.split('.')
        ipnum = 0
        ipnum += int(tempip[3]) + int(tempip[2]) * 256 + int(tempip[1]) * 256 * 256 + int(tempip[0]) * 256 * 256 * 256
        sql = '''SELECT Country, Local
              FROM {tbname}
              WHERE StartIPNum <= {ipnum}
              AND EndIPNum >= {ipnum}'''.format(tbname='procdata.ipwhere', ipnum=ipnum)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        temp = result[0][0] + result[0][1]
        return {'ipwhere': temp}
    #将每天比较突出的ip先做成快表，加快服务器与客户端的交流速度。（做这么一步主要就是因为ipwhere太慢了。。。）
    def forfaster(self, tempdate):
        date = tempdate
        topiplist = []
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND ordercount = 0 ORDER BY querycount desc LIMIT 0 , 50"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY ordercount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        topiplist = np.unique(topiplist)
        for i in range(len(topiplist)):
            sql = "SELECT querycount,ordercount FROM procdata.ft_day WHERE date = %s AND ip = %s"
            self.cursor.execute(sql, (date, topiplist[i]))
            result = self.cursor.fetchall()
            if len(result) == 0:
                continue
            iploc = self.ipwhere(topiplist[i])['ipwhere']
            sql2 = 'INSERT INTO procdata.topipmessage (date, ip, querycount,ordercount,kinds,address) VALUES (%s, %s, %s,%s, %s, %s)'
            self.cursor.execute(sql2, (date, topiplist[i], result[0][0], result[0][1], 4, iploc))
        topiplist = []
        sql = "SELECT ip FROM procdata.ft_three_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 30"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        for i in range(len(topiplist)):
            sql = "SELECT querycount,ordercount FROM procdata.ft_three_day WHERE date = %s AND ip = %s"
            self.cursor.execute(sql, (date, topiplist[i]))
            result = self.cursor.fetchall()
            if len(result) == 0:
                continue
            iploc = self.ipwhere(topiplist[i] + '.1')['ipwhere']
            sql2 = 'INSERT INTO procdata.topipmessage (date, ip, querycount,ordercount,kinds,address) VALUES (%s, %s, %s,%s, %s, %s)'
            self.cursor.execute(sql2, (date, topiplist[i], result[0][0], result[0][1], 3, iploc))
        return 0

