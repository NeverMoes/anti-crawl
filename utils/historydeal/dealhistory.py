import pymysql
import datetime
import numpy as np
from utils.consts import const
from sklearn.externals import joblib


class MakeTable(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()
        self.svmpath = const.SVM_PATH

    def maindeal(self, datea):
        self.Day_Count(datea)
        self.MakeThreeIp(datea)
        self.MakeTwoIp(datea)
        self.TenMinuteCount(datea)
        self.MakeIpPieData(datea)
        self.makepie2(datea)
        self.route_count_daily(datea)


    # 制作ft_day表
    def Day_Count(self, a):
        tempdate = a + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(tempdate, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta

        self.cursor.execute('''
        SELECT {ip}
        from {table}
        WHERE {querytime} >= '{date}'
        AND {querytime} < '{date2}'
        '''.format(ip=const.TABLE_IP, table=const.RAWFF, querytime=const.TABLE_QUERYTIME,
                   date=date1.strftime(timeformat), date2=date2.strftime(timeformat)))
        result = self.cursor.fetchall()

        ipdict = {}
        for i in range(len(result)):
            if result[i][0] in ipdict:
                ipdict[result[i][0]] += 1
            else:
                ipdict[result[i][0]] = 1
        sql = 'INSERT INTO procdata.ft_day (querytime, ip, querycount) VALUES (%s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]))
        '''
        到这里为止，查询统计完成
        '''
        self.cursor.execute('''
        SELECT {ip}
        from {table}
        WHERE {querytime} >= '{date}'
        AND {querytime} < '{date2}'
        '''.format(ip=const.TABLE_IP, table=const.RAWSS, querytime=const.TABLE_QUERYTIME,
                   date=date1.strftime(timeformat), date2=date2.strftime(timeformat)))
        result = self.cursor.fetchall()

        buydict = {}
        for i in range(len(result)):
            if result[i][0] in buydict:
                buydict[result[i][0]] += 1
            else:
                buydict[result[i][0]] = 1

        sql = "UPDATE procdata.ft_day SET ordercount = %s WHERE date = %s AND ip = %s"
        for i in buydict.keys():
            self.cursor.execute(sql, (buydict[i], a, i))
        return

    # 制作ip头三位的块表ft_three_day
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
            else:
                ipdict[tempip] = {'querycount': result[i][2], 'ordercount': result[i][3]}
            if result[i][3] > 0:
                ipdict[tempip]['ordercount'] += result[i][3]

        sql = 'INSERT INTO procdata.ft_three_day (date, ip, querycount, ordercount) VALUES (%s, %s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]['querycount'], ipdict[i]['ordercount']))
        return

    # 制作ip头二位的块表ft_two_day
    def MakeTwoIp(self, a):
        sql = 'SELECT * FROM procdata.ft_three_day where date = %s'
        self.cursor.execute(sql, (a))
        result = self.cursor.fetchall()

        ipdict = {}
        for i in range(len(result)):
            tempip = result[i][1]
            templist = tempip.split('.')
            tempip = templist[0] + '.' + templist[1]
            if tempip in ipdict:
                ipdict[tempip]['querycount'] += result[i][2]
            else:
                ipdict[tempip] = {'querycount': result[i][2], 'ordercount': result[i][3]}
            if result[i][3] > 0:
                ipdict[tempip]['ordercount'] += result[i][3]

        sql = 'INSERT INTO procdata.ft_two_day (date, ip, querycount, ordercount) VALUES (%s, %s, %s, %s)'
        for i in ipdict.keys():
            self.cursor.execute(sql, (a, i, ipdict[i]['querycount'], ipdict[i]['ordercount']))
        return

    # 获取头两位ip有问题的ip头，不是建表函数，在session的正负样本自动选取中有用。
    def GetTopTwoIp(self, date, num):
        sql = 'SELECT ip FROM procdata.ft_two_day where date = %s AND ordercount = 0 order by querycount desc limit 0,%s'
        self.cursor.execute(sql, (date, num))
        result = self.cursor.fetchall()

        templist = []
        for i in range(len(result)):
            templist.append(result[i][0])
        return templist

    # 制作ip航线的块表ip_route_count
    def route_count_daily(self, a):
        date = a + ' 00:00:00'  # 开始时间
        timeformat = '%Y-%m-%d %H:%M:%S'
        date = datetime.datetime.strptime(date, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date + delta

        class1list = ['PEK', 'PVG', 'CAN', 'CTU', 'SZX', 'SHA']
        class2list = ['KMG', 'XIY', 'CKG', 'HGH', 'XMN', 'NKG', 'WUH', 'CSX', 'URC', 'TAO', 'CGO', 'SYX', 'HAK', 'TSN',
                      'DLC', 'HRB', 'KWE', 'SHE', 'FOC', 'NNG']
        class3list = ['TNA', 'TYN', 'CGQ', 'LHW', 'KHN', 'HET', 'WNZ', 'NGB', 'HFE', 'KWL', 'SJW', 'LJG', 'INC', 'NAY',
                      'ZUH', 'WUX', 'YNT', 'JHG', 'XNN', 'JJN', 'SWA', 'LXA', 'BAV', 'HLD', 'CZX', 'KHG', 'MIG', 'DSN',
                      'UYN', 'YNJ', 'JZH', 'DYG', 'LUM', 'WEH', 'XUZ', 'DLU', 'YIH', 'ZHA', 'YIW', 'KRL', 'NTG', 'BHY',
                      'AKU', 'LYI']
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

        topiplist = np.unique(topiplist)
        # 到这里为止保存了top50的ip
        self.cursor.execute('''
        SELECT {depature},{arrival},{ip}
        from {table}
        WHERE {querytime} >= '{date}'
        AND {querytime} < '{date2}'
        '''.format(depature=const.TABLE_DE, arrival=const.TABLE_AR, ip=const.TABLE_IP, table=const.RAWFF,
                   querytime=const.TABLE_QUERYTIME, date=date.strftime(timeformat), date2=date2.strftime(timeformat)))
        result = self.cursor.fetchall()

        routedict = {}
        for i in range(len(result)):
            alltemp = result[i][0] + result[i][1] + 'all'
            if alltemp in routedict:
                routedict[alltemp] += 1
            else:
                routedict[alltemp] = 1
            if result[i][2] in topiplist:
                temp = result[i][0] + result[i][1] + result[i][2]
                if temp in routedict:
                    routedict[temp] += 1
                else:
                    routedict[temp] = 1
            else:
                temp = result[i][0] + result[i][1] + 'other'
                if temp in routedict:
                    routedict[temp] += 1
                else:
                    routedict[temp] = 1

        sql = 'INSERT INTO procdata.ip_route_count (time, ip, route, query,kinds) VALUES (%s, %s, %s, %s, %s)'
        for i in routedict.keys():
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
                    routeclass = 1
                elif tempar in class2list:
                    routeclass = 3
                else:
                    routeclass = 5
            else:
                if tempar in class1list:
                    routeclass = 4
                else:
                    routeclass = 5
            self.cursor.execute(sql, (a, i[6:], i[0:6], routedict[i], routeclass))
        # 到这里为止统计了航线的查询数量
        self.cursor.execute('''
        SELECT {ip},{ssmessage}
        from {table}
        WHERE {querytime} >= '{date}'
        AND {querytime} < '{date2}'
        '''.format(ssmessage=const.TABLE2_ROUTE, ip=const.TABLE_IP, table=const.RAWSS, querytime=const.TABLE_QUERYTIME,
                   date=date.strftime(timeformat), date2=date2.strftime(timeformat)))
        result = self.cursor.fetchall()
        buydict = {}
        for i in range(len(result)):
            alltemp = result[i][1][9:12] + result[i][1][13:16] + 'all'
            if alltemp in buydict:
                buydict[alltemp] += 1
            else:
                buydict[alltemp] = 1
            if result[i][0] in topiplist:
                temp = result[i][1][9:12] + result[i][1][13:16] + result[i][0]
                if temp in buydict:
                    buydict[temp] += 1
                else:
                    buydict[temp] = 1
            else:
                temp = result[i][1][9:12] + result[i][1][13:16] + 'other'
                if temp in buydict:
                    buydict[temp] += 1
                else:
                    buydict[temp] = 1
        for i in buydict.keys():
            sql = "UPDATE procdata.ip_route_count SET buy = %s WHERE time = %s AND ip = %s AND route = %s"
            self.cursor.execute(sql, (buydict[i], a, i[6:], i[0:6]))
        return

    # 制作第一张饼图数据
    def MakeIpPieData(self, a):
        date = a
        sql = "SELECT count(*) FROM procdata.ft_day WHERE date = %s AND querycount > 0 AND querycount < 10"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        bet0and10 = result[0][0]
        sql = "SELECT count(*) FROM procdata.ft_day WHERE date = %s AND querycount >= 10 AND querycount < 30"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        bet10and30 = result[0][0]
        sql = "SELECT count(*) FROM procdata.ft_day WHERE date = %s AND querycount >= 30 AND querycount < 50"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        bet30and50 = result[0][0]
        sql = "SELECT count(*) FROM procdata.ft_day WHERE date = %s AND querycount >= 50 AND querycount < 100"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        bet50and100 = result[0][0]
        sql = "SELECT count(*) FROM procdata.ft_day WHERE date = %s AND querycount >= 100"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        over100 = result[0][0]
        allcount = bet0and10 + bet10and30 + bet30and50 + bet50and100 + over100
        sql2 = 'INSERT INTO procdata.piedata (time, between_0_and_10, between_10_and_30, between_30_and_50, between_50_and_100, over100, allip) VALUES (%s, %s, %s,%s, %s, %s, %s)'
        self.cursor.execute(sql2, (date, bet0and10, bet10and30, bet30and50, bet50and100, over100, allcount))
        return

    def makepie2(self, dataa):
        tempdate = dataa + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(tempdate, timeformat)
        delta = datetime.timedelta(days=1)
        date2 = date1 + delta
        svm = joblib.load(self.svmpath)
        flagshoplist = ['114.80.10.1', '61.155.159.41', '103.37.138.14', '103.37.138.10']

        recordlist = [0, 0, 0, 0, 0]
        buylist = [0, 0, 0, 0, 0]
        crawlerlist = [0, 0, 0, 0, 0]
        iscrawler = 1
        sql = 'SELECT * FROM procdata.ft_day where date = %s'
        self.cursor.execute(sql, (dataa))
        result = self.cursor.fetchall()
        sql2 = 'SELECT duration,query,depature,arrival,variance,mean,error FROM procdata.sessiondiv where `ip` = %s AND `starttime`>= %s AND `starttime` < %s'

        for i in range(len(result)):
            temp_ip = result[i][1]
            temp_query = result[i][2]
            if temp_ip in flagshoplist:  # 不计算旗舰店
                continue
            self.cursor.execute(sql2, (
            temp_ip, datetime.datetime.strftime(date1, timeformat), datetime.datetime.strftime(date2, timeformat)))
            tempresult = self.cursor.fetchall()
            for j in range(len(tempresult)):
                iscrawler = 1
                if tempresult[j][1] >= 10:
                    templist = []
                    for k in range(7):
                        templist.append(tempresult[j][k])
                    # 归一化
                    templist[0] = 1.0 * templist[0] / 10000
                    if templist[0] > 1:
                        templist[0] = 1
                    templist[1] = 1.0 * templist[1] / 200
                    if templist[1] > 1:
                        templist[1] = 1
                    templist[2] = 1.0 * templist[2] / 50
                    if templist[2] > 1:
                        templist[2] = 1
                    templist[3] = 1.0 * templist[3] / 50
                    if templist[3] > 1:
                        templist[3] = 1
                    templist[4] = 1.0 * templist[4] / 1000
                    if templist[4] > 1:
                        templist[4] = 1
                    templist[5] = 1.0 * templist[5] / 1000
                    if templist[5] > 1:
                        templist[5] = 1

                    iscrawler = svm.predict_proba(templist)
                    iscrawler = iscrawler[0][1]
                    if iscrawler > 0.9:
                        if temp_query < 10:
                            crawlerlist[0] += tempresult[j][1]
                        elif temp_query < 30:
                            crawlerlist[1] += tempresult[j][1]
                        elif temp_query < 50:
                            crawlerlist[2] += tempresult[j][1]
                        elif temp_query < 100:
                            crawlerlist[3] += tempresult[j][1]
                        else:
                            crawlerlist[4] += tempresult[j][1]
            if temp_query < 10:
                recordlist[0] += temp_query
                buylist[0] += result[i][3]
            elif temp_query < 30:
                recordlist[1] += temp_query
                buylist[1] += result[i][3]
            elif temp_query < 50:
                recordlist[2] += temp_query
                buylist[2] += result[i][3]
            elif temp_query < 100:
                recordlist[3] += temp_query
                buylist[3] += result[i][3]
            else:
                recordlist[4] += temp_query
                buylist[4] += result[i][3]

        rangelist = ['0_9', '10_29', '30_49', '50_99', '100_']
        sql3 = 'INSERT INTO procdata.piedata2 (`time`, `range`, `querycount`, `ordercount`, `crawlercount`) VALUES (%s, %s, %s,%s, %s)'
        for i in range(5):
            self.cursor.execute(sql3, (dataa, rangelist[i], recordlist[i], buylist[i], crawlerlist[i]))
        return 1

    def TenMinuteCount(self, a):  # a的格式为 '2016-08-23'
        date = a
        topiplist = []
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 20"
        self.cursor.execute(sql, (date))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            topiplist.append(result[i][0])
        sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND ordercount = 0 ORDER BY querycount desc LIMIT 0 , 20"
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
        date = date + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date = datetime.datetime.strptime(date, timeformat)
        delta = datetime.timedelta(minutes=10)
        deltaend = datetime.timedelta(days=1)
        date2 = date + delta
        dateend = date + deltaend
        while date < dateend:
            self.cursor.execute('''
            SELECT {ip}
            from {table}
            WHERE {querytime} >= '{date}'
            AND {querytime} < '{date2}'
            '''.format(ip=const.TABLE_IP, table=const.RAWFF, querytime=const.TABLE_QUERYTIME,
                       date=date.strftime(timeformat), date2=date2.strftime(timeformat)))
            result = self.cursor.fetchall()
            ipdict = {}
            ipdict['all'] = 0
            for i in range(len(result)):
                ipdict['all'] += 1
                if result[i][0] in topiplist:
                    if result[i][0] in ipdict:
                        ipdict[result[i][0]] += 1
                    else:
                        ipdict[result[i][0]] = 1
            sql2 = 'INSERT INTO procdata.ten_minute_count (time, ip, query) VALUES (%s, %s, %s)'
            for key in ipdict:
                self.cursor.execute(sql2, (date.strftime(timeformat), key, ipdict[key]))
            buydict = {}
            buydict['all'] = 0
            self.cursor.execute('''
            SELECT {ip}
            from {table}
            WHERE {querytime} >= '{date}'
            AND {querytime} < '{date2}'
            '''.format(ip=const.TABLE_IP, table=const.RAWSS, querytime=const.TABLE_QUERYTIME,
                       date=date.strftime(timeformat), date2=date2.strftime(timeformat)))
            result = self.cursor.fetchall()
            for i in range(len(result)):
                buydict['all'] += 1
                if result[i][0] in topiplist:
                    if result[i][0] in buydict:
                        buydict[result[i][0]] += 1
                    else:
                        buydict[result[i][0]] = 1
            sql2 = "UPDATE procdata.ten_minute_count SET buy = %s WHERE time = %s AND ip = %s"
            for key in buydict:
                self.cursor.execute(sql2, (buydict[key], date.strftime(timeformat), key))
            date += delta
            date2 += delta
        return




