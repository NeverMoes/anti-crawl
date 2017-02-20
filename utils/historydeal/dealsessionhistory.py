# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 13:26:48 2017

@author: mao133132
"""
'''
CREATE TABLE IF NOT EXISTS `allsessiontable` (
  `date` date NOT NULL,
  `kinds` varchar(20) NOT NULL,
  `range` int NOT NULL,
  `class2count` int NOT NULL,
  `class1count` int NOT NULL,
  PRIMARY KEY  (`date`,`kinds`,`range`)
) ENGINE=MyISAM DEFAULT CHARSET=gbk;
'''





import pymysql
import datetime
from .const import const

class MakeSessionTable(object):
    
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()
        self.whiteipdata = []
        self.blackipdata = []
    def maindeal(self,date):
        self.getdata(date)
        self.MakeQuerySessionTable(date)
        self.MakeDurationSessionTable(date)
        self.MakeDepatureSessionTable(date)
        self.MakeArrivalSessionTable(date)
        self.MakeVarianceSessionTable(date)
        self.MakeMeanSessionTable(date)
        self.MakeErrorSessionTable(date)
        self.MakeHotCitySessionTable(date)
        self.MakeNormalCitySessionTable(date)
        self.whiteipdata = []
        self.blackipdata = []
        return 1
    def getdata(self,date):
        tempdate = date + ' 00:00:00'
        timeformat = '%Y-%m-%d %H:%M:%S'
        date1 = datetime.datetime.strptime(tempdate,timeformat)
        delta = datetime.timedelta(days = 1)
        date2 = date1 + delta
        sql = 'SELECT * FROM procdata.sessiondiv where `starttime` >= %s and `starttime` < %s AND `class` is not NULL'
        self.cursor.execute(sql,(date1.strftime(timeformat),date2.strftime(timeformat)))
        result = self.cursor.fetchall()
        for i in range(len(result)):
            if result[i][13] == 1:
                self.whiteipdata.append(result[i])
            else:
                self.blackipdata.append(result[i])
        return 0
    
    def MakeQuerySessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempquery = self.whiteipdata[i][4]
            if tempquery < 300:
                tempquery = int(tempquery/5)
            else:
                tempquery = 60
            if tempquery in poipquery:
                poipquery[tempquery] += 1
            else:
                poipquery[tempquery] = 1
        for i in range(len(self.blackipdata)):
            tempquery = self.blackipdata[i][4]
            if tempquery < 300:
                tempquery = int(tempquery/5)
            else:
                tempquery = 60
            if tempquery in neipquery:
                neipquery[tempquery] += 1
            else:
                neipquery[tempquery] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(61):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'query',temp*5,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'query',temp*5,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'query',temp*5,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'query',temp*5,0,0))
        return 0
    
    def MakeDepatureSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempdepature = self.whiteipdata[i][6]
            if tempdepature in poipquery:
                poipquery[tempdepature] += 1
            else:
                poipquery[tempdepature] = 1
        for i in range(len(self.blackipdata)):
            tempdepature = self.blackipdata[i][6]
            if tempdepature in neipquery:
                neipquery[tempdepature] += 1
            else:
                neipquery[tempdepature] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(55):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'depature',temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'depature',temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'depature',temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'depature',temp,0,0))
        return 0
    def MakeArrivalSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempdepature = self.whiteipdata[i][7]
            if tempdepature in poipquery:
                poipquery[tempdepature] += 1
            else:
                poipquery[tempdepature] = 1
        for i in range(len(self.blackipdata)):
            tempdepature = self.blackipdata[i][7]
            if tempdepature in neipquery:
                neipquery[tempdepature] += 1
            else:
                neipquery[tempdepature] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(55):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'arrival',temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'arrival',temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'arrival',temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'arrival',temp,0,0))
        return 0
    def MakeDurationSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempquery = self.whiteipdata[i][3]
            if tempquery < 3000:
                tempquery = int(tempquery/30)
            else:
                tempquery = 100
            if tempquery in poipquery:
                poipquery[tempquery] += 1
            else:
                poipquery[tempquery] = 1
        for i in range(len(self.blackipdata)):
            tempquery = self.blackipdata[i][3]
            if tempquery < 3000:
                tempquery = int(tempquery/30)
            else:
                tempquery = 100
            if tempquery in neipquery:
                neipquery[tempquery] += 1
            else:
                neipquery[tempquery] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(101):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'duration',30*temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'duration',30*temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'duration',30*temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'duration',30*temp,0,0))
        return 0
    def MakeErrorSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempdepature = int(100*self.whiteipdata[i][10])
            tempdepature = tempdepature/2
            if tempdepature in poipquery:
                poipquery[tempdepature] += 1
            else:
                poipquery[tempdepature] = 1
        for i in range(len(self.blackipdata)):
            tempdepature = int(100*self.blackipdata[i][10])
            tempdepature = tempdepature/2
            if tempdepature in neipquery:
                neipquery[tempdepature] += 1
            else:
                neipquery[tempdepature] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(51):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'error',2*temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'error',2*temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'error',2*temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'error',2*temp,0,0))
        return 0
    def MakeHotCitySessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempdepature = int(100*self.whiteipdata[i][11])
            tempdepature = tempdepature/2
            if tempdepature in poipquery:
                poipquery[tempdepature] += 1
            else:
                poipquery[tempdepature] = 1
        for i in range(len(self.blackipdata)):
            tempdepature = int(100*self.blackipdata[i][11])
            tempdepature = tempdepature/2
            if tempdepature in neipquery:
                neipquery[tempdepature] += 1
            else:
                neipquery[tempdepature] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(51):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'hotcity',2*temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'hotcity',2*temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'hotcity',2*temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'hotcity',2*temp,0,0))
        return 0
    def MakeNormalCitySessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempdepature = int(100*self.whiteipdata[i][12])
            tempdepature = tempdepature/2
            if tempdepature in poipquery:
                poipquery[tempdepature] += 1
            else:
                poipquery[tempdepature] = 1
        for i in range(len(self.blackipdata)):
            tempdepature = int(100*self.blackipdata[i][12])
            tempdepature = tempdepature/2
            if tempdepature in neipquery:
                neipquery[tempdepature] += 1
            else:
                neipquery[tempdepature] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(51):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'normalcity',2*temp,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'normalcity',2*temp,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'normalcity',2*temp,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'normalcity',2*temp,0,0))
        return 0
    def MakeVarianceSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempquery = self.whiteipdata[i][8]
            if tempquery < 500:
                tempquery = int(tempquery/5)
            else:
                tempquery = 100
            if tempquery in poipquery:
                poipquery[tempquery] += 1
            else:
                poipquery[tempquery] = 1
        for i in range(len(self.blackipdata)):
            tempquery = self.blackipdata[i][8]
            if tempquery < 500:
                tempquery = int(tempquery/5)
            else:
                tempquery = 100
            if tempquery in neipquery:
                neipquery[tempquery] += 1
            else:
                neipquery[tempquery] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(101):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'variance',temp*5,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'variance',temp*5,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'variance',temp*5,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'variance',temp*5,0,0))
            
        return 0
    def MakeMeanSessionTable(self,a):
        neipquery  = {}
        poipquery = {}
        for i in range(len(self.whiteipdata)):
            tempquery = self.whiteipdata[i][9]
            if tempquery < 500:
                tempquery = int(tempquery/5)
            else:
                tempquery = 100
            if tempquery in poipquery:
                poipquery[tempquery] += 1
            else:
                poipquery[tempquery] = 1
        for i in range(len(self.blackipdata)):
            tempquery = self.blackipdata[i][9]
            if tempquery < 500:
                tempquery = int(tempquery/5)
            else:
                tempquery = 100
            if tempquery in neipquery:
                neipquery[tempquery] += 1
            else:
                neipquery[tempquery] = 1
        sql3 = "INSERT INTO procdata.allsessiontable VALUES (%s, %s, %s, %s, %s)"
        for i in range(101):
            temp = i
            if temp in neipquery:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'mean',temp*5,neipquery[temp],poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'mean',temp*5,neipquery[temp],0))
            else:
                if temp in poipquery:
                    self.cursor.execute(sql3,(a,'mean',temp*5,0,poipquery[temp]))
                else:
                    self.cursor.execute(sql3,(a,'mean',temp*5,0,0))
            
        return 0
        
        
    