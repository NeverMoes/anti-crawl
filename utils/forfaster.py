# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 16:17:03 2017

@author: mao133132
"""

import pymysql
import numpy as np

config = {'host': '127.0.0.1', 'port': 3306,
                'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

def forfaster(tempdate):
    
    connect = pymysql.connect(config)
    cursor = connect.cursor()
    date = tempdate
    topiplist = []
    sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 50"
    cursor.execute(sql,(date))
    result = cursor.fetchall()
    for i in range(len(result)):
        topiplist.append(result[i][0])
    sql = "SELECT ip FROM procdata.ft_day WHERE date = %s AND ordercount = 0 ORDER BY querycount desc LIMIT 0 , 50"
    cursor.execute(sql,(date))
    result = cursor.fetchall()
    for i in range(len(result)):
        topiplist.append(result[i][0])
    sql = "SELECT ip FROM procdata.ft_day WHERE date = %s ORDER BY ordercount desc LIMIT 0 , 20"
    cursor.execute(sql,(date))
    result = cursor.fetchall()
    for i in range(len(result)):
        topiplist.append(result[i][0])
    topiplist = np.unique(topiplist)
    
    for i in range(len(topiplist)):
        sql = "SELECT querycount,ordercount FROM procdata.ft_day WHERE date = %s AND ip = %s"
        cursor.execute(sql,(date,topiplist[i]))
        result = cursor.fetchall()
        if len(result) == 0:
            continue
        iploc = ipwhere(topiplist[i], isref=True)['ipwhere']
        sql2 = 'INSERT INTO procdata.topipmessage (date, ip, querycount,ordercount,kinds,address) VALUES (%s, %s, %s,%s, %s, %s)'
        cursor.execute(sql2,(date,topiplist[i],result[0][0],result[0][1],4,iploc))
    
    topiplist = []
    sql = "SELECT ip FROM procdata.ft_three_day WHERE date = %s ORDER BY querycount desc LIMIT 0 , 30"
    cursor.execute(sql,(date))
    result = cursor.fetchall()
    for i in range(len(result)):
        topiplist.append(result[i][0])
    
    for i in range(len(topiplist)):
        sql = "SELECT querycount,ordercount FROM procdata.ft_three_day WHERE date = %s AND ip = %s"
        cursor.execute(sql,(date,topiplist[i]))
        result = cursor.fetchall()
        if len(result) == 0:
            continue
        iploc = ipwhere(topiplist[i]+'.1', isref=True)['ipwhere']
        sql2 = 'INSERT INTO procdata.topipmessage (date, ip, querycount,ordercount,kinds,address) VALUES (%s, %s, %s,%s, %s, %s)'
        cursor.execute(sql2,(date,topiplist[i],result[0][0],result[0][1],3,iploc))
    return 0 

def ipwhere(ip, isref=False):
    with pymysql.connect(config) as cursor:
        if isref:
            tempip = ip.split('.')
            ipnum = 0
            ipnum += int(tempip[3]) + int(tempip[2]) * 256 + int(tempip[1]) * 256 * 256 + int(
                tempip[0]) * 256 * 256 * 256
            sql = '''SELECT Country, Local
                  FROM {tbname}
                  WHERE StartIPNum <= {ipnum}
                  AND EndIPNum >= {ipnum}'''.format(tbname='procdata.ipwhere', ipnum=ipnum)
            cursor.execute(sql)
            result = cursor.fetchall()
            temp = result[0][0] + result[0][1]
            return {'ipwhere': temp}
        else:
            tempip = ip.split('.')
            ipnum = 0
            ipnum += int(tempip[3]) + int(tempip[2]) * 256 + int(tempip[1]) * 256 * 256 + int(tempip[0]) * 256 * 256 * 256
            sql = '''SELECT Country, Local
                  FROM {tbname}
                  WHERE StartIPNum <= {ipnum}
                  AND EndIPNum >= {ipnum}'''.format(tbname='procdata.ipwhere', ipnum=ipnum)
            cursor.execute(sql)
            result = cursor.fetchall()
            temp = result[0][0] + result[0][1]
    return {'ipwhere': temp}
    

if __name__ == '__main__':

    forfaster('')
