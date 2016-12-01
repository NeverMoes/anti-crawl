import pymysql as mdb


class Mysqldb(object):
    def __init__(self):
        self.connect = mdb.connect(
            host='127.0.0.1', port=3306, user='root', passwd='yi')
        self.cursor = self.connect.cursor()
        self.cursor.execute('USE zhx')

    # 接受：日期
    # 返回：ip: 排名，ip访问次数，ip订票量
    def topten(self, date, type, num):

        if type == 'query':
            data = dict()
            self.cursor.execute('''
            select ip, querycount, ordercount
            from ft_day
            where querytime = '{date}'
            order by querycount desc
            limit {num}
            '''.format(date=date, num=num))
            for index, row in enumerate(self.cursor.fetchall()):
                data[index] = {'ip': row[0], 'querycount': row[1], 'ordercount': row[2]}
            return data

        elif type == 'order':
            data = dict()
            self.cursor.execute('''
            select ip, querycount, ordercount
            from ft_day
            where querytime = '{date}'
            order by ordercount desc
            limit num{num}
            '''.format(date=date, num=num))
            for index, row in enumerate(self.cursor.fetchall()):
                data[index] = {'ip': row[0], 'querycount': row[1], 'ordercount': row[2]}
            return data

        elif type == 'justquery':
            data = dict()
            self.cursor.execute('''
            select ip, querycount, ordercount
            from ft_day
            where querytime = '{date}'
            and ordercount = 0
            order by querycount desc
            limit {num}
            '''.format(date=date, num=num))
            for index, row in enumerate(self.cursor.fetchall()):
                data[index] = {'ip': row[0], 'querycount': row[1], 'ordercount': row[2]}
            return data

        else:
            raise Exception('queryerr')