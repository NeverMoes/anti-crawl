import pymysql
import redis
from datetime import datetime

from collections import namedtuple
from .const import const


class Mysqldb(object):
    def __init__(self):
        self.connect = pymysql.connect(**const.DBCONF)
        self.cursor = self.connect.cursor()


class Cache():
    def __init__(self, speed=False, wpro=500, spro=200, timeout=7200, mlen=30000):
        super().__init__()
        self.speed = speed
        self.wpro = wpro
        self.spro = spro
        self.timeout = timeout
        self.mlen= mlen

        # log文件文件的存储路径
        self.file = open('/home/nemos/project/log.txt', 'w')

        # 连接redis
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        # 初始化报表
        self.rd.hmset('end', {'svm': 0, 'pass': 0, 'warn': 0})

        self.mdb = Mysqldb()

        self.log('cache开始运行, 模拟速度无限大, 警告比例: {wpro}, 触发svm比例: {spro}, 废弃时间(单位秒): {timeout}'.format(wpro=self.wpro, spro=self.spro, timeout=self.timeout))

    # 生成日志
    def log(self, msg):
        print(msg)
        self.file.write(msg+'\n')
        return

    # 生成数据
    def produce(self):
        # 获得数据总数
        self.mdb.cursor.execute('''select count(*)
                                   from procdata.cmd_cache
                                   where querytime < "2016-08-24"
                                   ''')
        datacount = self.mdb.cursor.fetchone()[0]
        self.log('数据总数: ' + str(datacount))

        # 创建数据对象
        # ip, querytime, command, index_id
        self.mdb.cursor.execute('select * from procdata.cmd_cache limit 1')
        self.Data = namedtuple('Data', [x[0] for x in self.mdb.cursor.description])

        # 输出起始日期
        data = self.Data(*(self.mdb.cursor.fetchone()))
        self.log('起始日期: ' + str(data.querytime))

        # 开始输出数据
        for i in range(datacount):
            if i % 1000 == 0:
                print(i)
            self.rec(self.Data(*(self.mdb.cursor.fetchone())))

        self.end()
        self.file.close()
        return

    def clear(self, now):
        print('start clear')
        ips = self.rd.keys(pattern=r'\d+\.\d+\.\d+\.\d+')
        for ip in ips:
            data = self.dp(ip)
            if now.timestamp() - self.tp(data['ltime']).timestamp() > self.timeout:
                self.passed(data)
        return


    # msg接受到一个命名元组
    # ip, querytime, command

    def rec(self, msg):
        # 判断是否超出缓存
        # if self.rd.dbsize() > self.mlen:
        #     self.clear(now=msg.querytime)

        # 判断是否已存在
        if self.rd.exists(msg.ip):
            # 判断之前的数据是否要废弃
            if msg.querytime.timestamp() - self.tp(self.rd.hget(msg.ip, 'ltime').decode('utf8')).timestamp() > self.timeout:
                data = self.dp(msg.ip)
                # 清除数据
                self.passed(data)
                # 初始化数据
                self.rd.hmset(msg.ip, {'query': 1, 'stime': msg.querytime, 'ltime': msg.querytime, 'order': 0})
                return

            else:
                # 将哈希表解析成字典
                data = self.dp(msg.ip)

                # 判断是否为极端爬虫行为
                if int(data['query']) > self.wpro and int(data['order']) == 0:
                    self.warn(data)
                    return
                else:
                    # 订票量不为0,但是查询次数过高
                    if int(data['order']) != 0 and int(data['query']) / int(data['order']) > self.spro:
                        self.svm(data)
                        return
                    else:
                        # 刷新缓存
                        if msg.command == 'FlightShopping':
                            self.rd.hincrby(msg.ip, 'query')
                        else:
                            self.rd.hincrby(msg.ip, 'order')
                        self.rd.hset(msg.ip, 'ltime', msg.querytime)
                        return

        else:
            self.rd.hmset(msg.ip, {'query': 1, 'stime': msg.querytime, 'ltime': msg.querytime, 'order': 0})
            return

    # msg为字典
    def passed(self, msg):
        self.rd.hincrby('end', 'pass')
        self.rd.delete(msg['ip'])
        self.log('{ip}超时, 查询次数: {querycount}, 订票次数: {ordercount}, 上次查询时间: {ltime}'.format(
            ip=msg['ip'], querycount=msg['query'], ordercount=msg['order'], ltime=msg['ltime']))
        return


    # msg为字典
    def warn(self, msg):
        self.rd.hincrby('end', 'warn')
        self.rd.delete(msg['ip'])
        self.log('检测到极端爬虫行为: IP: {ip}, 查询次数: {querycount}, 订票次数: {ordercount}, 缓存时长: {time}(s)'.format(
            ip=msg['ip'], querycount=msg['query'], ordercount=msg['order'],
            time=self.tp(msg['ltime']).timestamp() - self.tp(msg['stime']).timestamp()))
        return


    # msg为字典
    def svm(self, msg):
        self.rd.hincrby('end', 'svm')
        self.rd.delete(msg['ip'])
        self.log('检测到需要svm的行为: IP: {ip}, 查询次数: {querycount}, 订票次数: {ordercount}, 缓存时长: {time}(s)'.format(
            ip=msg['ip'], querycount=msg['query'], ordercount=msg['order'],
            time=self.tp(msg['ltime']).timestamp() - self.tp(msg['stime']).timestamp()))
        return


    def end(self):
        # self.clear()
        data = self.rd.hgetall('end')
        data = {key.decode('utf-8'): value.decode('utf-8') for key, value in data.items()}
        self.log('模拟结束, 超时: {pass_}, 需要svm: {svm}, 极端爬虫: {warn}'.format(
            pass_=data['pass'], svm=data['svm'], warn=data['warn']))
        return


    @staticmethod
    def tp(strtime):
        timeymd, timehms = strtime.split(' ')
        year, mon, day = timeymd.split('-')
        hour, min, sec = timehms.split('.')[0].split(':')
        return datetime(int(year), int(mon), int(day), int(hour), int(min), int(sec))

    def dp(self, ip):
        data = {key.decode('utf-8'): value.decode('utf-8') for key, value in self.rd.hgetall(ip).items()}
        data['ip'] = ip
        return data


