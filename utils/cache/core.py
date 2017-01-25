from collections import namedtuple
import redis
import logging
import pymysql
from utils.consts import const


class Core(object):
    """
    筛选算法的具体实现
    """

    def __init__(self, conf):
        self.init_redis()
        self.init_conf(conf)
        return

    def init_redis(self):
        self.rd = redis.StrictRedis()
        self.rd.flushall()
        return

    def init_conf(self, conf):
        self.outputs = list()

        if conf.db:
            self.outputs.append(Database())
        if conf.log:
            self.outputs.append(Logger())
        if conf.file:
            self.outputs.append(FileLogger())
        return

    def output(self, catchedpak):
        [output.output(catchedpak) for output in self.outputs]
        return

    def rec(self, rawpak):
        pass


class Svmjuage(object):
    def __init__(self):
        pass


class Output(object):
    """
    输出的抽象类
    """
    def output(self, catchedpak):
        raise NotImplementedError


class Database(Output):
    """
    数据库的实例
    """

    def __init__(self):
        self.connection = pymysql.connect(**const.DBCONF)
        self.cursor = self.connection.cursor()

    def output(self, catchedpak):
        pass


class Logger(Output):
    """
    日志的实例
    """
    def __init__(self):
        pass

    def output(self, catchedpak):
        pass


class FileLogger(Output):
    """
    文件日志的实例
    """
    def __init__(self):
        pass

    def output(self, catchedpak):
        pass





