import logging
import sys
from .pkg import *


class Outputor(object):
    """
    输出的抽象类
    """

    def output(self, catchedpkg):
        raise NotImplementedError


class Database(Outputor):
    """
    数据库的实例
    """

    def __init__(self, connpool):
        self.connpool = connpool
        self.rawpkg = None

        conn = self.connpool.connect()
        cursor = conn.cursor()
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS cachedata.catchedinfo (\n'
            '`ip` varchar(30) NOT NULL,\n'
            '`querytime` datetime NOT NULL,\n'
            '`type` varchar(30) DEFAULT NULL\n'
            ') ENGINE=MyISAM DEFAULT CHARSET=utf8\n'
        )
        conn.close()

    def output(self, catchedpkg):
        conn = self.connpool.connect()
        cursor = conn.cursor()
        cursor.execute(
            ('insert into {table}\n'
             '(`ip`, `time`, `type`)\n'
             'VALUES (\'{ip}\', \'{time}\', \'{type}\')'
             ).format(
                table=cacheconf.CATCHEDTABLE,
                ip=catchedpkg.ip,
                time=catchedpkg.time,
                type='cache'
            )
        )
        conn.close()


class Logger(Outputor):
    """
    日志的实例
    """

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.setLevel(logging.INFO)

    def output(self, catchedpkg):
        self.logger.info('ip: ' + str(catchedpkg.ip) + ', time: ' + str(catchedpkg.time))


class FileLogger(Outputor):
    """
    文件日志的实例
    """

    def __init__(self):
        self.logger = logging.getLogger('filelogger')
        self.logger.addHandler(logging.FileHandler(cacheconf.FILELOG_PATH))
        self.logger.setLevel(logging.INFO)

    def output(self, catchedpkg):
        self.logger.info('ip: ' + str(catchedpkg.ip) + ', time: ' + str(catchedpkg.time))
