import pymysql
from .core import Core
from utils.consts import const
from .pak import *


class Cache(object):
    """
    负责对进出core，进行调度的类
    """

    def __init__(self, cacheconf):
        self.init_conf(cacheconf)
        self.db = Database()
        return

    def init_conf(self, cacheconf):
        return

    def init_core(self, coreconf):
        self.core = Core(coreconf)
        return

    def sent(self, rawpak):
        """
        负责对原始数据进行转发
        """
        self.db.backup(rawpak)
        self.core.rec(rawpak)
        return

    def rec(self):
        """
        所有进入cache数据的统一入口
        """
        return


class Database(object):
    """
    对所有数据进行备份供svm进行使用
    为提高效率进行了缓存处理
    """
    def __init__(self, max=10000):
        self.connection = pymysql.connect(**const.DBCONF)
        self.cursor = self.connection.cursor()
        self.cursor.execute('''
        create table if not exists cachedata.backup(
        `ip` varchar(30) DEFAULT NULL,
        `querytime` datetime NOT NULL,
        `command` varchar(20) DEFAULT NULL,
        `depature` varchar(5) NOT NULL,
        `arrival` varchar(5) NOT NULL,
        `result` varchar(500) NOT NULL,
        KEY `querytime` (`querytime`),
        KEY `ip` (`ip`)
        ) ENGINE=MyISAM DEFAULT CHARSET=gbk
        ''')
        self._maxlen = max
        self._cache = list()
        return

    def __del__(self):
        self.cursor.execute('''
        DROP TABLE IF EXISTS cachedata.backup
        ''')
        self.cursor.close()
        self.connection.close()
        return

    def backup(self, rawpak):
        if len(self._cache) < self._maxlen:
            self._cache.append(rawpak)
            return
        else:
            self.flush()
            return

    def flush(self):
        basesql = '''
        insert into cachedata.backup
        (`ip`, `querytime`, `command`, `depature`, `arrival`, `result`)
        VALUES
        '''
        datafm = ("('{ip}', '{querytime}', '{command}',"
                  "'{depature}', '{arrival}', '{result}')")
        for rawpak in self._cache:
            basesql += datafm.format(ip=rawpak.ip, querytime=rawpak.querytime,
                                     command=rawpak.command, depature=rawpak.depature,
                                     arrival=rawpak.arrival, result=rawpak.result)
        print(basesql)
        self.cursor.execute(basesql)
        self._cache.clear()
        return
