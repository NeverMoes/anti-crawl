import pymysql
import multiprocessing as processing
import multiprocessing.dummy as threading
from queue import LifoQueue
from .core import Core
from utils.consts import const
from .pak import *


class Cache(processing.Process):
    """
    负责对进出core，进行调度的类
    """

    def __init__(self, cacheconf):
        super().__init__()
        self.init_conf(cacheconf)
        self.init_core(cacheconf)

        self.queue = LifoQueue()
        self.backup_queue = LifoQueue()
        self.core_queue = LifoQueue()
        self.backupdb = Backupdb(self.backup_queue)
        return

    def run(self):
        self.core.run()
        self.backupdb.run()
        return

    def init_conf(self, cacheconf):
        """
        根据不同的配置实例化不同的cache对象
        以便确定采用怎样的数据获取方式
        """
        if cacheconf == Cacheconf.SOCKET:
            pass
        if cacheconf == Cacheconf.LOCAL:
            pass
        if cacheconf == Cacheconf.SERVER:
            pass
        return

    def init_core(self, coreconf):
        """
        根据cache配置决定不同的core配置
        确定输出的方向
        """

        if cacheconf == Cacheconf.SOCKET:
            coreconf = Coreconf(
                log=True,
                db=True,
                file=True
            )
        if cacheconf == Cacheconf.LOCAL:
            coreconf = Coreconf(
                log=True,
                db=True,
                file=False
            )
        if cacheconf == Cacheconf.SERVER:
            coreconf = Coreconf(
                log=False,
                db=True,
                file=False
            )

        self.core = Core(coreconf, self.core_queue)
        return

    def sent(self, rawpak):
        """
        维护队列并对对原始数据进行转发
        """
        self.core_queue.put(rawpak)
        self.backup_queue.put(rawpak)
        return

    def rec(self):
        """
        所有进入cache数据的统一入口，转化成统一的数据格式并填入队列
        """
        return


class Backupdb(threading.Process):
    """
    对所有数据进行备份供svm进行使用
    """

    def __init__(self, queue):
        super().__init__(name='db_backup')
        self.queue = queue
        self.connection = pymysql.connect(**const.DBCONF)
        self.cursor = self.connection.cursor()

        self.cursor.execute(
            'create table if not exists cachedata.backup(\n'
            '`ip` varchar(30) DEFAULT NULL,\n'
            '`querytime` datetime NOT NULL,\n'
            '`command` varchar(20) DEFAULT NULL,\n'
            '`depature` varchar(5) NOT NULL,\n'
            '`arrival` varchar(5) NOT NULL,\n'
            '`result` varchar(500) NOT NULL,\n'
            'KEY `querytime` (`querytime`),\n'
            'KEY `ip` (`ip`)\n'
            ') ENGINE=MyISAM DEFAULT CHARSET=gbk\n'
        )
        return

    def __del__(self):
        self.cursor.execute("DROP TABLE IF EXISTS {table}".format(table=cacheconf.BACKUPTABLE))
        self.cursor.close()
        self.connection.close()
        return

    def run(self):
        while True:
            rawpak = self.queue.get(block=True)
            self.backup(rawpak)
        return

    def backup(self, rawpak):
        basesql = (
            'insert into cachedata.backup\n'
            '(`ip`, `querytime`, `command`, `depature`, `arrival`, `result`)\n'
            'VALUES (\'{ip}\', \'{querytime}\', \'{command}\', \'{depature}\', \'{arrival}\', \'{result}\')\n'
        ).format(
            ip=rawpak.ip, querytime=rawpak.querytime,
            command=rawpak.command, depature=rawpak.depature,
            arrival=rawpak.arrival, result=rawpak.result)
        self.cursor.execute(basesql)
        return
