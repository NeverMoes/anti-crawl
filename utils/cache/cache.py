import pymysql
import multiprocessing as processing
import multiprocessing.dummy as threading
from queue import LifoQueue

from .core import Core
from .pak import *
from . import output
from . import input


class Cache(processing.Process):
    """

    """

    def __init__(self, server=False, local=False, sock=False):
        super().__init__()

        self.queue = LifoQueue()
        self.backup_queue = LifoQueue()
        self.core_queue = LifoQueue()

        self.input = None
        self.outputs = list()

        self.backupdb = Backupdb(self.backup_queue)
        self.core = Core(self.core_queue)

        if server and local and sock:
            raise Exception('wrong params')

        if server:
            self.outputs.extend([output.Database()])
            self.input = None
        elif local:
            self.outputs.extend([output.Database(), output.Logger()])
            self.input = None
        elif sock:
            self.outputs.extend([output.Database(), output.Logger(), output.FileLogger()])
            self.input = None
        else:
            raise Exception('wrong params')

    def run(self):
        self.core.start()
        self.backupdb.start()

        while True:
            pass

        return

    def output(self, rawpak):
        catchedpak = Catchedpak(
            ip=rawpak.ip,
            time=rawpak.querytime,
            type='cache'
        )
        [output.output(catchedpak) for output in self.outputs]
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
    对所有进入cache的数据进行备份供svm进行使用
    """

    def __init__(self, queue):
        super().__init__(name='db_backup')
        self.queue = queue
        self.connection = pymysql.connect(**cacheconf.DBCONF)
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
            ip=rawpak.ip,
            querytime=rawpak.querytime,
            command=rawpak.command,
            depature=rawpak.depature,
            arrival=rawpak.arrival,
            result=rawpak.result
        )
        self.cursor.execute(basesql)
        return
