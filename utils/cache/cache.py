import pymysql
import sqlalchemy.pool as pool
import multiprocessing as processing
import multiprocessing.dummy as threading
from queue import LifoQueue
import concurrent.futures
import socket
import signal

from .core import Core
from .pak import *
from . import outputor
from . import inputor


class Cache(processing.Process):
    """
    进程封装，处理输入和输出的类
    """

    def __init__(self, server=False, local=False, sock=False, **kwargs):
        super().__init__()

        self.queue = LifoQueue()

        self.inputor = None
        self.outputors = list()
        self.watcher = None
        self.kwargs = kwargs

        self.connpool = pool.QueuePool(
            lambda: pymysql.connect(**cacheconf.DBCONF),
            pool_size=10,
            max_overflow=10
        )

        self.backupdb = Backupdb(self.connpool)
        self.core = Core(self.connpool)

        if server and local and sock:
            raise Exception('wrong params')

        if server:
            self.outputors.extend([outputor.Database(self.connpool)])
            self.inputor = inputor.Database(self.kwargs['sdate'], self.kwargs['edate'])

            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if os.path.exists(cacheconf.SOCK_PATH):
                os.unlink(cacheconf.SOCK_PATH)
            self.sock.bind(cacheconf.SOCK_PATH)
            self.sock.listen(0)

            self.watcher = Watcher()

        elif local:
            self.outputors.extend([outputor.Database(self.connpool), outputor.Logger()])
            self.inputor = inputor.Database(self.kwargs['sdate'], self.kwargs['edate'])

        elif sock:
            self.outputors.extend([outputor.Database(self.connpool), outputor.Logger(), outputor.FileLogger()])
            self.inputor = inputor.Socket()

        else:
            raise Exception('wrong params')

    def run(self):
        if self.watcher:
            self.watcher.start()

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            """
            进程池
            使用mapreduce方法对数据进行处理
            """
            rawpak_generator = self.input()
            for rawpak, result in zip(rawpak_generator, executor.map(self.predict, rawpak_generator)):
                print(rawpak)
                if result:
                    self.output(rawpak)
                else:
                    pass
        return

    def predict(self, rawpak):
        self.backupdb.backup(rawpak)

        if self.core.predict(rawpak):
            return True
        else:
            return False

    def output(self, rawpak):
        catchedpak = Catchedpak(
            ip=rawpak.ip,
            time=rawpak.querytime,
            type='cache'
        )
        [outputor.output(catchedpak) for outputor in self.outputors]
        return

    def input(self):
        return self.inputor.input()


class Watcher(threading.Process):
    """
    服务器用的
    当cache在后台运行时
    这个线程会监听一个文件
    当服务器接受请求时，会找到这个监听的文件关闭后台的cache
    """
    def __init__(self):
        super().__init__()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    def run(self):
        while True:
            connection, address = self.sock.accept()
            data = connection.recv(1024)
            if data == 'close'.encode('utf-8'):
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                pass

"""
用于关闭cache的代码

client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
try:
    client.connect(cacheconf.SOCK_PATH)
except ConnectionRefusedError :
    pass
else:
    client.send('close'.encode('utf-8'))
finally:
    client.close()
"""


class Backupdb(object):
    """
    对所有进入cache的数据进行备份供svm进行使用
    """

    def __init__(self, connpool):
        self.connpool = connpool

        conn = self.connpool.connect()
        cursor = conn.cursor()
        cursor.execute(
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
        conn.close()

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
        conn = self.connpool.connect()
        cursor = conn.cursor()
        cursor.execute(basesql)
        conn.close()
        return
