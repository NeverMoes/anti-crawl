import socket
import pymysql
from .pak import *


class Inputor(object):
    """
    输入实例的抽象类
    """

    def input(self):
        raise NotImplementedError


class Database(Inputor):
    """
    从数据库中取数据的类
    用于复现模块
    """

    def __init__(self, sdate, edate):
        self.sdate = sdate
        self.edate = edate
        pass

    def input(self):
        with pymysql.connect(**cacheconf.DBCONF) as cursor:

            start_date = self.sdate + ' 00:00:00'
            end_date = self.edate + ' 00:00:00'

            # 确定数据总量
            cursor.execute(
                ('select count(*)\n'
                 'from {table}\n'
                 'WHERE querytime >= \'{sdate}\'\n'
                 'AND querytime < \'{edate}\'\n'
                 ).format(
                    sdate=start_date,
                    edate=end_date,
                    table=cacheconf.INPUT_TABLE
                )
            )
            total_count = cursor.fetchone()[0]

            # 确定索引起始位置
            cursor.execute(
                ('select index_id\n'
                 'from {table}\n'
                 'WHERE querytime >= \'{sdate}\'\n'
                 'limit 1\n'
                 ).format(
                    sdate=start_date,
                    edate=end_date,
                    table=cacheconf.INPUT_TABLE
                )
            )
            start_index = cursor.fetchone()[0]

            for index in range(start_index, start_index + total_count, 10000):
                # 一次读取10000条
                cursor.execute(
                    ('select ip, querytime, command, index_id\n'
                     'from {table}\n'
                     'where index_id >= {index}\n'
                     'limit 10000\n'
                     ).format(
                        table=cacheconf.INPUT_TABLE,
                        index=index
                    )
                )

                rows = cursor.fetchall()
                for row in rows:
                    rawpak = Rawpak(
                        ip=row[0],
                        querytime=row[1],
                        command=row[2],
                        depature='x',
                        arrival='x',
                        result='ok.'
                    )
                    yield rawpak
                    yield rawpak


class Socket(Inputor):
    """
    用于监听端口的输入类
    """

    def input(self):
        pass
