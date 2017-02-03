import socket
import pymysql
from .pak import *


class Input(object):
    """
    输入实例的抽象类
    """
    def input(self):
        raise NotImplementedError


class Database(Input):
    """
    从数据库中取数据的类
    用于复现模块
    """
    def __init__(self):
        pass

    def input(self):
        pass


class Socket(Input):
    """
    用于监听端口的输入类
    """
    def input(self):
        pass