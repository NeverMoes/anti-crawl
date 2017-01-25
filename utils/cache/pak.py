from collections import namedtuple
from enum import Enum

"""
数据包和配置的定义
"""


Rawpak = namedtuple('Rawpak', ['ip', 'querytime', 'command', 'depature', 'arrival', 'result'])
"""
原始的数据包，即选出的一部分有意义的数据进入cache
id         查询的id号
ip         ip地址
depature   出发地
arrival    到达地
querytime  查询时间
result     查询结果
"""


Rdpak = namedtuple('Rdpak', ['ip', 'query', 'order', 'stime', 'ltime'])
"""
进入redis的数据包
ip     ip地址
query  查询次数
order  订票次数
stime  开始查询时间
ltime  最后一次查询时间
"""


Catchedpak = namedtuple('CatchedPak', ['ip', 'time', 'type'])
"""
被捕捉到的查询的数据包
ip    ip地址
time  查询的时间
type  被捕捉的类型
"""


Svmpak = namedtuple('Svmpak', ['id', 'ip', 'depature', 'arrival', 'querytime', 'result'])
"""
进入svm的数据包
ip

"""


Coreconf = namedtuple('CoreConf', ['log', 'db', 'file'])
"""
core算法的配置
log    是否在屏幕上输出log
db     是否将捕获记录记录到数据库
file   是否将log输出到文件中
"""


Cacheconf = Enum('Cacheconf', ('socket', 'server', 'local'))
"""
cache实例化的配置
socket   监听端口，正式生产环境使用
server   从服务器中启动，即模拟实时
local    本地跑
"""


