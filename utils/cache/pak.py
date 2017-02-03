from collections import namedtuple
from enum import Enum
from utils.consts import _const

"""
数据包和配置的定义
"""


Rawpak = namedtuple('Rawpak', ['ip', 'querytime', 'command', 'depature', 'arrival', 'result'])
"""
原始的数据包
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
!!! 注意这个数据包内的时间为时间戳
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


Svmpak = namedtuple('Svmpak', ['duration', 'querycount', 'depcount', 'arrcount', 'errpro', 'std', 'mean'])
"""
svm所需要的数据
duration     持续时间
querycount   查询数
depcount     出发地数目
arrcount     到达地数目
errpro       查询错误率
std          标准差
mean         平均值

"""


Coreconf = namedtuple('CoreConf', ['log', 'db', 'file'])
"""
core算法的配置
log    是否在屏幕上输出log
db     是否将捕获记录记录到数据库
file   是否将log输出到文件中
"""



cacheconf = _const()

cacheconf.DBCONF = {'host': '127.0.0.1', 'port': 3306,
                'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

cacheconf.TIMEOUT = 7200
cacheconf.SVMPROBABILITY = 0.9

cacheconf.BACKUPTABLE = 'cachedata.backup'
cacheconf.CATCHEDTABLE = 'cachedata.catchedinfo'
cacheconf.FILELOG_PATH = 'catchedlog.log'




