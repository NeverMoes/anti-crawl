from collections import namedtuple
from utils.consts import _const
import os

"""
数据包和配置的定义
"""

Rawpkg = namedtuple('Rawpkg', ['ip', 'querytime', 'command', 'depature', 'arrival', 'result'])
"""
原始的数据包
ip         ip地址
command    类型
depature   出发地
arrival    到达地
querytime  查询时间
result     查询结果
"""

Rdpkg = namedtuple('Rdpkg', ['ip', 'query', 'order', 'stime', 'ltime'])
"""
进入redis的数据包
!!! 注意这个数据包内的时间为时间戳
ip     ip地址
query  查询次数
order  订票次数
stime  开始查询时间
ltime  最后一次查询时间
"""

Catchedpkg = namedtuple('Catchedpkg', ['ip', 'time', 'type'])
"""
被捕捉到的查询的数据包
ip    ip地址
time  查询的时间
type  被捕捉的类型
"""

Svmpkg = namedtuple('Svmpkg', ['duration', 'querycount', 'depcount', 'arrcount', 'errpro', 'std', 'mean'])
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

# /cache
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

cacheconf = _const()

cacheconf.DBCONF = {'host': '127.0.0.1', 'port': 3306,
                    'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

cacheconf.TIMEOUT = 7200
cacheconf.SVMPROBABILITY = 0.9

cacheconf.SVM_PATH = os.path.join(BASE_DIR, 'svmmodels', 'SVMmodel', 'svmmodel.pkl')

cacheconf.BACKUP_TABLE = 'cachedata.backup'
cacheconf.CATCHED_TABLE = 'cachedata.catchedinfo'
cacheconf.INPUT_TABLE = 'zhxdata.totalcmd_dzairb2c_szx411'

cacheconf.FILELOG_PATH = 'catchedlog.log'
cacheconf.SOCK_PATH = os.path.join(BASE_DIR, 'cache.sock')
