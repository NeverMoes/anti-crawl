class _const:
    class ConstError(TypeError):
        pass

    class ConstCaseError(ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError('Cant change const %s.' % name)
        if not name.isupper():
            raise self.ConstCaseError(
                'const name %s is not all uppercase' % name)
        self.__dict__[name] = value


const = _const()

# 数据库参数
const.DBCONF = {'host': '127.0.0.1', 'port': 3306,
                'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

# 各种表名
const.DBPROC = 'procdata'

const.APITOP = 'procdata.ft_day'
const.APIROUTE = 'procdata.ip_route_count'
const.IPWHERE = 'procdata.ipwhere'
const.APITENMINUTECOUNT = 'procdata.ten_minute_count'
const.RAWFF = 'zhxdata.flightshopping_dzairb2c_szx411'
const.RAWSS = 'zhxdata.sellseat_dzairb2c_szx411'
const.IPCATCHEDRUN = 'procdata._ipcatched'
const.PROCCMD = 'procdata.cmd_cache'
const.IPCATCHED = 'procdata.ip_catched'


const.TABLE_IP = '`ip`'
const.TABLE_QUERYTIME = '`querytime`'
const.TABLE_DE = '`depature`'
const.TABLE_AR = '`arrival`'
const.TABLE_ER = '`result`'
const.TABLE2_ROUTE = '`semgent`'


# 路径参数
import os
# /cache
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
const.SVM_PATH = os.path.join(BASE_DIR, 'svmmodels', 'SVMmodel', 'svmmodel.pkl')

