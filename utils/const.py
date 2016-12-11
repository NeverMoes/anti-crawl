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


const.APITOP = 'procdata.ft_day'
const.APIROUTE = 'procdata.ip_route_count'
const.IPWHERE = 'procdata.ipwhere'
const.APITENMINUTECOUNT = 'procdata.ten_minute_count'


const.PROCCMD = 'procdata.cmd_cache'
const.IPCATCHED = 'procdata.ip_catched'


const.DBCONF = {'host': '127.0.0.1', 'port': 3306,
                'user': 'root', 'passwd': 'yi', 'charset': 'utf8'}

const.DBPROC = 'procdata'


