from enum import Enum
Cacheconf = Enum('Cacheconf', ('SOCKET', 'SERVER', 'LOCAL'))
conf = Cacheconf.SOCKET

if conf == Cacheconf.SOCKET:
    print('sadf')