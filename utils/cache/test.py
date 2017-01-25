from utils.cache.cache import Database
from utils.cache.pak import Rawpak

db = Database()

db.backup(Rawpak(ip='dsfa',
                 querytime='2016-08-23 00:00:00',
                 command='sdafas',
                 depature='sad',
                 arrival='asd',
                 result='sadfsd'
                 ))
db.flush()