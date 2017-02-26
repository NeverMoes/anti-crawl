import datetime
from utils.historydeal.dealhistory import MakeTable

import sys

sys.path.append("/srv/anti_crawl")


def run(stime, etime):
    date1 = stime
    date2 = etime
    timeformat = '%Y-%m-%d'
    delta = datetime.timedelta(days = 1)
    date_1 = datetime.datetime.strptime(date1,timeformat)
    date_2 = datetime.datetime.strptime(date2,timeformat)
    db = 0
    while(date_1<date_2):
        db = MakeTable()
        db.maindeal(date_1.strftime(timeformat))
        db = 0
        print(date_1.strftime(timeformat))
        date_1 += delta
    return


if __name__ == '__main__':
    run('2016-08-23', '2016-10-24')