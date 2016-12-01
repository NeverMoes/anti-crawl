import datetime



begin = datetime.date(2016, 9, 23)
end = datetime.date(2016, 9, 30)

for i in range((end - begin).days+1):
    day = begin + datetime.timedelta(days=i)
    day_1 = begin + datetime.timedelta(days=i+1)

    print(day,day_1)