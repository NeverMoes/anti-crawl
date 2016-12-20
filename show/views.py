from django.shortcuts import render
from django.http import JsonResponse
from django.http import HttpResponse
from .form import LoginForm
import multiprocessing as mp
import datetime
from .mysql import Mysqldb
from utils.cacherun import Cache
from utils.const import const
from django.views.decorators.csrf import csrf_exempt

db = Mysqldb()


def jsonres(data=None, result=True, reason=None):
    res = dict()
    if result:
        if data:
            res['data'] = data
            res['code'] = 0
            res['msg'] = 'ok'
            return JsonResponse(res)
        else:
            res['code'] = -1
            res['msg'] = 'no data'
            return JsonResponse(res)
    else:
        res['code'] = 1
        res['msg'] = reason or 'error'
    return JsonResponse(res)


def api_deletesvm(request):
    if request.GET.get('name'):
        db.deletesvm(request.GET['name'])
        return jsonres({'deletesvm': 'ok'})
    else:
        return jsonres(result=False)


def api_showsvmmessage(request):
    return jsonres(db.showsvmmessage())

@csrf_exempt
def api_makesvm(request):
    params = request.POST
    if params.get('name') and params.get('datea') and params.get('dateb') and params.get('properties') and params.get('datas'):
        return jsonres(db.makesvm(name=params['name'], datea=params['datea'], dateb=params['dateb'],
                   properties=params['properties'], datas=params['datas']))
    else:
        return jsonres(result=False)


def api_addtrain(request):
    if request.GET.get('ip') and request.GET.get('starttime') and request.GET.get('class'):
        return jsonres(db.addtrainsession(inip=request.GET.get('ip'),
                                          instarttime=request.GET.get('starttime'),
                                          inclass=request.GET.get('class')))
    else:
        return jsonres(result=False)


def api_selecttrainsession(request):
    if request.GET.get('datea') and request.GET.get('dateb') and request.GET.get('properties') and request.GET.get('details'):

        return jsonres(db.selecttrainsession(datea=request.GET.get('datea'), dateb=request.GET.get('dateb'),
                              properties=request.GET.get('properties'),
                              details=request.GET.get('details')))

    else:
        return jsonres(result=False)


def api_getsessiontabledate(request):
    if request.GET.get('startdate') and request.GET.get('enddate'):
        return jsonres(db.getsessiontabledate(request.GET['startdate'], request.GET['enddate']))
    else:
        return jsonres(result=False)


def api_getiplist(request):
    return jsonres(db.getiplist())


def api_setiplist(request):
    params = request.GET
    if params.get('isdel'):
        if params.get('isdel') == 'true':
            return jsonres(db.setiplist(isdel=True, ip=params['ip']))
        else:
            if params.get('ip') and params.get('type') and params.get('label') and params.get('time') and params.get('istrash'):
                return jsonres(db.setiplist(ip=params['ip'], istrash=params['istrash'], label=params['label'],
                                            type=params['type'], time=params['time']))
            else:
                return jsonres(result=False, reason='wrong params')
    else:
        return jsonres(result=False, reason='wrong params')


def api_getcatchedcount(request):
    if request.GET.get('date'):
        return jsonres(db.getcatchedcount(request.GET['date']))
    else:
        return jsonres(result=False, reason='wrong params')


def api_getippiedata(request):
    if request.GET.get('date'):
        return jsonres(db.getippiedata(request.GET['date']))
    else:
        return jsonres(result=False, reason='wrong params')


def api_gettenminutecount(request):
    if request.GET.get('date') and request.GET.get('ip'):
        return jsonres(db.gettenminutecount(date=request.GET['date'], ip=request.GET['ip']))
    else:
        return jsonres(result=False, reason='wrong param')


# 给ip查其地址
def api_ipwhere(request):
    if request.GET.get('ip'):
        ip = request.GET['ip']
        return jsonres(db.ipwhere(ip))
    else:
        return jsonres(result=False, reason='lack ip')


# cache重现模块
def api_re(request):
    if request.GET.get('param'):
        param = request.GET.get('param')
        if param == 'start':
            if request.GET.get('date'):
                if mp.active_children():
                    return jsonres(result=False, reason='server is already running')
                else:
                    cache = Cache(date=request.GET.get('date'))
                    cache.start()
                    return jsonres({'start': 'ok'})
            else:
                return jsonres(result=False)

        elif param == 'stop':
            if mp.active_children():
                [(x.terminate(), x.join()) for x in mp.active_children()]
                return jsonres({'stop': 'ok'})
            else:
                return jsonres(result=False, reason='no server is running now')

        elif param == 'data':
            if mp.active_children():
                data = list()
                db.get_connetc()

                db.cursor.execute('''
                                  SELECT `time`
                                  FROM procdata._ipcatched
                                  ORDER BY `time` DESC
                                  LIMIT 1
                                  ''')

                _stupid = db.cursor.fetchone()

                if not _stupid:
                    db.close_connenct()
                    return jsonres(result=False, reason='please wait')

                now = _stupid[0]

                sdate = datetime.datetime.strptime(request.GET['date'], '%Y-%m-%d')
                mdate = sdate
                interval = datetime.timedelta(minutes=10)

                while True:
                    if mdate < now:
                        db.cursor.execute('''
                                          select '{stime}', count(ip)
                                          from procdata.cmd_cache
                                          where `querytime` >= '{stime}'
                                          and `querytime` < '{etime}'
                                          and `command` = 'FlightShopping'
                                          '''.format(stime=mdate, etime=mdate + interval))
                        querycount = db.cursor.fetchone()[1]

                        db.cursor.execute('''
                                          select '{stime}', count(ip)
                                          from procdata.cmd_cache
                                          where `querytime` >= '{stime}'
                                          and `querytime` < '{etime}'
                                          and `command` = 'SellSeat'
                                          '''.format(stime=mdate, etime=mdate + interval))
                        ordercount = db.cursor.fetchone()[1]

                        db.cursor.execute('''
                                          select '{stime}', count(ip)
                                          from procdata._ipcatched
                                          where `time` >= '{stime}'
                                          and `time` < '{etime}'
                                          '''.format(stime=mdate, etime=mdate + interval))

                        row = db.cursor.fetchone()
                        data.append({'time': row[0], 'catchedcount': row[1],
                                     'ordercount': ordercount, 'querycount': querycount})

                        mdate += interval
                    else:
                        break

                db.close_connenct()
                return jsonres(data)
            else:
                return jsonres(result=False, reason='you should first start the server')
        else:
            return jsonres(result=False, reason='wrong param')
    else:
        return jsonres(result=False, reason='lack param')


# 查路线
def api_route(request):
    if request.GET.get('date') and request.GET.get('ip'):
        return jsonres(db.route(date=request.GET['date'], ip=request.GET['ip']))
    elif request.GET.get('date'):
        return jsonres(db.route(date=request.GET['date']))
    else:
        return jsonres(result=False, reason='wrong param')


# 查一天的数据
def api_top(request):
    if request.GET.get('type') and request.GET.get('date') and request.GET.get('limit') and request.GET['type'] in ['order', 'query', 'onlyquery']:
        return jsonres(db.top(request.GET['date'], request.GET['type'], request.GET['limit']))
    else:
        return jsonres(result=False, reason='wrong param')


@csrf_exempt
def api(request):
    return JsonResponse(request.POST)


def index(request):
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            password = form.cleaned_data['password']
            if password == 'zhx2016':
                return render(request, 'list.html')
            else:
                return HttpResponse(u'密码错误')
    else:
        form = LoginForm()
        return render(request, 'index.html', {'form': form})
