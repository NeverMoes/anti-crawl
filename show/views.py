from django.shortcuts import render
from django.http import JsonResponse
from django.http import HttpResponse
from .form import LoginForm
import multiprocessing as mp

from .mysql import Mysqldb
from utils.cacherun import Cache

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
            if mp.active_children():
                return jsonres(result=False, reason='server is already running')
            else:
                queue = mp.Queue()
                cache = Cache(queue)
                cache.start()
                return jsonres({'start': 'ok'})

        elif param == 'stop':
            if mp.active_children():
                [(x.terminate(), x.join()) for x in mp.active_children()]
                return jsonres({'stop': 'ok'})
            else:
                return jsonres(result=False, reason='no server is running now')


        elif param == 'data':
            if mp.active_children():
                data = list()
                queue = mp.active_children()[0].queue
                for _ in range(10):
                    try:
                        data.append(queue.get(block=False))
                    except:
                        break
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


def api(request):
    return JsonResponse(request.GET)


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
