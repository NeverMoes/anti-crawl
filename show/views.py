from django.shortcuts import render
# Create your views here.
from django.http import JsonResponse
from django.http import HttpResponseRedirect
from django.http import HttpResponse
from .form import LoginForm
from .mysql import Mysqldb
db = Mysqldb()


def api(request):
    data = request.GET
    return JsonResponse(data)


def api_topten(request):
        return JsonResponse(db.topten(request.GET['date'], request.GET['type']))


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


def ajax(request):
    data = request.GET
    return JsonResponse(data)