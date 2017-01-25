"""anti_crawl URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from show import views as show_views


urlpatterns = [

    ######################################################
    # 格式：
    # url(r'^api/deletesvm/$', show_views.api_deletesvm),
    # 访问示例
    # 简单说明(可省略)
    ######################################################

    ##########################################
    # 示例
    ##########################################

    #########################################
    url(r'^api/route/$', show_views.api_route),
    # /api/route/?date=2016-08-24
    # top模块用的
    ########################################


    url(r'^api/deletesvm/$', show_views.api_deletesvm),
    #


    url(r'^api/makesvm/$', show_views.api_makesvm),
    #


    url(r'^api/showsvmmessage/$', show_views.api_showsvmmessage),
    # api/showsvmmessage


    url(r'^api/addtrain/$', show_views.api_addtrain),
    # api/addtrain/?ip=xxxx&starttime=xxxx&class=x



    url(r'^api/selecttrainsession/$', show_views.api_selecttrainsession),
    # /api/selecttrainsession/


    url(r'^api/getarrivalsession/$', show_views.api_getsessiontabledate),
    # /api/getarrivalsession/?startdate=2016-09-01&enddate=2016-09-01



    url(r'^api/getiplist/$', show_views.api_getiplist),
    # api/getiplist/?


    url(r'^api/setiplist/$', show_views.api_setiplist),
    # api/setiplist/?ip=a&type=a&time=2016-08-24_00:00:00&istrash=false&isdel=false



    url(r'^api/getcatchedcount/$', show_views.api_getcatchedcount),
    # /api/getcatchedcount/?date=2016-08-23



    url(r'^api/getippiedata/$', show_views.api_getippiedata),
    # /api/getippiedata/?date=2016-08-23


    url(r'^api/gettenminutecount/$', show_views.api_gettenminutecount),
    # /api/gettenminutecount/?date=2016-08-23&ip=61.155.159.41


    url(r'^api/ipwhere/$', show_views.api_ipwhere),
    #


    url(r'^api/re/$', show_views.api_re),
    # /api/re/?params=start&date=2016-08-23


    url(r'^api/$', show_views.api),
    #


    url(r'^api/top/$', show_views.api_top),
    # api/top/?limit=10&type=query&date=2016-8-24


    url(r'^$', show_views.index),
    url(r'^admin/', admin.site.urls),
]
