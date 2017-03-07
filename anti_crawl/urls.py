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
    # /api/route/?date=2016-08-24&ip=115.220$114.80&kinds=1$2$3
    #  例子表示： 返回ip为 115.220.*.* 在8月24日 第1,2,3类航线的查询订票数据总和
    # ip的参数可以包括四位，三位，二位
    ########################################


    url(r'^api/deletesvm/$', show_views.api_deletesvm),
    ########################################
    # api/deletesvm/?name=111
    # 例子表示：删除已经训练出来的name为111的svm模型，存在删除后返回ok，不存在返回name error
    ########################################

    url(r'^api/makesvm/$', show_views.api_makesvm),
    ########################################
    # api/makesvm/?name=111&datas.....
    # 例子表示：训练的name为111的svm模型，datas用json提交
    ########################################

    url(r'^api/showsvmmessage/$', show_views.api_showsvmmessage),
    ########################################
    # api/showsvmmessage/
    # 获取已经制作的svm模型信息
    ########################################


    url(r'^api/addtrain/$', show_views.api_addtrain),
    ########################################
    # api/addtrain/?ip=xxxx&starttime=xxxx&class=x
    # 现在没用
    ########################################



    url(r'^api/selecttrainsession/$', show_views.api_selecttrainsession),
    ########################################
    # /api/selecttrainsession/?datea=2016-09-01&dateb=2016-09-05&
    # details=duration$0$3000@query$0$3000@depature$0$45@arrival$0$45@variance$0$300@mean$0$300@error$0$1@hotcity$0$1@normalcity$0$1
    # &pagenum=30&page=1
    # 获取符合条件的session 开始时间datea，终止时间dateb，details表示具体筛选条件，一页显示30个数据，返回第一页数据
    # 该url返回数据过多会导致速度十分缓慢，请勿将时间间隔设定过大，或范围过广
    ########################################

    url(r'^api/getsvmwrong/$', show_views.api_getsvmwrong),
    ########################################
    # /api/getsvmwrong/?datea=2016-09-01&dateb=2016-09-02&pagenum=30&page=1
    # 获取9月1号到9月2号之间，svm可能训练错误的数据，一页30，返回第一页
    ########################################

    url(r'^api/getsvmsuspect/$', show_views.api_getsvmsuspect),
    # /api/getsvmsuspect/?datea=2016-09-01&dateb=2016-09-02&pagenum=30&page=1

    url(r'^api/gettraineddata/$', show_views.api_gettraineddata),
    # /api/gettraineddata/?pagenum=30&page=1

    url(r'^api/getarrivalsession/$', show_views.api_getsessiontabledate),
    # /api/getarrivalsession/?startdate=2016-09-01&enddate=2016-09-02



    url(r'^api/getiplist/$', show_views.api_getiplist),
    # api/getiplist/?


    url(r'^api/changeiplist/$', show_views.api_changeiplist),
    # api/setiplist/?ip=a&type=a&time=2016-08-24_00:00:00&istrash=false&isdel=false


    url(r'^api/setiplist/$', show_views.api_setiplist),
    # api/setiplist/?ip=a&type=a&time=2016-08-24_00:00:00&istrash=false&isdel=false&reason=aaaa



    url(r'^api/getcatchedcount/$', show_views.api_getcatchedcount),
    # /api/getcatchedcount/?date=2016-08-23



    url(r'^api/getippiedata/$', show_views.api_getippiedata),
    # /api/getippiedata/?date=2016-08-23&kinds=1$2


    url(r'^api/getippiedata2/$', show_views.api_getippiedata2),
    # /api/getippiedata2/?date=2016-08-23&kinds=1$2


    url(r'^api/gettenminutecount/$', show_views.api_gettenminutecount),
    # /api/gettenminutecount/?date=2016-08-23&ip=61.155.159.41


    url(r'^api/ipwhere/$', show_views.api_ipwhere),
    #


    # url(r'^api/re/$', show_views.api_re),
    # /api/re/?params=start&date=2016-08-23


    url(r'^api/$', show_views.api),
    #


    url(r'^api/top/$', show_views.api_top),
    # api/top/?limit=10&type=query&date=2016-8-24

    url(r'^api/topthree/$', show_views.api_top_three),
    # api/topthree/?limit=10&date=2016-8-24

    url(r'^api/toptwo/$', show_views.api_top_two),
    # api/toptwo/?limit=10&date=2016-8-24

    #########################################
    url(r'^api/ipdaymessage/$', show_views.api_getipdaymessage),
    # /api/ipdaymessage/?ip=114.80.10.1&kinds=query$buy
    # 第四个页面用的
    ########################################

    #########################################
    url(r'^api/iphourmessage/$', show_views.api_getiphourmessage),
    # /api/iphourmessage/?ip=114.80.10.1&date=2016-08-23
    # 第四个页面用的
    ########################################

    url(r'^api/ipbriefmessage/$', show_views.api_getipbriefmessage),
    # /api/ipbriefmessage/?ip=114.80.10.1
    # 第四个页面用的
    ########################################

########################################

    url(r'^api/selectedip/$', show_views.api_getselectedip),
    # /api/selectedip/?
    # 第四个页面用的,获取可选ip
    ########################################

    ########################################

    url(r'^api/ipsessionmessage/$', show_views.api_getipsessionmessage),
    # /api/ipsessionmessage/?ip=114.80.10.1&kinds=query
    # session分布表用的
    ########################################


    url(r'^$', show_views.index),
    url(r'^admin/', admin.site.urls),
]
