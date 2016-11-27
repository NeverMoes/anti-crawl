"""
WSGI config for anti_crawl project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.10/howto/deployment/wsgi/
"""


import os
from os.path import join,dirname,abspath
 
os.environ["DJANGO_SETTINGS_MODULE"] = "anti_crawl.settings" # 7
 
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
