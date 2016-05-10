from __future__ import absolute_import

import os,sys
from celery import Celery
from django.conf import settings

# Indicate Celery to use the default Django settings module
#sys.path.append('/home/ansible/git/sales-temperature/SalesTemperature')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SalesTemperature.settings')
 
app = Celery('SalesTemperature')
app.config_from_object('django.conf:settings')

# This line will tell Celery to autodiscover all your tasks.py that are in your app folders
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

