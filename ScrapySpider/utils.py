# -*- coding: utf-8 -*-

import os
import time
import random
import logging
import datetime
import requests

"""
@定制日志输出
"""
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False, #set to True see skip scrapy running information
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)s %(name)s %(message)s'
        },
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'logging.NullHandler',
        },
        'mysql_handler':{
            # The values below are popped from this dictionary and
            # used to create the handler, set the handler's level and
            # its formatter.
            "class": "logging.handlers.TimedRotatingFileHandler",
            'level':'INFO',
            'formatter': 'simple',
            # The values below are passed to the handler creator callable
            # as keyword arguments.
            'filename': 'spider.sql',
            "backupCount": 100,
            'encoding': 'utf-8'
        }
    },
    'loggers': {
        'default': {
            'handlers':['null'],
            'level':'DEBUG',
            'propagate': True
        },
        'mysql': {
            'handlers': ['mysql_handler'],
            'level': 'INFO',
            'propagate': False
        }
    },
    "root": {
        "level": "NOTSET",
        "handlers": ["null"]
    }
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('mysql')

"""
@获取列表的第一个元素
"""
def get_extracted(value, index=0):
    try:
        return value[index]
    except:
        return ""

"""
@发送GET请求失败后优雅的重试 
"""
def retry(attempt):
    def decorator(func):
        def wrapper(*args, **kw):
            att = 0
            while att < attempt:
                try:
                    return func(*args, **kw)
                #requests.exceptions.Timeout
                #requests.exceptions.ConnectionError
                #requests.exceptions.SSLError
                except Exception as e:
                    print "%s:%s" % (datetime.datetime.today(), e)
                    time.sleep(att * random.randint(3, 20))
                    att += 1
        return wrapper
    return decorator

## 使用语法糖@来装饰函数
## A = retry(get_response)
@retry(attempt=3*2)
def get_response(url):
    #requests超时时间默认是 urllib3 中的 DEFAULT_TIMEOUT, getdefaulttimeout  默认的超时时间是 None，亦即连接永远不会超时。
    response = requests.get(url, timeout=20)
    return response

"""
@下载媒体资源
"""
from sys import platform as _platform

BASE = r'D:/Media/'
if _platform == "linux" or _platform == "linux2":
    BASE = r' /root/Pictures/'#root
    BASE = r'/home/<username>/Downloads/'
elif _platform == "darwin":
    BASE = r' /Users/<username>/Downloads/'
elif _platform == "win32":
    pass
elif _platform == "win64":
    pass

DOMAIN = r'http://m.hotlinks.org'
def media_dl(src_url, dst_name='', user_id='avatar'):
    save_path = BASE + user_id
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    if not dst_name:
        dst_name = os.path.basename(src_url)
    
    save_file = save_path + '/' + dst_name
    
    if not os.path.exists(save_file):
        print "%s:request (%s)." % (datetime.datetime.today(), src_url)
        image = get_response(src_url)
        if not image:
            print "%s:request error (%s)." % (datetime.datetime.today(), src_url)
            return
        f = open(save_file, 'wb')
        f.write(image.content)
        f.close()
    