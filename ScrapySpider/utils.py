# -*- coding: utf-8 -*-

import os
import time
import datetime
import random
import requests

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
BASE = r'D:/Media/'
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
        image = get_response('https://ig-s-d-a.akamaihd.com/123/10665410_490343621069339_798024183_a.jpg')
        if not image:
            print "%s:request error (%s)." % (datetime.datetime.today(), src_url)
            return
        f = open(save_file, 'wb')
        f.write(image.content)
        f.close()
    