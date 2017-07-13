# -*- coding: utf-8 -*-

import time
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
                    print e
                    time.sleep(random.randint(0, 10))
                    att += 1
        return wrapper
    return decorator

## 使用语法糖@来装饰函数
## A = retry(get_response)
@retry(attempt=3)
def get_response(url):
    #requests超时时间默认是 urllib3 中的 DEFAULT_TIMEOUT, getdefaulttimeout  默认的超时时间是 None，亦即连接永远不会超时。
    response = requests.get(url, timeout=15)
    return response
