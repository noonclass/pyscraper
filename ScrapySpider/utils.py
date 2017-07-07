# -*- coding: utf-8 -*-

def get_extracted(value, index=0):
    try:
        return value[index]
    except:
        return ""