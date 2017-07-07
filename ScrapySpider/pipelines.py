# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import json
import time
import redis
import logging
import requests
from ScrapySpider.items import InstagramPostItem, InstagramCommentItem

## sql写文件
logging.basicConfig(
    filename='spider.sql',
    format='%(message)s',
    level=logging.INFO
)

## 缓存连接
REDIS = redis.Redis(host='127.0.0.1', port=6379, db=0)
## 评论ID，不可重复。启动前必须配合数据库表当前值
COMMENT_INDEX = REDIS.get('instagram_comment_id') if REDIS.get('instagram_comment_id') else 1

def get_mysql4comment(comment, post):
    global COMMENT_INDEX
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(comment['date']))
    sql = ur"""INSERT INTO `wp_comments` (`comment_ID`, `comment_post_ID`, `comment_author`, `comment_date`, `comment_date_gmt`, `comment_content`, `user_id`) VALUES ({}, {}, '{}', '{}', '{}', '{}', {});""".format(COMMENT_INDEX, post['id'], comment['user'], dt, dt, comment['text'], comment['uid'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_commentmeta` (`comment_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_avatar', '{}');".format(COMMENT_INDEX, comment['avatar'])
    logging.info(sql)
    #自动生成最大评论ID
    COMMENT_INDEX += 1
    REDIS.set('instagram_comment_id', COMMENT_INDEX)

BASE = r'D:/Media/'

## 下载图片并保存到本地
class InstagramPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, InstagramPostItem):
            return
        
        #下载图片地址
        url = item['display_url']
        
        #创建文件夹，拼接真实的目标路径
        path = BASE + item['uid']
        if not os.path.exists(path):
            os.makedirs(path)
        
        file = path + '/' + item['display_res']

        image = requests.get(url)
        f = open(file, 'wb')
        f.write(image.content)
        f.close()
        
        #获取评论信息，为了简单只取一次，时间上由远及近，取300条评论(默认为30条)
        url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item['query_id']) + '&variables={"shortcode":"' + item['shortcode'] +'","first":300}'
        response = requests.get(url)
        json_data = json.loads(response.text)
        data  = json_data['data']
        nodes = data['shortcode_media']['edge_media_to_comment']['edges']
        for node in nodes:
            item_comment = InstagramCommentItem() #评论信息
            item_comment['user'] = node['node']['owner']['username']
            item_comment['uid']  = node['node']['owner']['id']
            item_comment['date'] = node['node']['created_at']
            item_comment['text'] = node['node']['text'].replace(r"'", r"\'")
            item_comment['avatar'] = node['node']['owner']['profile_pic_url']
            
            if not REDIS.hexists(item['id'], item_comment['date']):
                REDIS.hset(item['id'], item_comment['date'], '') #缓存
                get_mysql4comment(item_comment, item) #写数据库
        
        return item
