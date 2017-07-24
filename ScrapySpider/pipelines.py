# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import redis
import threading
from ScrapySpider.utils import *
from ScrapySpider.items import *

## 缓存连接
REDIS = redis.Redis(host='127.0.0.1', port=6379, db=0)

def get_mysql4comment(comment, media):
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(comment['date']))
    sql = ur"""INSERT INTO `wp_comments` (`comment_ID`, `comment_post_ID`, `comment_author`, `comment_date`, `comment_date_gmt`, `comment_content`, `user_id`) VALUES ({}, {}, '{}', '{}', '{}', '{}', {});""".format(comment['id'], media['id'], comment['owner_username'], dt, dt, comment['text'], comment['owner_id'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_commentmeta` (`comment_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_avatar', '{}');".format(comment['id'], comment['owner_avatar'])
    logger.info(sql)

## 下载图片并保存到本地
class InstagramPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, InstagramMediaItem):
            return
        
        #下载图片
        media_dl(item['display_url'], item['save_name'], item['owner_id'])
        if item['type'] == 'GraphVideo':#只记录不下载视频
            logger.info('-- Pending media: %s' % (datetime.datetime.today(), item['video_url']))
        
        # Gallery相册处理，下载相册每页数据
        if item['type'] == 'GraphSidecar':
            for node in item['sidecar_edges']:
                media_dl(node['node']['display_url'], node['node']['save_name'], item['owner_id'])
                if node['node']['__typename'] == 'GraphVideo':#只记录不下载视频
                    logger.info('-- Pending media: %s' % (datetime.datetime.today(), node['node']['video_url']))
        
        # Media评论处理，缓存评论数
        if REDIS.hexists('instagram_comments', item['id']) and (int(REDIS.hget('instagram_comment_urls', item['id'])) == item['comment_count']):
            print "%s:pipeline exists (%s) (%s)." % (datetime.datetime.today(), int(REDIS.hget('instagram_comments', item['id'])), item['comment_count'])
            return item
        
        # 已缓存最新32条评论的处理
        i = 0
        for node in item['comment_edges']:
            i += 1
            item_comment = InstagramCommentItem() #评论信息
            item_comment['id']  = node['node']['id']
            item_comment['date'] = node['node']['created_at']
            item_comment['text'] = node['node']['text'].replace(r"'", r"\'")
            item_comment['owner_id'] = node['node']['owner']['id']
            item_comment['owner_username'] = node['node']['owner']['username']
            item_comment['owner_avatar'] = node['node']['owner']['profile_pic_url']
            if i % 2 == 0:#奇偶判定，使用两个线程'并发'处理avatar
                media_dl(item_comment['owner_avatar'])
            else:
                th = threading.Thread(target=media_dl,args=(item_comment['owner_avatar'],))
                th.start()
            
            if not REDIS.hexists(item['id'], item_comment['id']):
                REDIS.hset(item['id'], item_comment['id'], '') #缓存
                get_mysql4comment(item_comment, item) #写数据库
            else:
                item['comment_page_info']['has_next_page'] = False #无需再加载更多评论
                break
        
        # 加载更多评论
        while item['comment_page_info']['has_next_page']:
            end_cursor = item['comment_page_info']['end_cursor']
            
            #获取更多评论信息，为了简单只取一次，时间上由远及近，取300条评论(默认为30条)
            url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item['query_id']) + '&variables={"shortcode":"' + item['shortcode'] +'","first":300,"after":"'+ end_cursor +'"}'
            
            #reactor.callInThread(self.DO_SOME_SYNC_OPERATION, argv) #线程池
            print "%s:pipeline request (%s)." % (datetime.datetime.today(), url)
            response = get_response(url)
            data = response.json()
            item['comment_page_info'] = data['data']['shortcode_media']['edge_media_to_comment']['page_info']
            item['comment_edges'] = data['data']['shortcode_media']['edge_media_to_comment']['edges']
            
            i = 0
            for node in item['comment_edges']:
                i += 1
                item_comment = InstagramCommentItem() #评论信息
                item_comment['id']  = node['node']['id']
                item_comment['date'] = node['node']['created_at']
                item_comment['text'] = node['node']['text'].replace(r"'", r"\'")
                item_comment['owner_id'] = node['node']['owner']['id']
                item_comment['owner_username'] = node['node']['owner']['username']
                item_comment['owner_avatar'] = node['node']['owner']['profile_pic_url']
                if i % 2 == 0:#奇偶判定，使用两个线程'并发'处理avatar
                    media_dl(item_comment['owner_avatar'])
                else:
                    th = threading.Thread(target=media_dl,args=(item_comment['owner_avatar'],))
                    th.start()
                
                if not REDIS.hexists(item['id'], item_comment['id']):
                    REDIS.hset(item['id'], item_comment['id'], '') #缓存
                    get_mysql4comment(item_comment, item) #写数据库
                else:
                    break
            
        #@缓存评论数
        REDIS.hset('instagram_comments', item['id'], item['comment_count'])
        
        return item
