# -*- coding: utf-8 -*-

import re
import json
import redis
import urllib
import logging
import threading
from scrapy import Spider, Request
from ScrapySpider.utils import *
from ScrapySpider.items import *

## sql写文件
logging.basicConfig(
    filename='spider.sql',
    format='%(message)s',
    level=logging.INFO
)

## 缓存连接
REDIS = redis.Redis(host='127.0.0.1', port=6379, db=0)

## 断点续传开关
RESUME_BROKEN = False

def get_mysql4user(user):
    sql = u"INSERT INTO `wp_users` (`ID`, `user_login`, `user_pass`, `user_nicename`, `user_email`, `user_registered`, `user_activation_key`, `user_status`, `display_name`) VALUES ({}, '{}', '$P$Bv4tMmzqpt9nBWKAdo7FUMUajheklN0', '{}', '{}@hotlinks.org', '1999-09-09 09:09:09', '', 0, '{}');".format(user['id'], user['username'], user['username'], user['username'],  user['full_name'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'nickname', '{}');".format(user['id'], user['username'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'wp_capabilities', '{}');".format(user['id'], 'a:1:{s:6:\"author\";b:1;}')
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'wp_user_level', '2');".format(user['id'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_avatar', '{}');".format(user['id'], user['avatar'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_followed', '{}');".format(user['id'], user['followed_count'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_follows', '{}');".format(user['id'], user['follows_count'])
    logging.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_posts', '{}');".format(user['id'], user['media_count'])
    logging.info(sql)

def get_mysql4post(post, user):
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(post['date']))
    #拼接Content
    post_content = u''
    if post['type'] == 'GraphImage':
        image_src = u'{}/{}/{}'.format(post['save_domain'], user['id'], post['save_name'])
        post_content += u'<a href="{}" data-rel="lightbox[folio]"><img class="scale-with-grid alignnone" src="{}" alt=""/></a>'.format(image_src, image_src)
    elif post['type'] == 'GraphVideo':
        image_src = u'{}/{}/{}'.format(post['save_domain'], user['id'], post['save_name'])
        post_content += u'<a href="{}" data-rel="lightbox[folio]"><video class="scale-with-grid alignnone" playsinline="" poster="{}" src="{}" preload="none" type="video/mp4"/></a>'.format(image_src, image_src, post['video_url'])
    elif post['type'] == 'GraphSidecar':
        for node in post['sidecar_edges']:
            if node['node']['__typename'] == 'GraphImage':
                image_src = u'{}/{}/{}'.format(node['node']['save_domain'], user['id'], node['node']['save_name'])
                post_content += u'<a href="{}" data-rel="lightbox[folio]"><img class="scale-with-grid alignnone" src="{}" alt=""/></a>'.format(image_src, image_src)
            elif node['node']['__typename'] == 'GraphVideo':
                image_src = u'{}/{}/{}'.format(node['node']['save_domain'], user['id'], node['node']['save_name'])
                post_content += u'<a href="{}" data-rel="lightbox[folio]"><video class="scale-with-grid alignnone" playsinline="" poster="{}" src="{}" preload="none" type="video/mp4"/></a>'.format(image_src, image_src, node['node']['video_url'])
            else:
                print "%s:type error (%s)." % (datetime.datetime.today(), node['node']['__typename'])
    else:
        print "%s:type error (%s)." % (datetime.datetime.today(), post['type'])
    #NOTE:: json.dumps 会转码字串，会将'couldn\'t'替换为'couldn\\'t',其它转为unicode编码如'\u3084\n'
    post2 = InstagramMediaItem(post)
    post2['comment_page_info'] = scrapy.Field()
    post2['comment_edges'] = scrapy.Field()
    sql = ur"""INSERT INTO `wp_posts` (`ID`, `post_author`, `post_date`, `post_date_gmt`, `post_content`, `post_title`, `post_excerpt`, `post_status`, `comment_status`, `ping_status`, `post_password`, `post_name`, `to_ping`, `pinged`, `post_modified`, `post_modified_gmt`, `post_content_filtered`, `post_parent`, `guid`, `menu_order`, `post_type`, `post_mime_type`, `comment_count`) VALUES ({}, {}, '{}', '{}', '{}', '{}', '{}', 'publish', 'open', 'open', '', '{}', '', '', '{}', '{}', '', 0, 'http://hotlinks.org/?p={}', 0, '{}', '', {});""".format(post['id'], user['id'], dt, dt, post_content, post['caption'], json.dumps(dict(post2.items())).replace("\\'", "\'"), post['shortcode'], dt, dt, post['id'], 'post', post['comment_count'])
    logging.info(sql)
    
    #NOTE:: post-format-video[1], post-format-gallery[2], post-format-image[0]. Uncategorized is 0 by default
    if post['type'] == 'GraphVideo':
        sql = u"INSERT INTO `wp_term_relationships` (`object_id`, `term_taxonomy_id`, `term_order`) VALUES ({}, {}, 0);".format(post['id'], 1)
        logging.info(sql)
    elif post['type'] == 'GraphSidecar':
        sql = u"INSERT INTO `wp_term_relationships` (`object_id`, `term_taxonomy_id`, `term_order`) VALUES ({}, {}, 0);".format(post['id'], 2)
        logging.info(sql)

class InstagramSpider(Spider):
    name = 'instagram'

    def start_requests(self):
        #追写评论，对上次爬取过的最新12篇文章再次获取评论信息
        self.rescrapys = 12
        logging.info('-- Generation Time: %s' % (datetime.datetime.today()))
        
        urls = [
            #'https://www.instagram.com/moeka_nozaki/',        #39516-1
            #'https://www.instagram.com/cocoannne/',           #173008-1        #hot
            #'https://www.instagram.com/maggymoon/',           #263325-1        #hot
            'https://www.instagram.com/saekoofficial/',
        ]
        for url in urls:
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, callback=self.parse)
    
    def parse(self, response):
        """首页处理：获取用户信息和当前显示的文章信息
        """
        print "%s:parse (%s)." % (datetime.datetime.today(), response.url)
         
        item_user = InstagramUserItem() #用户信息
        
        #获取动态配置，用来构造自动加载更多内容的请求时，查询ID是必带参数
        url = 'https://www.instagram.com' + ''.join(response.xpath('//script[contains(@src, "Commons.js")]/@src').extract())
        print "%s:request (%s)." % (datetime.datetime.today(), url)
        javascript = get_response(url)
        javascript = javascript.text
        pattern = re.compile(r'PROFILE_POSTS_UPDATED(.*?)queryId:"(\d+)"', re.S)
        item_user['query_id'] = re.search(pattern, javascript).group(2) #文章查询ID
         
        pattern = re.compile(r'COMMENT_REQUEST_UPDATED(.*?)queryId:"(\d+)"', re.S)
        item_user['query_id2'] = re.search(pattern, javascript).group(2) #评论查询ID
        
        #获取内容，当前页面的图片获取主逻辑
        javascript = ''.join(response.xpath('//script[contains(text(), "sharedData")]/text()').extract())
        json_data = json.loads(''.join(re.findall(r'window._sharedData = (.*);', javascript)))
        
        data = get_extracted(json_data['entry_data']['ProfilePage'])
        item_user['id'] = data['user']['id']
        item_user['is_private'] = data['user']['is_private']
        item_user['is_verified'] = data['user']['is_verified']
        item_user['username'] = data['user']['username']
        item_user['full_name'] = data['user']['full_name'].replace(r"'", r"\'") if data['user']['full_name'] else ''
        item_user['avatar'] = data['user']['profile_pic_url']
        th = threading.Thread(target=media_dl,args=(item_user['avatar'],))
        th.start()
        item_user['followed_count'] = data['user']['followed_by']['count']
        item_user['follows_count'] = data['user']['follows']['count']
        item_user['media_count'] = data['user']['media']['count']
        
        self.log('parse user %s' % item_user["full_name"])
        get_mysql4user(item_user) #写数据库
        
        for node in data['user']['media']['nodes']:
            item_post = InstagramMediaItem() #文章信息
            item_post['type'] = node['__typename']
            item_post['id'] = node['id']
            item_post['owner_id'] = node['owner']['id']
            try:
                item_post['caption'] = node['caption'].replace(r"'", r"\'") if node.has_key('caption') else ''
            except Exception:
                item_post['caption'] = ''
            item_post['date'] = node['date']
            item_post['like_count'] = node['likes']['count']
            item_post['comment_count'] = node['comments']['count']
            item_post['shortcode'] = node['code']
            item_post['query_id'] = item_user['query_id2']
            item_post['thumbnail_url'] = node['thumbnail_src']
            item_post['display_url'] = node['display_src']
            item_post['save_domain'] = DOMAIN
            item_post['save_name'] = os.path.basename(item_post['display_url'])
            item_post['width'] = node['dimensions']['width']
            item_post['height'] = node['dimensions']['height']
            item_post['is_video'] = node['is_video']
            
            self.log('parse post %s' % item_post['display_url'])
            if REDIS.hexists('instagram_posts', item_post['id']):
                self.rescrapys -= 1 #已缓存，倒数12个退出爬取
                if (self.rescrapys <= 0):
                    if RESUME_BROKEN:
                        pass
                    else:
                        return
            else:
                REDIS.hset('instagram_posts', item_post['id'], item_post['date']) #缓存
                self.parse_single(item_post, item_user)
                get_mysql4post(item_post, item_user) #写数据库
            
            #触发 PIPE
            print "%s:item (%s)." % (datetime.datetime.today(), item_post['id'])
            yield item_post
        
        #触发请求：自动加载更多内容
        page = data['user']['media']['page_info']
        if page['has_next_page']:
            url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"'+ page['end_cursor'] +'"}'
            ##
            if RESUME_BROKEN:
                url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"AQCXB7EBzOA7ojIomTrr0Cofs3OuzmlbpzTGDDOQg7V5e-JrG41x1xGKlLv7nsYQhn715FDqGPx79ldOu0oASxkSAPmvz-Ib19Jv2LhldNGsoA"}'
                REDIS.hdel('instagram_urls', urllib.quote(url, ":/?=&,"))
            
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, meta={'item':item_user}, callback=self.parse2)
        
        print "%s:parse end." % datetime.datetime.today()
    
    def parse2(self, response):
        """加载更多文章的处理：数据格式和parse稍有不同，区别对待
        """
        print "%s:parse2 (%s)." % (datetime.datetime.today(), response.url)
        
        item_user = response.meta['item']
        
        json_data = json.loads(response.body)
        data  = json_data['data']
        nodes = data['user']['edge_owner_to_timeline_media']['edges']
        
        #@缓存请求链接的过滤，以减轻压力和防止被屏蔽
        if REDIS.hexists('instagram_urls', response.url):
            #自动加载更多内容
            page = data['user']['edge_owner_to_timeline_media']['page_info']
            if page['has_next_page']:
                url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"'+ page['end_cursor'] +'"}'
                print "%s:request (%s)." % (datetime.datetime.today(), url)
                yield Request(url=url, meta={'item':item_user}, callback=self.parse2)
            return
        
        #@缓存请求链接
        REDIS.hset('instagram_urls', response.url, '')
        
        self.log('parse2 user %s...' % item_user["full_name"])
        
        for node in nodes:
            item_post = InstagramMediaItem() #文章信息
            item_post['type'] = node['node']['__typename']
            item_post['id'] = node['node']['id']
            item_post['owner_id'] = node['node']['owner']['id']
            try:
                item_post['caption'] = node['node']['edge_media_to_caption']['edges'][0]['node']['text']
            except KeyError:
                item_post['caption'] = ''
            except IndexError:
                item_post['caption'] = ''
            item_post['caption'] = item_post['caption'].replace(r"'", r"\'")
            item_post['date'] = node['node']['taken_at_timestamp']
            try:
                item_post['like_count'] = node['node']['edge_liked_by']['count']
            except KeyError:
                item_post['like_count'] = node['node']['edge_media_preview_like']['count']
            item_post['comment_count'] = node['node']['edge_media_to_comment']['count']
            item_post['shortcode'] = node['node']['shortcode']
            item_post['query_id'] = item_user['query_id2']
            item_post['thumbnail_url'] = node['node']['thumbnail_src']
            item_post['width'] = node['node']['dimensions']['width']
            item_post['display_url'] = node['node']['display_url']
            item_post['save_domain'] = DOMAIN
            item_post['save_name'] = os.path.basename(item_post['display_url'])
            item_post['width'] = node['node']['dimensions']['width']
            item_post['height'] = node['node']['dimensions']['height']
            item_post['is_video'] = node['node']['is_video']
            
            self.log('parse post %s' % item_post['display_url'])
            if REDIS.hexists('instagram_posts', item_post['id']):
                self.rescrapys -= 1
                if (self.rescrapys <= 0):
                    if RESUME_BROKEN:
                        pass
                    else:
                        return
            else:
                REDIS.hset('instagram_posts', item_post['id'], item_post['date']) #缓存
                self.parse_single(item_post, item_user)
                get_mysql4post(item_post, item_user) #写数据库
                
            #触发 PIPE
            print "%s:item (%s)." % (datetime.datetime.today(), item_post['id'])
            yield item_post
        
        #自动加载更多内容
        page = data['user']['edge_owner_to_timeline_media']['page_info']
        if page['has_next_page']:
            url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"'+ page['end_cursor'] +'"}'
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, meta={'item':item_user}, callback=self.parse2)
        
        print "%s:parse2 end." % datetime.datetime.today()
    
    def parse_single(self, post, user):
        """更多数据的收集
        """
        url = 'https://www.instagram.com/p/' + post['shortcode'] + '/?taken-by=' + user['username'] +'&__a=1'
        print "%s:request (%s)." % (datetime.datetime.today(), url)
        response = get_response(url)
        data = response.json()
        media  = data['graphql']['shortcode_media']
        
        #保持地理位置位置、最新评论、相册集等数据
        post['location'] = media['location']
        post['comment_page_info'] = media['edge_media_to_comment']['page_info']
        post['comment_edges'] = media['edge_media_to_comment']['edges']
        
        if post['type'] == 'GraphImage':
            pass
        elif post['type'] == 'GraphVideo':
            post['video_url']  = media['video_url']
            post['video_view_count']  = media['video_view_count']
        elif post['type'] == 'GraphSidecar':
            post['sidecar_edges'] = media['edge_sidecar_to_children']['edges']
            for node in post['sidecar_edges']:
                node['node']['save_domain'] = DOMAIN
                node['node']['save_name'] = os.path.basename(node['node']['display_url'])
        else:
            pass