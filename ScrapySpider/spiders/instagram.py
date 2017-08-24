# -*- coding: utf-8 -*-

import re
import json
import urllib
from scrapy import Spider, Request
from ScrapySpider.utils import *
from ScrapySpider.items import *

## 断点续传开关-爬取一个新用户的过程中网络中断导致需要继续爬取
RESUME_BROKEN = False

## 增量爬取开关-爬取一个旧用户自上次爬取后的更新数据
INCREMENT_SWITCH = False

def get_mysql4user(user):
    if RESUME_BROKEN:#续传爬行，不添加用户信息
        return
    if INCREMENT_SWITCH:#增量爬取，不写用户表，只更新用户扩展表
        sql = u"UPDATE `wp_usermeta` SET `meta_value` = '{}' WHERE `wp_usermeta`.`user_id` = {};".format(user['media_count'], user['id'])
        logger.info(sql)
        return
    sql = u"INSERT INTO `wp_users` (`ID`, `user_login`, `user_pass`, `user_nicename`, `user_email`, `user_registered`, `user_activation_key`, `user_status`, `display_name`) VALUES ({}, '{}', '$P$Bv4tMmzqpt9nBWKAdo7FUMUajheklN0', '{}', '{}@hotlinks.org', '1999-09-09 09:09:09', '', 0, '{}');".format(user['id'], user['username'], user['username'], user['username'],  user['full_name'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'nickname', '{}');".format(user['id'], user['username'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'wp_capabilities', '{}');".format(user['id'], 'a:1:{s:6:\"author\";b:1;}')
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'wp_user_level', '2');".format(user['id'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_avatar', '{}');".format(user['id'], user['avatar'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_followed', '{}');".format(user['id'], user['followed_count'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_follows', '{}');".format(user['id'], user['follows_count'])
    logger.info(sql)
    sql = u"INSERT INTO `wp_usermeta` (`user_id`, `meta_key`, `meta_value`) VALUES ({}, 'si_medias', '{}');".format(user['id'], user['media_count'])
    logger.info(sql)

def get_mysql4media(media, user):
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(media['date']))
    #拼接Content
    content = u''
    if media['type'] == 'GraphImage':
        image_src = u'{}/{}/{}'.format(media['save_domain'], user['id'], media['save_name'])
        content += u'<a class="wplightbox" data-group="wpgallery" data-thumbnail="{}" href="{}" title="image"><img class="scale-with-grid alignnone" src="{}"/></a>'.format(image_src, image_src, image_src)
    elif media['type'] == 'GraphVideo':
        image_src = u'{}/{}/{}'.format(media['save_domain'], user['id'], media['save_name'])
        content += u'<a class="wplightbox" data-group="wpgallery" data-thumbnail="{}" href="{}" title="video"><video class="owl-video" controls loop preload="none" poster="{}" src="{}" type="video/mp4"/></a>'.format(image_src, media['video_url'], image_src, media['video_url'])
    elif media['type'] == 'GraphSidecar':
        for node in media['sidecar_edges']:
            if node['node']['__typename'] == 'GraphImage':
                image_src = u'{}/{}/{}'.format(node['node']['save_domain'], user['id'], node['node']['save_name'])
                content += u'<a class="wplightbox" data-group="wpgallery" data-thumbnail="{}" href="{}" title="image"><img class="scale-with-grid alignnone" src="{}"/></a>'.format(image_src, image_src, image_src)
            elif node['node']['__typename'] == 'GraphVideo':
                image_src = u'{}/{}/{}'.format(node['node']['save_domain'], user['id'], node['node']['save_name'])
                content += u'<a class="wplightbox" data-group="wpgallery" data-thumbnail="{}" href="{}" title="video"><video class="owl-video" controls loop preload="none" poster="{}" src="{}" type="video/mp4"/></a>'.format(image_src, node['node']['video_url'], image_src, node['node']['video_url'])
            else:
                print "%s:type error (%s)." % (datetime.datetime.today(), node['node']['__typename'])
    else:
        print "%s:type error (%s)." % (datetime.datetime.today(), media['type'])
    #NOTE:: json.dumps 会转码字串，会将'couldn\'t'替换为'couldn\\'t',其它转为unicode编码如'\u3084\n'
    media2 = InstagramMediaItem(media)
    media2['comment_page_info'] = scrapy.Field()
    media2['comment_edges'] = scrapy.Field()
    #NOTE:: location的name需转码单引号, sidecar_edges不含caption无需处理
    try:
        media2['location']['name'] = media2['location']['name'].replace(r"'", r"\'")#"name": "Val d' Orcia"
    except Exception:
        pass
    sql = ur"""INSERT INTO `wp_posts` (`ID`, `post_author`, `post_date`, `post_date_gmt`, `post_content`, `post_title`, `post_excerpt`, `post_status`, `comment_status`, `ping_status`, `post_password`, `post_name`, `to_ping`, `pinged`, `post_modified`, `post_modified_gmt`, `post_content_filtered`, `post_parent`, `guid`, `menu_order`, `post_type`, `post_mime_type`, `comment_count`) VALUES ({}, {}, '{}', '{}', '{}', '{}', '{}', 'publish', 'open', 'open', '', '{}', '', '', '{}', '{}', '', 0, 'http://hotlinks.org/?p={}', 0, '{}', '', {});""".format(media['id'], user['id'], dt, dt, content, media['caption'], json.dumps(dict(media2.items())).replace("\\'", "\'"), media['shortcode'], dt, dt, media['id'], 'post', media['comment_count'])
    logger.info(sql)
    
    #NOTE:: post-format-video[1], post-format-gallery[2], post-format-image[0]. Uncategorized is 0 by default
    if media['type'] == 'GraphVideo':
        sql = u"INSERT INTO `wp_term_relationships` (`object_id`, `term_taxonomy_id`, `term_order`) VALUES ({}, {}, 0);".format(media['id'], 1)
        logger.info(sql)
    elif media['type'] == 'GraphSidecar':
        sql = u"INSERT INTO `wp_term_relationships` (`object_id`, `term_taxonomy_id`, `term_order`) VALUES ({}, {}, 0);".format(media['id'], 2)
        logger.info(sql)

class InstagramSpider(Spider):
    name = 'instagram'

    def start_requests(self):
        logger.info('-- Generation Time: %s' % (datetime.datetime.today()))
        
        urls = [
            #@Japan
            #'https://www.instagram.com/cocoannne/',
            #'https://www.instagram.com/maggymoon/',             #hot
            #'https://www.instagram.com/rolaofficial/',
            #'https://www.instagram.com/nyanchan22/',            #warm
            #'https://www.instagram.com/moeka_nozaki/',
            #'https://www.instagram.com/saekoofficial/',
            #@Australia
            #'https://www.instagram.com/tuulavintage/',
            #@Korea
            #'https://www.instagram.com/chuustagram/',
            #'https://www.instagram.com/kimjeongyeon__/',
            #'https://www.instagram.com/seul__p/',
            #'https://www.instagram.com/sora_pppp/',
            #photographer
            #'https://www.instagram.com/brahmino',#travel
            #'https://www.instagram.com/iwwm',#scenery, sight
            'https://www.instagram.com/benjaminheath',#scenery, sight
        ]
        for url in urls:
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, callback=self.parse)
    
    def parse(self, response):
        """首页处理：获取用户信息和当前显示的文章信息
        """
        print "%s:parse (%s)." % (datetime.datetime.today(), response.url)
        
        item_user = InstagramUserItem() #用户信息
        
        try:
            #获取动态配置，用来构造自动加载更多内容的请求时，查询ID是必带参数
            url = 'https://www.instagram.com' + ''.join(response.xpath('//script[contains(@src, "Commons.js")]/@src').extract())
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            javascript = get_response(url)
            javascript = javascript.text
            #{pageSize:u,pagesToPreload:0,getState:function(e,t){return e.profilePosts.byUserId.get(t).pagination},queryId:"17888483320059182",queryParams:...
            pattern = re.compile(r'e\.profilePosts\.byUserId\.get(.*?)\.pagination\},queryId:"(\d+)"', re.S)
            item_user['query_id'] = re.search(pattern, javascript).group(2) #文章查询ID
            #{pageSize:l,pagesToPreload:1,getState:function(e,t){return e.comments.byPostId.get(t).pagination},queryId:"17852405266163336",queryParams:...
            pattern = re.compile(r'e\.comments\.byPostId\.get(.*?)\.pagination\},queryId:"(\d+)"', re.S)
            item_user['query_id2'] = re.search(pattern, javascript).group(2) #评论查询ID
            print "%s:queryId:(%s, %s)." % (datetime.datetime.today(), item_user['query_id'], item_user['query_id2'])
        except Exception as e:
            print "%s:%s" % (datetime.datetime.today(), e)
            return
        
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
        #缓存avatar，放在单独的线程中处理，不影响主线程的处理速度
        REDIS.lpush('instagram_avatars', item_user['avatar'])
        thread_avatar = threading.Thread(target=avatar_dl)
        thread_avatar.start()#@独立线程启动
        item_user['followed_count'] = data['user']['followed_by']['count']
        item_user['follows_count'] = data['user']['follows']['count']
        item_user['media_count'] = data['user']['media']['count']
        
        self.log('parse user %s' % item_user["full_name"])
        get_mysql4user(item_user) #写数据库
        
        #更新最新媒体ID
        item_user['latest_id'] = '0'
        item_user['latest_ct'] = 0
        if REDIS.hexists('instagram_latest', item_user['username']):
            item_user['latest_id'] = REDIS.hget('instagram_latest', item_user['username'])
        
        #前11个不作为更新的内容，等待其点赞和评论基本停顿后更新
        latest = get_extracted(data['user']['media']['nodes'], -1)
        REDIS.hset('instagram_latest', item_user['username'], latest['id'])
        logger.info('-- User (%s) Latest ID (%s)' % (item_user['username'], latest['id']))
        
        for node in data['user']['media']['nodes']:
            if RESUME_BROKEN:
                continue
            #最新的11个不写SQL
            item_user['latest_ct'] += 1
            if item_user['latest_ct'] < 12:
                continue
            
            item_media = InstagramMediaItem() #文章信息
            item_media['type'] = node['__typename']
            item_media['id'] = node['id']
            item_media['owner_id'] = node['owner']['id']
            try:
                item_media['caption'] = node['caption'].replace(r"'", r"\'") if node.has_key('caption') else ''
            except Exception:
                item_media['caption'] = ''
            item_media['date'] = node['date']
            item_media['like_count'] = node['likes']['count']
            item_media['comment_count'] = node['comments']['count']
            item_media['shortcode'] = node['code']
            item_media['query_id'] = item_user['query_id2']
            item_media['thumbnail_url'] = node['thumbnail_src']
            item_media['display_url'] = node['display_src']
            item_media['save_domain'] = DOMAIN
            item_media['save_name'] = os.path.basename(item_media['display_url'])
            item_media['width'] = node['dimensions']['width']
            item_media['height'] = node['dimensions']['height']
            item_media['is_video'] = node['is_video']
            ##获取更多
            self.parse_single(item_media, item_user)
            
            self.log('parse media %s' % item_media['display_url'])
            if item_user['latest_id'] == item_media['id']:
                print "%s:parse stoped by hexists latest." % (datetime.datetime.today())
                return
            
            if REDIS.hexists('instagram_medias', item_media['id']):
                print "%s:parse stoped by hexists medias." % (datetime.datetime.today())
                return
            else:
                REDIS.hset('instagram_medias', item_media['id'], item_media['date']) #缓存
                get_mysql4media(item_media, item_user) #写数据库
            
            #触发 PIPE
            yield item_media
        
        #触发请求：自动加载更多内容
        page = data['user']['media']['page_info']
        if page['has_next_page']:
            url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"'+ page['end_cursor'] +'"}'
            ##
            if RESUME_BROKEN:
                url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"AQA3O-kSthe4Zq7QEI-6PDkPM5PkyCkTw6X0XiT9xrSsXdig18ewWT_PpyKJxBVh783EmWKwJdD7dNKiiye6TsYUyzag-J-KA_USerqYcnVt2g"}'
                REDIS.hdel('instagram_urls', urllib.quote(url, ":/?=&,"))
            
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, meta={'item':item_user}, callback=self.parse2)
        
        print "%s:parse end." % datetime.datetime.today()
    
    def parse2(self, response):
        """加载更多文章的处理：数据格式和parse稍有不同，区别对待
        """
        if REDIS.hexists('config', 'stop'):
            print "%s:parse2 stoped by redis stop signale." % (datetime.datetime.today())
            return
        
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
            item_media = InstagramMediaItem() #文章信息
            item_media['type'] = node['node']['__typename']
            item_media['id'] = node['node']['id']
            item_media['owner_id'] = node['node']['owner']['id']
            try:
                item_media['caption'] = node['node']['edge_media_to_caption']['edges'][0]['node']['text']
            except KeyError:
                item_media['caption'] = ''
            except IndexError:
                item_media['caption'] = ''
            item_media['caption'] = item_media['caption'].replace(r"'", r"\'")
            item_media['date'] = node['node']['taken_at_timestamp']
            try:
                item_media['like_count'] = node['node']['edge_liked_by']['count']
            except KeyError:
                item_media['like_count'] = node['node']['edge_media_preview_like']['count']
            item_media['comment_count'] = node['node']['edge_media_to_comment']['count']
            item_media['shortcode'] = node['node']['shortcode']
            item_media['query_id'] = item_user['query_id2']
            item_media['thumbnail_url'] = node['node']['thumbnail_src']
            item_media['width'] = node['node']['dimensions']['width']
            item_media['display_url'] = node['node']['display_url']
            item_media['save_domain'] = DOMAIN
            item_media['save_name'] = os.path.basename(item_media['display_url'])
            item_media['width'] = node['node']['dimensions']['width']
            item_media['height'] = node['node']['dimensions']['height']
            item_media['is_video'] = node['node']['is_video']
            ##获取更多
            self.parse_single(item_media, item_user)
            
            self.log('parse media %s' % item_media['display_url'])
            if item_user['latest_id'] == item_media['id']:
                print "%s:parse stoped by hexists latest." % (datetime.datetime.today())
                return
            
            if REDIS.hexists('instagram_medias', item_media['id']):
                print "%s:parse2 stoped by hexists medias." % (datetime.datetime.today())
                return
            else:
                REDIS.hset('instagram_medias', item_media['id'], item_media['date']) #缓存
                get_mysql4media(item_media, item_user) #写数据库
                
            #触发 PIPE
            yield item_media
        
        #自动加载更多内容
        page = data['user']['edge_owner_to_timeline_media']['page_info']
        if page['has_next_page']:
            url = 'https://www.instagram.com/graphql/query/?query_id=' + str(item_user['query_id']) + '&variables={"id":"' + item_user['id'] +'","first":12,"after":"'+ page['end_cursor'] +'"}'
            print "%s:request (%s)." % (datetime.datetime.today(), url)
            yield Request(url=url, meta={'item':item_user}, callback=self.parse2)
        
        print "%s:parse2 end." % datetime.datetime.today()
    
    def parse_single(self, media, user):
        """更多数据的收集
        """
        url = 'https://www.instagram.com/p/' + media['shortcode'] + '/?taken-by=' + user['username'] +'&__a=1'
        print "%s:request (%s)." % (datetime.datetime.today(), url)
        response = get_response(url)
        data = response.json()
        graphql_media = data['graphql']['shortcode_media']
        
        #保持地理位置位置、最新评论、相册集等数据
        media['location'] = graphql_media['location']
        media['comment_page_info'] = graphql_media['edge_media_to_comment']['page_info']
        media['comment_edges'] = graphql_media['edge_media_to_comment']['edges']
        
        if media['type'] == 'GraphImage':
            pass
        elif media['type'] == 'GraphVideo':
            media['video_url']  = graphql_media['video_url']
            media['video_view_count']  = graphql_media['video_view_count']
        elif media['type'] == 'GraphSidecar':
            media['sidecar_edges'] = graphql_media['edge_sidecar_to_children']['edges']
            for node in media['sidecar_edges']:
                node['node']['save_domain'] = DOMAIN
                node['node']['save_name'] = os.path.basename(node['node']['display_url'])
        else:
            pass