# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class InstagramUserItem(scrapy.Item):
    #用户信息
    id                  = scrapy.Field() #用户ID
    is_private          = scrapy.Field()
    is_verified         = scrapy.Field()
    username            = scrapy.Field() #登录名, moeka_nozaki
    full_name           = scrapy.Field() #全名, "\u91ce\u5d0e\u840c\u9999"=野崎萌香 
    avatar              = scrapy.Field() #头像
    followed_count      = scrapy.Field() #F计数
    follows_count       = scrapy.Field() #F计数
    media_count         = scrapy.Field() #M计数
    #动态获取
    query_id            = scrapy.Field() #查询更多文章使用
    query_id2           = scrapy.Field() #查询更多评论使用<暂存传递给POST的query_id>

class InstagramMediaItem(scrapy.Item):
    #图片信息
    type                = scrapy.Field() #__typename<GraphImage/GraphSidecar/GraphVideo>
    id                  = scrapy.Field() #文章ID
    owner_id            = scrapy.Field() #用户ID<转存供本地保存路径使用>
    caption             = scrapy.Field() #标题
    date                = scrapy.Field() #日期
    like_count          = scrapy.Field() #喜欢计数
    comment_count       = scrapy.Field() #评论计数
    comment_page_info   = scrapy.Field() #评论分页信息<Dict>
    comment_edges       = scrapy.Field() #评论集缓存<List>
    shortcode           = scrapy.Field() #短码，查询评论使用
    query_id            = scrapy.Field() #查询更多评论使用
    #媒体信息
    thumbnail_url       = scrapy.Field() #图片-缩略图地址
    display_url         = scrapy.Field() #图片-原图地址
    save_domain         = scrapy.Field() #图片-存储域
    save_name           = scrapy.Field() #图片-存储路径
    width               = scrapy.Field() #尺寸，图片/视频相同
    height              = scrapy.Field() #尺寸，...
    is_video            = scrapy.Field() #视频？
    video_url           = scrapy.Field() #视频地址
    video_view_count    = scrapy.Field() #视频浏览次数
    ##扩展支持Gallay类型的Post
    location            = scrapy.Field() #地理位置<Dict>
    sidecar_edges       = scrapy.Field() #相册集缓存<List>

class InstagramCommentItem(scrapy.Item):
    #评论信息
    id                  = scrapy.Field() #文章ID
    date                = scrapy.Field() #评论时间
    text                = scrapy.Field() #评论内容
    owner_id            = scrapy.Field() #评论用户ID
    owner_username      = scrapy.Field() #评论用户
    owner_avatar        = scrapy.Field() #头像
    
