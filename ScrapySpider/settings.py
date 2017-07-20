# -*- coding: utf-8 -*-

# Scrapy settings for ScrapySpider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'ScrapySpider'

SPIDER_MODULES = ['ScrapySpider.spiders']
NEWSPIDER_MODULE = 'ScrapySpider.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'ScrapySpider (+http://www.yourdomain.com)'

#定制选项
LOG_FILE = 'spider.log'
#USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0"

#是否遵循机器人规则
ROBOTSTXT_OBEY = False
#默认是16，一次可以请求的最大次数
CONCURRENT_REQUESTS = 16
#下载延迟
DOWNLOAD_DELAY = 0.5
#启用缓存
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 28800
#Cookies设置
COOKIES_ENABLED = True
#headers设置
DEFAULT_REQUEST_HEADERS = {
'Accept':'*/*',
'Accept-Encoding':'gzip, deflate, sdch, br',
'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Cache-Control':'max-age=0',
'Connection':'keep-alive',
'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'}

#管道设置
ITEM_PIPELINES = {'ScrapySpider.pipelines.InstagramPipeline': 300}