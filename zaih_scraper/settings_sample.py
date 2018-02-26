# 请将本文件重命名为 seetings.py


# -*- coding: utf-8 -*-

# Scrapy settings for zaih_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'zaih_scraper'

SPIDER_MODULES = ['zaih_scraper.spiders']
NEWSPIDER_MODULE = 'zaih_scraper.spiders'

# FEED_EXPORT_ENCODING = 'utf-8'


#ITEM_PIPELINES = {
#    'zaih_scraper.pipelines.JsonWriterPipeline': 800,
#}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'zaih_scraper (+http://www.yourdomain.com)'
#USER_AGENT = 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 320
#CONCURRENT_ITEMS = 1280

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 128
#CONCURRENT_REQUESTS_PER_IP = 128

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'zaih_scraper.middlewares.ZaihScraperSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'zaih_scraper.middlewares.MyCustomDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'zaih_scraper.pipelines.ZaihScraperPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

#HTTPCACHE_POLICY = 'scrapy.extensions.httpcache.DummyPolicy'

DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

#DOWNLOAD_TIMEOUT = 10

DUPEFILTER_DEBUG = True


IP_APIS = [
            "一些请求代理ip的api，如果不想使用代理ip，将每个spider中的'zaih_scraper.middlewares.ProxiesMiddleware'注释掉"
        ]

# 当一个代理ip请求失败时，从代理池中清除此ip的概率		
P_REMOVE = 0.2


CONNECT_INFO = {'host': 'localhost',
                'port': 3306,
                'user': '[youruser]',
                'passwd': '[yourpwd]',
                'charset': 'utf8mb4'}
