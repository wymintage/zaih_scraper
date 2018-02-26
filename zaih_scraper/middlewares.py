# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import logging
import datetime
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
import random
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
        ConnectionRefusedError, ConnectionDone, ConnectError, \
        ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed
from scrapy.core.downloader.handlers.http11 import TunnelError
import requests
import os
import zaih_scraper.settings

logger = logging.getLogger(__name__)


class CustomUserAgentMiddleware(UserAgentMiddleware):
    # the default user_agent_list composes chrome,I E,firefox,Mozilla,opera,netscape
    # for more user agent strings,you can find it in http://www.useragentstring.com/pages/useragentstring.php
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
        "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 \
        (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36 OPR/49.0.2725.39",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 \
        (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 \
        (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 "
        "(KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"
    ]

    def __init__(self, user_agent='', *args, **kwargs):
        super(CustomUserAgentMiddleware, self).__init__(*args, **kwargs)
        self.user_agent = user_agent

    def process_request(self, request, spider):
        ua = random.choice(self.user_agent_list)
        if ua:
            request.headers.setdefault('User-Agent', ua)


class ProxiesMiddleware(object):

    def __init__(self, spider):
        self.ip_apis = zaih_scraper.settings.IP_APIS

        self.interval = 8
        self.sleep_time = 2.5

        self.time1 = datetime.datetime.now()
        self.time2 = datetime.datetime.now()

        self.p_remove = zaih_scraper.settings.P_REMOVE

        # 从本地代理ip文件中获取ip，转换为ip列表，并添加到爬虫的代理ip列表中
        if os.path.exists('./zaih_data/Proxies.txt'):
            with open('./zaih_data/Proxies.txt', 'rb') as proxy_file:
                proxies_bytes = proxy_file.read()
                proxies_str = str(proxies_bytes, encoding="utf8")

        proxies_list = proxies_str.split('\r\n')

        try:
            proxies_list.remove('')
        except ValueError:
            pass
        spider.proxies += proxies_list

        print('proxies at middleware:', len(spider.proxies))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def process_request(self, request, spider):

        self.get_proxies(spider)

        if spider.proxies:
            proxy = random.choice(spider.proxies).strip()
        else:
            raise ConnectError

        logger.info('request ip: {}'.format(proxy))
        print('request ip: {}'.format(proxy))

        request.meta['proxy'] = proxy

    def process_response(self, request, response, spider):

        if response.status not in (200, 403, 404):

            self.my_retry(request, spider)

            return request

        return response

    def get_proxies(self, spider):

        self.time2 = datetime.datetime.now()
        interval = self.time2 - self.time1

        if interval.seconds > self.interval:
            logger.info('Proxies not enough, need to be refilled.')
            print('Proxies not enough, need to be refilled.')

            ip_api = random.choice(self.ip_apis)
            ip_requests = requests.request('GET', '{}'.format(ip_api))
            if ip_requests.status_code == 200:
                if '提取太频繁' in ip_requests.text:
                    pass
                else:
                    ip_list = ip_requests.text.split('\r\n')
                    try:
                        ip_list.remove('')
                    except ValueError:
                        pass
                    spider.proxies += ip_list
                self.time1 = self.time2
            print('{0} ip left.'.format(len(spider.proxies)))
            with open('./zaih_data/Proxies.txt', 'wb') as proxy_file:
                # 此处是为了使文本格式与api返回的一致
                proxies_text = '\r\n'.join(spider.proxies)
                proxies_bytes = bytes(proxies_text, encoding="utf8")
                proxy_file.write(proxies_bytes)
        #elif len(spider.proxies) < 50:
        #    spider.proxies += spider.proxies
        else:
            print("Too frequent request, wait a second.")
            #time.sleep(self.sleep_time)

        return spider.proxies

    def my_retry(self, request, spider):

        last_proxy = request.meta['proxy'][2:]
        print(last_proxy, end=' ')
        print(last_proxy in spider.proxies)

        if last_proxy in spider.proxies \
                and random.random() < self.p_remove \
                and len(spider.proxies) > 60:
            spider.proxies.remove(last_proxy)
            print('{0} was removed, {1} ip left.'.format(
                last_proxy.strip(), len(spider.proxies))
            )

        if spider.proxies:
            proxy = random.choice(spider.proxies).strip()
        else:
            raise ConnectError
        print('Request ip: ' + proxy)
        request.meta['proxy'] = proxy

    def process_exception(self, request, exception, spider):

        exceptions_to_retry = (defer.TimeoutError, TimeoutError, DNSLookupError,
                               ConnectionRefusedError, ConnectionDone, ConnectError,
                               ConnectionLost, TCPTimedOutError, ResponseFailed,
                               IOError, TunnelError)

        if isinstance(exception, exceptions_to_retry) \
                and not request.meta.get('dont_retry', False):

            self.my_retry(request, spider)

            return request
