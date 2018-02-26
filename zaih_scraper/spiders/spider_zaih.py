import scrapy
import re
import json
import zaih_scraper.items
import os
import csv
from scrapy import crawler
import scrapy.loader
from scrapy.loader.processors import TakeFirst
import scrapy.settings
import scrapy.selector
import time
import datetime
import logging
import logging.handlers
import pymysql


class SetLogger(object):
    @staticmethod
    def set_logger(name):
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        log_file = './zaih_data/zaih - %s.log' % date

        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=16 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        fmt = '[%(asctime)s][%(levelname)s][%(name)s]: %(message)s'

        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)

        logger = logging.getLogger(name)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger


class ZaihCategorySpider(scrapy.Spider):
    name = 'zaih_index'
    start_urls = [
        'http://www.zaih.com/?city=beijing'
    ]
    custom_settings = {
        'ITEM_PIPELINES': {
            'zaih_scraper.pipelines.MySqlSavePipeline': 110
        }
    }

    def __init__(self, *args, **kwargs):
        super(ZaihCategorySpider, self).__init__(*args, **kwargs)
        self.item = zaih_scraper.items.ZaihCategoryItem()

    def parse(self, response):
        for category in response.xpath('//a[@class=""]'):
            category_link = category.xpath('@href').extract_first()
            if category_link.startswith("/topics"):
                category_id = re.findall('\d.{2}', category_link)
                category_name = category.xpath('text()').extract_first()
                if category_id[0]:
                    yield {
                        'cat_name': category_name,
                        'cat_idx_PK': category_id[0]
                    }


# need to process is-leaf
class ZaihCategoryDetailSpider(scrapy.Spider):
    name = 'zaih_index_detail'

    custom_settings = {
        'ITEM_PIPELINES': {
            'zaih_scraper.pipelines.MySqlProcessIsLeafPipeline': 110,
        }
    }

    def __init__(self, *args, **kwargs):
        super(ZaihCategoryDetailSpider, self).__init__(*args, **kwargs)
        self.category = []
        self.category_idx_li = []
        self.item = zaih_scraper.items.ZaihCategoryDetailItem()

    def process_idx(self):
        # 从分类数据文件读取分类并储存为列表

        with open('./zaih_data/zaih_index.csv', 'r',
                  newline='', encoding='utf-8') as zaih_index:

            lines = csv.DictReader(zaih_index, quoting=csv.QUOTE_ALL)
            for line in lines:
                self.category.append(line)

        # 从列表取出分类并将相应id储存为列表
        for item in self.category:
            self.category_idx_li.append(item['cat_idx_PK'])

    def start_requests(self):
        self.process_idx()
        for idx_num, idx in enumerate(self.category_idx_li):
            yield scrapy.Request(
                'http://www.zaih.com/topics/?category_id={0}&city=beijing'.format(idx)
            )

    def parse(self, response):
        level_1 = None
        level_2 = None
        for category in response.xpath('//a[contains(@data-received, "分类-") \
        and starts-with(@href, "/topics/")]'):

            item = zaih_scraper.items.ZaihCategoryDetailItem()

            cat_name = category.xpath('text()').extract_first()
            cat_idx = category.xpath('@href').re_first('(\d.{2})')
            level = category.xpath('@data-received').re_first('(\d)')

            item['cat_name'] = cat_name
            item['cat_idx_PK'] = cat_idx
            item['level'] = level

            if level == 1:
                level_1 = cat_idx
                item['parent'] = level_1
                item['root'] = level_1
                item['is_leaf'] = 0
            elif level == 2:
                level_2 = cat_idx
                item['parent'] = level_1
                item['root'] = level_1
                item['is_leaf'] = 1
            elif level == 3:
                item['parent'] = level_2
                item['root'] = level_1
                item['is_leaf'] = 1

            yield item


class ZaihCityListSpider(scrapy.Spider):
    name = 'city'

    start_urls = [
        'http://www.zaih.com/topics/?category_id=397&city=beijing'
    ]

    custom_settings = {
        'ITEM_PIPELINES': {
            'zaih_scraper.pipelines.MySqlSavePipeline': 110
        }
    }

    def __init__(self, *args, **kwargs):
        super(ZaihCityListSpider, self).__init__(*args, **kwargs)
        self.item = zaih_scraper.items.ZaihCityItem()

    def parse(self, response):
        city_li = response.xpath('//div[@class="drop-select dropCityWrap"]\
        /ul[@class="drop-select-options"]/li')

        for city_indv in city_li:
            city = city_indv.xpath('a/@data-received').re('to.(.+);')
            if city:
                city = city[0]
            city_link = city_indv.xpath('a/@href').re('city=(\w+)')

            if city_link:
                city_link = city_link[0]
            yield {
                'city_PK': city,
                'city_link': city_link
                         }


class ZaihTopicListSpider(scrapy.Spider):
    name = 'topic_list'

    custom_settings = {
        'ITEM_PIPELINES': {
            'zaih_scraper.pipelines.TopicListPipeline': 130
        },
        'DOWNLOADER_MIDDLEWARES': {
            #'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware':None,
            'zaih_scraper.middlewares.ProxiesMiddleware': 125,
            'zaih_scraper.middlewares.CustomUserAgentMiddleware': 225,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
        }
    }

    def __init__(self, *args, **kwargs):
        super(ZaihTopicListSpider, self).__init__(*args, **kwargs)

        self.cat_list_to_crawl = []
        self.item = {
            'topic': zaih_scraper.items.ZaihTopicItem(),
            'mentor': zaih_scraper.items.ZaihMentorItem()
        }

        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.mylogger = SetLogger.set_logger(self.name)

        self.proxies = []

    def start_requests(self):
        
        self.cursor.execute('USE zaih')

        self.cursor.execute("SELECT city_link_PK, cat_idx_PK, city, cat_name \
                            FROM cat_list_to_crawl WHERE crawled='0' ORDER BY level;")

        self.cat_list_to_crawl = self.cursor.fetchall()

        #yield scrapy.Request('http://www.zaih.com/topics/?category_id=432&city=shenzhen')

        #'''
        for cat_item in self.cat_list_to_crawl:  # 此处是测试变量[:1]

            request = scrapy.Request(
                'http://www.zaih.com/topics/?category_id={0}&city={1}'
                    .format(cat_item['cat_idx_PK'], cat_item['city_link_PK'])
            )
            request.meta['city_link'] = cat_item['city_link_PK']
            request.meta['cat'] = cat_item['cat_idx_PK']
            request.meta['cat_name'] = cat_item['cat_name']
            request.meta['city'] = cat_item['city']
            yield request

        #'''

    def parse(self, response):
        # print('request from %s' %response.url)

        self.mylogger.debug('Parse function called on %s', response.url)
        self.mylogger.debug('request ip :{0}'.format(response.meta['proxy'][2:]))
        self.mylogger.debug('request user-agent :{0}'.format(response.request.headers.get('User-Agent')))

        #i = 0

        topic_list = response.xpath('//li[@class="a-topic"]')

        for topic in topic_list:

            # topic_item
            # ----------------------------------------
            topic_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihTopicItem(),
                topic)
            topic_item.default_output_processor = TakeFirst()

            # topic_link
            topic_link = topic.xpath('./a/@href').extract_first()
            topic_link = 'http://www.zaih.com' + topic_link
            topic_item.add_value('topic_link', topic_link)
            topic_item.add_value('topic_link', 'No data')

            # topic_id_PK
            topic_id_PK = topic.xpath('./a/@data-received').re_first('tID\.(\w+?);')
            topic_item.add_value('topic_id_PK', topic_id_PK)
            topic_item.add_value('topic_id_PK', 'No data')

            # topic_type
            # ----------------------------------------
            #topic_item.add_value('topic_type', 'Not scraped yet')
            # ----------------------------------------

            # topic_name
            topic_name = topic.xpath('./a/@data-received').re_first('tName\.(.+?);')
            topic_item.add_value('topic_name', topic_name)
            topic_item.add_value('topic_name', 'No data')

            #

            # mentor_id
            mentor_id = topic.xpath('./a/@data-received').re_first('mID\.(.+?);')
            topic_item.add_value('mentor_id', mentor_id)
            topic_item.add_value('mentor_id', 'No data')

            # mentor_name
            mentor_name = topic.xpath('.//span[@class="topic-tutor-name"]/text()')\
                .extract_first().strip()
            topic_item.add_value('mentor_name', mentor_name)
            topic_item.add_value('mentor_name', 'No data')

            # mentor_title
            mentor_title = topic.xpath('.//span[@class="topic-tutor-intro"]/text()')\
                .extract_first().strip()
            topic_item.add_value('mentor_title', mentor_title)
            topic_item.add_value('mentor_title', 'No data')

            # mentor_image
            mentor_image = topic.xpath('.//span[@class="topic-tutor-pic"]/@style')\
                .re_first('url\((.+?)\)')
            topic_item.add_value('mentor_image', mentor_image)
            topic_item.add_value('mentor_image', 'No data')

            #

            # cat_name
            cat_name = topic.xpath('./a/@data-received').re_first('tag\.(.+?);')
            topic_item.add_value('cat_name', cat_name)
            topic_item.add_value('cat_name', 'No data')

            # cat_idx
            cat_idx = scrapy.selector.Selector(text=response.url).re_first('(\d.{2})')
            topic_item.add_value('cat_idx', cat_idx)
            topic_item.add_value('cat_idx', 'No data')

            # other_cat
            topic_item.add_value('other_cat', 'No other cat')

            # city_link
            topic_item.add_value('city_link', response.meta['city_link'])
            topic_item.add_value('city_link', 'No data')

            # city
            topic_item.add_value('city', response.meta['city'])
            topic_item.add_value('city', 'No data')

            #

            # rating
            rating = topic.xpath('.//div[@class="rating withRatingTip"]/text()').extract_first()
            if rating:
                topic_item.add_value('rating', rating.strip())
            topic_item.add_value('rating', 'No rating')

            # price
            price = topic.xpath('.//div[starts-with(@class, "price")]/em/text()')\
                .re_first('(\d+)')
            topic_item.add_value('price', price)
            topic_item.add_value('price', 'No data')

            #

            # meet_num_topic
            # ----------------------------------------
            #topic_item.add_value('meet_num_topic', 'Not scraped yet')
            # ----------------------------------------

            # meet_time
            # ----------------------------------------
            #topic_item.add_value('meet_time', 'Not scraped yet')
            # ----------------------------------------

            #

            # topic_intro_LONG
            # ----------------------------------------
            #topic_item.add_value('topic_intro_LONG', 'Not scraped yet')
            # ----------------------------------------

            # comments_count_topic
            # ----------------------------------------
            #topic_item.add_value('comments_count_topic', 'Not scraped yet')
            # ----------------------------------------

            #

            # published_date
            # ----------------------------------------
            published_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            topic_item.add_value('published_date', published_date)
            topic_item.add_value('published_date', 'No data')
            # ----------------------------------------

            #
            #

            topic_item.load_item()
            # ----------------------------------------

            #
            #=========================================
            #

            # mentor_item
            # ----------------------------------------
            mentor_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihMentorItem(),
                topic)
            mentor_item.default_output_processor = TakeFirst()

            # mentor_link
            mentor_link = topic.xpath('./a/@data-received').re_first('mID\.(.+?);')
            mentor_link = 'http://www.zaih.com/mentor/{0}/'.format(mentor_link)
            mentor_item.add_value('mentor_link', mentor_link)
            mentor_item.add_value('mentor_link', 'No data')

            # mentor_id_PK
            mentor_id_PK = topic.xpath('./a/@data-received').re_first('mID\.(.+?);')
            mentor_item.add_value('mentor_id_PK', mentor_id_PK)
            mentor_item.add_value('mentor_id_PK', 'No data')

            # updated_time
            # ----------------------------------------
            pass
            # ----------------------------------------

            # mentor_name
            mentor_name = topic.xpath('.//span[@class="topic-tutor-name"]/text()')\
                .extract_first()
            mentor_item.add_value('mentor_name', mentor_name)
            mentor_item.add_value('mentor_name', 'No data')

            #

            # mentor_image
            mentor_image = topic.xpath('.//span[@class="topic-tutor-pic"]/@style')\
                .re_first('url\((.+?)\)')
            mentor_item.add_value('mentor_image', mentor_image)
            mentor_item.add_value('mentor_image', 'No data')

            #

            # mentor_title
            mentor_title = topic.xpath('.//span[@class="topic-tutor-intro"]/text()')\
                .extract_first()
            if mentor_title:
                mentor_title = mentor_title.strip()
            mentor_item.add_value('mentor_title', mentor_title)
            mentor_item.add_value('mentor_title', 'No data')

            # mentor_intro_LONG
            # ----------------------------------------
            #mentor_item.add_value('mentor_intro_LONG', 'Not scraped yet')
            # ----------------------------------------

            #

            # respond_time
            # ----------------------------------------
            #mentor_item.add_value('respond_time', 'Not scraped yet')
            # ----------------------------------------

            # meet_num_total
            # ----------------------------------------
            #mentor_item.add_value('meet_num_online', 'Not scraped yet')
            # ----------------------------------------

            # meet_num_online
            # ----------------------------------------
            #mentor_item.add_value('meet_num_online', 'Not scraped yet')
            # ----------------------------------------

            # heart
            # ----------------------------------------
            #mentor_item.add_value('heart', 'Not scraped yet')
            # ----------------------------------------

            # accept_rate
            # ----------------------------------------
            #mentor_item.add_value('accept_rate', 'Not scraped yet')
            # ----------------------------------------

            #

            # city
            mentor_item.add_value('city', response.meta['city'])
            mentor_item.add_value('city', 'No data')

            # location
            # ----------------------------------------
            #mentor_item.add_value('location', 'Not scraped yet')
            # ----------------------------------------

            #

            # comments_count_total
            # ----------------------------------------
            #mentor_item.add_value('comments_count_total', 'Not scraped yet')
            # ----------------------------------------

            mentor_item.load_item()

            #i += 1
            #if i < 2:
            yield {'topic': topic_item.item, 'mentor': mentor_item.item}

        if response.xpath('//li[a/span/@class="icon icon-next"]'):
            next_page = response.xpath('//li/a[span/@class="icon icon-next"]/@href')\
                .extract_first()
            yield response.follow(next_page, callback=self.parse, meta=response.meta)
            #pass
        else:
            self.cursor.execute(
                '''
                UPDATE cat_list_to_crawl
                SET crawled = '1'
                WHERE cat_idx_PK = '{0}' AND city_link_PK = '{1}';
                '''.format(response.meta['cat'], response.meta['city_link'])
            )
            self.connect.commit()


class ZaihMentorSpider(scrapy.Spider):
    name = 'mentor'

    custom_settings = {
        'ITEM_PIPELINES': {
            'zaih_scraper.pipelines.MentorMysqlPipeline': 130,
        },
        'DOWNLOADER_MIDDLEWARES': {
            #'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware':None,
            #'zaih_scraper.middlewares.ProxiesMiddleware': 125,
            'zaih_scraper.middlewares.CustomUserAgentMiddleware': 225,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
        }
    }

    def __init__(self, *args, **kwargs):
        super(ZaihMentorSpider, self).__init__(*args, **kwargs)
        self.mentor_list_dict = []

        self.item = {
            'topic': zaih_scraper.items.ZaihTopicItem(),
            'mentor': zaih_scraper.items.ZaihMentorItem(),
            'recommend': zaih_scraper.items.ZaihRecommendItem(),
            'comment': zaih_scraper.items.ZaihCommentItem(),
            'user':  zaih_scraper.items.ZaihUserItem()
        }

        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.mylogger = SetLogger.set_logger(self.name)

        self.proxies = []

    def start_requests(self):

        self.cursor.execute('USE zaih')

        #self.cursor.execute('SELECT mentor_id_PK,updated_time FROM mentor;')
        self.cursor.execute("SELECT mentor_id_PK,updated_time FROM mentor \
                            WHERE updated_time LIKE '2018-01-05%';")
        self.mentor_list_dict = self.cursor.fetchall()
        self.mentor_list_dict = self.mentor_list_dict
        #print(len(self.mentor_list))
        count = 0

        for mentor_item_dict in self.mentor_list_dict:
            mentor_id = mentor_item_dict['mentor_id_PK']
            time_last = mentor_item_dict['updated_time']
            time_last = datetime.datetime.strptime(time_last, '%Y-%m-%d %H:%M:%S')

            count += 1

            request = scrapy.Request("http://www.zaih.com/mentor/{0}/".format(mentor_id))
            request.meta['mentor_id'] = mentor_id

            time_now = datetime.datetime.now()

            print('complete : {:%} {}/{}'.format(
                count/len(self.mentor_list_dict), count, len(self.mentor_list_dict))
            )

            interval = time_now - time_last
            if interval.days > 30:
                request.meta['time_now'] = time_now.strftime('%Y-%m-%d %H:%M:%S')
                yield request
            else:
                print("Don't need to update.")

    def awdstart_requests(self):
        self.mentor_list = ['85213836']

        for mentor_id in self.mentor_list:
            request = scrapy.Request("http://www.zaih.com/mentor/{0}/".format(mentor_id))
            request.meta['mentor_id'] = mentor_id
            request.meta['time_now'] = None
            yield request

    def parse(self, response):

        mentor_items = []
        topic_items = []
        recommend_items = []

        self.mylogger.debug('Parse function called on %s', response.url)
        # 测试
        #self.mylogger.debug('request ip :{0}'.format(response.meta['proxy'][2:]))
        #self.mylogger.debug('request user-agent :{0}'.format(response.request.headers.get('User-Agent')))

        # mentor部分
        # mentor part

        mentor_item = scrapy.loader.ItemLoader(
            zaih_scraper.items.ZaihMentorItem(),
            response)
        mentor_item.default_output_processor = TakeFirst()

        # mentor_link

        # mentor_id_PK
        mentor_id_PK = response.meta['mentor_id']
        mentor_item.add_value('mentor_id_PK', mentor_id_PK)
        mentor_item.add_value('mentor_id_PK', 'No data')

        # updated_time
        if response.meta['time_now']:
            updated_time = response.meta['time_now']
        else:
            updated_time = '2017-01-01 00:00:00'
        mentor_item.add_value('updated_time', updated_time)

        # mentor_name

        #

        # mentor_image

        #

        # mentor_title  # introduction

        # mentor_intro_LONG  # summary
        '''
        intro_sep = response.xpath('//div[@class="about-tutor-summary"]\
        /div//text()|//div[@class="about-tutor-summary"]/div//img').extract()
        '''
        intro_sep = response.xpath('//div[@class="about-tutor-summary"]\
                /div').xpath('.//text()|.//img/@src').extract()
        mentor_intro_LONG = ''.join(intro_sep)
        mentor_item.add_value('mentor_intro_LONG', mentor_intro_LONG)
        mentor_item.add_value('mentor_intro_LONG', 'No data')

        #

        # respond_time
        respond_time = response.xpath('//span[@class="icon icon-clock-solid"]\
        /following-sibling::span[1]/text()').extract_first()
        mentor_item.add_value('respond_time', respond_time)
        mentor_item.add_value('respond_time', 'No data')

        # meet_num_total
        meet_num_total = response.xpath('//span[@class="icon icon-meet"]\
        /following-sibling::span[1]/text()').extract_first()
        mentor_item.add_value('meet_num_total', meet_num_total)
        mentor_item.add_value('meet_num_total', 'No data')

        #meet_num_online
        meet_num_online = response.xpath('//span[@class="icon icon-online"]\
        /following-sibling::span[1]/text()').extract_first()
        mentor_item.add_value('meet_num_online', meet_num_online)
        mentor_item.add_value('meet_num_online', 'No data')

        #heart
        heart = response.xpath('//span[@class="icon icon-heart"]\
        /following-sibling::span[1]/text()').extract_first()
        mentor_item.add_value('heart', heart)
        mentor_item.add_value('heart', 'No data')

        #accept_rate
        accept_rate = response.xpath('//li[starts-with(text(), "接受率")]\
        /span/text()').extract_first()
        mentor_item.add_value('accept_rate', accept_rate)
        mentor_item.add_value('accept_rate', 'No data')

        #

        # city = scrapy.Field()

        #location
        location = response.xpath('//div[@class="tutor-info"]\
        /p[@class="tutor-title"]/span[@class="location"]/text()').extract_first()
        if location:
            location = location.strip()
        mentor_item.add_value('location', location)
        mentor_item.add_value('location', "No data")

        #

        #comments_count_total
        comments_count_total = response.xpath('//div[@class="comments" \
        and @data-total-count]/@data-total-count').extract_first()
        mentor_item.add_value('comments_count_total', comments_count_total)
        mentor_item.add_value('comments_count_total', 0)

        #
        #

        mentor_item.load_item()
        mentor_items.append(mentor_item)

        #
        #==================================================
        #

        # topic部分
        # topic part

        topic_list = response.xpath('//ul[@class="topic-list"]/li')

        for topic in topic_list:
            topic_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihTopicItem(),
                topic)
            topic_item.default_output_processor = TakeFirst()

            # topic_link

            # topic_id_PK
            topic_id_PK = topic.xpath('h2/@data-topic_id').extract_first()
            topic_item.add_value('topic_id_PK', topic_id_PK)
            topic_item.add_value('topic_id_PK', 'No data')

            # topic_type
            topic_type = topic.xpath('div[@class="topic-tag topicTag"]/text()')\
                .extract_first()
            topic_item.add_value('topic_type', topic_type)
            topic_item.add_value('topic_type', 'Normal')

            # topic_name

            #

            # mentor_id

            # mentor_name

            # mentor_title

            # mentor_image

            #

            # cat_name  # tag

            # cat_idx

            # other_cat

            # city_link

            # city

            #

            # rating

            # price

            #

            # meet_num_topic
            meet_num_topic = topic.xpath('.//span[@class="meet-num"]/text()')\
                .re_first('(\d+)')
            topic_item.add_value('meet_num_topic', meet_num_topic)
            topic_item.add_value('meet_num_topic', 'No data')

            # meet_time
            meet_time = topic.xpath('.//span[@class="meet-time"]/text()')\
                .re_first('([\d\.]+)')
            topic_item.add_value('meet_time', meet_time)
            topic_item.add_value('meet_time', 'No data')

            #

            # topic_intro_LONG
            topic_intro_LONG_sep = topic.xpath(
                './/div[@class="topic-introduction topicIntroduction"]\
                /div[1]').xpath('.//text()|.//img/@src').extract()
            topic_intro_LONG = ''.join(topic_intro_LONG_sep)
            topic_item.add_value('topic_intro_LONG', topic_intro_LONG)
            topic_item.add_value('topic_intro_LONG', 'No data')

            # comments_count_topic

            #

            # published_date

            #
            #

            topic_item.load_item()
            topic_items.append(topic_item)

            #
            #==================================================
            #

        # recommend部分
        # recommend part

        recommend_list = response.xpath('//ul[@class="tutor-topic-list"]/li')
        for recommend in recommend_list:
            recommend_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihRecommendItem(),
                recommend)
            recommend_item.default_output_processor = TakeFirst()

            #recommend_from_m_id_PK
            recommend_from_m_id = recommend.xpath('./a/@data-received')\
                .re_first('from\.(.+?);')
            recommend_item.add_value('recommend_from_m_id_PK', recommend_from_m_id)
            recommend_item.add_value('recommend_from_m_id_PK', 'No data')

            #recommend_from_m_name
            recommend_from_m_name = recommend.xpath('./a/@data-received')\
                .re_first('fName\.(.+?);')
            recommend_item.add_value('recommend_from_m_name', recommend_from_m_name)
            recommend_item.add_value('recommend_from_m_name', 'No data')

            #

            #recommend_target_m_id_PK
            recommend_target_m_id = recommend.xpath('./a/@data-received')\
                .re_first('target\.(.+?);')
            recommend_item.add_value('recommend_target_m_id_PK', recommend_target_m_id)
            recommend_item.add_value('recommend_target_m_id_PK', 'No data')

            #recommend_target_m_name
            recommend_target_m_name = recommend.xpath('./a/@data-received')\
                .re_first('tName\.(.+?);')
            recommend_item.add_value('recommend_target_m_name', recommend_target_m_name)
            recommend_item.add_value('recommend_target_m_name', 'No data')

            #

            #recommend_target_t_name
            recommend_target_t_name = recommend.xpath('.//p[@class="topic-title"]/text()')\
                .extract_first()
            if recommend_target_t_name:
                recommend_target_t_name = recommend_target_t_name.strip()
            recommend_item.add_value('recommend_target_t_name', recommend_target_t_name)
            recommend_item.add_value('recommend_target_t_name', 'No data')

            #

            #recommend_target_city
            recommend_target_city = recommend.xpath('.//span[@class="location"]/text()')\
                .extract_first()
            if recommend_target_city:
                recommend_target_city = recommend_target_city.strip()
            recommend_item.add_value('recommend_target_city', recommend_target_city)
            recommend_item.add_value('recommend_target_city', 'No data')

            #

            #recommend_discription_LONG
            recommend_discription_LONG_sep = recommend.xpath(
                './/div[@class="discription"]')\
                .xpath('.//text()|.//img/@src').extract()
            recommend_discription_LONG = ''.join(recommend_discription_LONG_sep)
            recommend_item.add_value('recommend_discription_LONG', recommend_discription_LONG)
            recommend_item.add_value('recommend_discription_LONG', 'No data')

            #
            #

            recommend_item.load_item()
            recommend_items.append(recommend_item)

            #
            #==================================================
            #

        # 保存到目前为止的所有item

        item = {'mentor': mentor_items, 'topic': topic_items, 'recommend': recommend_items}

        # comments部分
        # comments part

        #动态获取所有comments

        offset = 0
        limit = 20

        request = scrapy.Request(
            'http://www.zaih.com/apis/open'
            '/topic_reviews?tutor_id={0}&offset={1}&limit={2}&sort_by=likings_count'
            .format(response.meta['mentor_id'], offset, limit),
            callback=self.parse_comment)
        request.meta['comments_count_total'] = comments_count_total

        request.meta['item'] = item

        request.meta['offset'] = offset
        request.meta['limit'] = limit

        request.meta['mentor_id'] = response.meta['mentor_id']

        yield request

    "http://www.zaih.com/apis/open/topic_reviews?tutor_id=84764709&offset=0&limit=20&sort_by=likings_count"

    def parse_comment(self, response):

        self.mylogger.debug(
            'Parse comments {0} - {1} of mentor: {2} .'.format(
                response.meta['offset'],
                response.meta['offset'] + response.meta['limit'],
                response.meta['mentor_id']
            )
        )

        comment_items = []
        user_items = []

        topic_not_processed = []

        self.mylogger.debug('Parse function called on %s', response.url)
        #测试
        #self.mylogger.debug('request ip :{0}'.format(response.meta['proxy'][2:]))

        for topic_item in response.meta['item']['topic']:
            topic_not_processed.append(topic_item.item['topic_id_PK'])

        comment_list_text = response.body_as_unicode()[response.body_as_unicode().find('['):]
        comment_list = json.loads(comment_list_text)

        for comment in comment_list:

            comment_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihCommentItem(),
                response)
            comment_item.default_output_processor = TakeFirst()

            #comment_user_nick_name
            comment_user_nick_name = comment['user']['nickname']
            comment_item.add_value('comment_user_nick_name', comment_user_nick_name)
            comment_item.add_value('comment_user_nick_name', 'No data')

            #comment_user_real_name
            comment_user_real_name = comment['user']['realname']
            comment_item.add_value('comment_user_real_name', comment_user_real_name)
            comment_item.add_value('comment_user_real_name', 'No data')

            #comment_user_id
            comment_user_id = comment['user_id']
            comment_item.add_value('comment_user_id', comment_user_id)
            comment_item.add_value('comment_user_id', 'No data')

            #

            #comment_id_PK
            comment_id_PK = comment['id']
            comment_item.add_value('comment_id_PK', comment_id_PK)
            comment_item.add_value('comment_id_PK', 'No data')

            #comment_content_LONG
            comment_content_LONG = comment['content']
            comment_item.add_value('comment_content_LONG', comment_content_LONG)
            comment_item.add_value('comment_content_LONG', 'No data')

            #

            #comment_topic_id
            comment_topic_id = comment['topic_id']
            comment_item.add_value('comment_topic_id', comment_topic_id)
            comment_item.add_value('comment_topic_id', 'No data')

            #comment_mentor_id
            comment_mentor_id = response.meta['mentor_id']
            comment_item.add_value('comment_mentor_id', comment_mentor_id)
            comment_item.add_value('comment_mentor_id', 'No data')

            #

            #comment_date
            comment_date = comment['date_created']
            comment_item.add_value('comment_date', comment_date)
            comment_item.add_value('comment_date', 'No data')

            #comment_heart
            comment_heart = comment['likings_count']
            comment_item.add_value('comment_heart', comment_heart)
            comment_item.add_value('comment_heart', 'No data')

            #

            #have_reply
            have_reply = bool(comment['reply'])
            comment_item.add_value('have_reply', have_reply)
            comment_item.add_value('have_reply', 'No data')

            #comment_reply_LONG
            comment_reply_LONG = comment['reply']
            comment_item.add_value('comment_reply_LONG', comment_reply_LONG)
            comment_item.add_value('comment_reply_LONG', 'No data')

            #comment_reply_date
            comment_reply_date = comment['date_replied']
            comment_item.add_value('comment_reply_date', comment_reply_date)
            comment_item.add_value('comment_reply_date', 'No data')

            #

            #comment_order_id
            comment_order_id = comment['order_id']
            comment_item.add_value('comment_order_id', comment_order_id)
            comment_item.add_value('comment_order_id', 'No data')

            #
            #

            comment_item.load_item()
            comment_items.append(comment_item)

            #
            #==================================================
            #

            # topic部分
            # topic part

            #comments_count_topic
            if topic_not_processed:
                if comment_topic_id in topic_not_processed:
                    for topic_item in response.meta['item']['topic']:
                        if topic_item.item['topic_id_PK'] == comment_topic_id:
                            topic_item.add_value('comments_count_topic', comment['topic']['reviews_count'])
                            topic_item.add_value('comments_count_topic', 'No data')

                            topic_item.load_item()

                            topic_not_processed.remove(comment_topic_id)

            else:
                pass

            #
            #==================================================
            #

            # user部分
            # user part
            user_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihUserItem(),
                response)
            user_item.default_output_processor = TakeFirst()

            #comment_user_nick_name
            comment_user_nick_name = comment['user']['nickname']
            user_item.add_value('comment_user_nick_name', comment_user_nick_name)
            user_item.add_value('comment_user_nick_name', 'No data')

            #comment_user_real_name
            comment_user_real_name = comment['user']['realname']
            user_item.add_value('comment_user_real_name', comment_user_real_name)
            user_item.add_value('comment_user_real_name', 'No data')

            #

            #comment_user_id_PK
            comment_user_id_PK = comment['user']['id']
            user_item.add_value('comment_user_id_PK', comment_user_id_PK)
            user_item.add_value('comment_user_id_PK', 'No data')

            #

            #comment_user_image
            comment_user_image = comment['user']['avatar']
            user_item.add_value('comment_user_image', comment_user_image)
            user_item.add_value('comment_user_image', 'No data')

            #

            #comment_user_location
            comment_user_location = comment['user']['location']
            user_item.add_value('comment_user_location', comment_user_location)
            user_item.add_value('comment_user_location', 'No data')

            #

            #comment_user_title
            comment_user_title = comment['user']['title']
            user_item.add_value('comment_user_title', comment_user_title)
            user_item.add_value('comment_user_title', 'No data')

            #comment_user_is_mentor
            comment_user_is_mentor = comment['user']['is_tutor']
            user_item.add_value('comment_user_is_mentor', comment_user_is_mentor)
            user_item.add_value('comment_user_is_mentor', 'No data')

            #

            #comment_user_followers_count
            comment_user_followers_count = comment['user']['followers_count']
            user_item.add_value('comment_user_followers_count', comment_user_followers_count)
            user_item.add_value('comment_user_followers_count', 'No data')

            #

            #comment_user_industry
            comment_user_industry = comment['user']['industry']
            user_item.add_value('comment_user_industry', comment_user_industry)
            user_item.add_value('comment_user_industry', 'No data')

            #

            #comment_user_labels
            comment_user_labels = comment['user']['labels']
            user_item.add_value('comment_user_labels', comment_user_labels)
            user_item.add_value('comment_user_labels', 'No data')

            #comment_user_label
            comment_user_label = comment['user_label']
            user_item.add_value('comment_user_label', comment_user_label)
            user_item.add_value('comment_user_label', 'No data')

            #
            #

            user_item.load_item()
            user_items.append(user_item)

            #
            #==================================================
            #

        # 若未获取所有comment，则请求剩余的comment
        # 若所有comment都已获取，则返回每个mentor页面的item

        comments_count_total = 0
        if response.meta['comments_count_total']:
            comments_count_total = response.meta['comments_count_total']

        offset = response.meta['offset']
        limit = response.meta['limit']

        if offset < int(comments_count_total):

            if offset == 0:
                response.meta['item']['comment'] = comment_items
                response.meta['item']['user'] = user_items
            else:
                response.meta['item']['comment'] += comment_items
                response.meta['item']['user'] += user_items

            offset += limit

            request = scrapy.Request(
                'http://www.zaih.com/apis/open'
                '/topic_reviews?tutor_id={0}&offset={1}&limit={2}&sort_by=likings_count'
                .format(response.meta['mentor_id'], offset, limit),
                callback=self.parse_comment)

            request.meta['comments_count_total'] = comments_count_total

            request.meta['item'] = response.meta['item']

            request.meta['offset'] = offset
            request.meta['limit'] = limit

            request.meta['mentor_id'] = response.meta['mentor_id']

            if offset < int(comments_count_total):

                yield request

            else:
                '''
                for item_k, item_v in response.meta['item'].items():
                    print(item_k, 'Number : ', len(item_v))
                    for item in item_v:
                        print(item.item)
                '''
                # 测试

                yield response.meta['item']

        else:
            yield response.meta['item']


class ZaihTopicSupSpider(scrapy.Spider):
    name = 'topic_sup'

    custom_settings = {
        'ITEM_PIPELINES': {
            #'zaih_scraper.pipelines.CSVWriterPipeline' : 101,
            #'zaih_scraper.pipelines.MySqlSavePipeline': 110,
            #'zaih_scraper.pipelines.ItemTestPipeline': 120,
            #'zaih_scraper.pipelines.TopicListPipeline': 130,
            'zaih_scraper.pipelines.TopicSupPipeline': 130,
        },
        'DOWNLOADER_MIDDLEWARES': {
            #'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware':None,
            #  'zaih_scraper.middlewares.ProxiesMiddleware': 125,
            'zaih_scraper.middlewares.CustomUserAgentMiddleware': 225,
            #'zaih_scraper.middlewares.CountMiddleware': 250,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
        }
    }

    def __init__(self, *args, **kwargs):

        super(ZaihTopicSupSpider, self).__init__(*args, **kwargs)

        self.topic_to_sup = []

        self.city = []
        self.city_link_dict = {}

        self.item = {'topic': zaih_scraper.items.ZaihTopicItem()}

        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.mylogger = SetLogger.set_logger(self.name)

        self.proxies = []

    def start_requests(self):

        self.cursor.execute('USE zaih')
        self.cursor.execute("SELECT topic_id_PK FROM topic\
                            WHERE price = 'None';")
        self.topic_to_sup = self.cursor.fetchall()
        print(len(self.topic_to_sup))

        self.cursor.execute('SELECT city_PK, city_link FROM city_list;')
        cities = self.cursor.fetchall()
        for city_item in cities:
            self.city.append(city_item['city_PK'])
            self.city_link_dict[city_item['city_PK']] = city_item['city_link']

        for tid_dict in self.topic_to_sup:
            request = scrapy.Request("http://www.zaih.com/topic/{0}/".format(tid_dict['topic_id_PK']))
            request.meta['topic_id_PK'] = tid_dict['topic_id_PK']
            yield request

    def parse(self, response):

        try:
            topic = response.xpath('//li[contains(@class, "topic-item")][h2[@data-topic_id={}]]'
                                   .format(response.meta['topic_id_PK'])
                                   )[0]

            # topic_item
            # ----------------------------------------
            topic_item = scrapy.loader.ItemLoader(
                zaih_scraper.items.ZaihTopicItem(),
                topic)
            topic_item.default_output_processor = TakeFirst()

            # topic_link
            topic_link = response.url
            topic_item.add_value('topic_link', topic_link)
            topic_item.add_value('topic_link', 'No data')

            # topic_id_PK
            topic_id_PK = response.meta['topic_id_PK']
            topic_item.add_value('topic_id_PK', topic_id_PK)
            topic_item.add_value('topic_id_PK', 'No data')

            # topic_type
            topic_type = topic.xpath('div[@class="topic-tag topicTag"]/text()')\
                .extract_first()
            topic_item.add_value('topic_type', topic_type)
            topic_item.add_value('topic_type', 'Normal')

            # topic_name
            topic_name = topic.xpath('./h2[@class="topicTitle"]/text()').extract_first()
            topic_item.add_value('topic_name', topic_name)
            topic_item.add_value('topic_name', 'No data')

            #

            # mentor_id
            mentor_id = response.xpath('.//h1[@class="tutor-name"]/@data-tutor_id').extract_first()
            topic_item.add_value('mentor_id', mentor_id)
            topic_item.add_value('mentor_id', 'No data')

            # mentor_name
            mentor_name = response.xpath('.//h1[@class="tutor-name"]/text()').extract_first().strip()
            topic_item.add_value('mentor_name', mentor_name)
            topic_item.add_value('mentor_name', 'No data')

            # mentor_title
            mentor_title = response.xpath('.//p[@class="tutor-title"]/text()') \
                .extract_first().strip()
            topic_item.add_value('mentor_title', mentor_title)
            topic_item.add_value('mentor_title', 'No data')

            # mentor_image
            mentor_image = response.xpath('.//span[@class="tutor-avatar tutorAvatar"]/img/@src').extract_first()
            topic_item.add_value('mentor_image', mentor_image)
            topic_item.add_value('mentor_image', 'No data')

            #

            # city
            city = response.xpath('.//span[@class="city"]/text()').extract_first().strip()
            topic_item.add_value('city', city)
            topic_item.add_value('city', 'No data')

            # city_link
            try:
                topic_item.add_value('city_link', self.city_link_dict[city])
            except KeyError:
                topic_item.add_value('city_link', 'No data')

            #

            # rating
            rating = topic.xpath('.//div[@class="rating withRatingTip"]/text()').extract_first()
            if rating:
                topic_item.add_value('rating', rating.strip())
            topic_item.add_value('rating', 'No rating')

            # price
            price = topic.xpath('.//span[@class="price"]/text()').extract_first().strip()
            topic_item.add_value('price', price)
            topic_item.add_value('price', 'No data')

            #

            # meet_num_topic
            meet_num_topic = topic.xpath('.//span[@class="meet-num"]/text()')\
                .re_first('(\d+)')
            topic_item.add_value('meet_num_topic', meet_num_topic)
            topic_item.add_value('meet_num_topic', 'No data')
            # ----------------------------------------
            # alerady_exists
            # ----------------------------------------

            # meet_time
            meet_time = topic.xpath('.//span[@class="meet-time"]/text()')\
                .re_first('([\d\.]+)')
            topic_item.add_value('meet_time', meet_time)
            topic_item.add_value('meet_time', 'No data')
            # ----------------------------------------
            # alerady_exists
            # ----------------------------------------

            #

            # topic_intro_LONG
            topic_intro_LONG_sep = topic.xpath(
                './/div[@class="topic-introduction topicIntroduction"]\
                /div[1]').xpath('.//text()|.//img/@src').extract()
            topic_intro_LONG = ''.join(topic_intro_LONG_sep)
            topic_item.add_value('topic_intro_LONG', topic_intro_LONG)
            topic_item.add_value('topic_intro_LONG', 'No data')
            # ----------------------------------------
            # alerady_exists
            # ----------------------------------------

            # comments_count_topic
            # ----------------------------------------
            # alerady_exists
            # ----------------------------------------

            #

            # published_date
            # ----------------------------------------
            published_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            topic_item.add_value('published_date', published_date)
            topic_item.add_value('published_date', 'No data')
            # ----------------------------------------

            #
            #

            topic_item.load_item()
            # ----------------------------------------

            yield topic_item.item

        except IndexError:
            print('topic_not_exist')


# scrapy.settings.default_settings.DOWNLOADER_MIDDLEWARES_BASE

#process = scrapy.crawler.CrawlerProcess()
#process.crawl(ZaihTopicListSpider)
#process.start()
