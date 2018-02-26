import scrapy
import scrapy.cmdline
import sys
import os


def run_a_spider(spider_name):
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    scrapy.cmdline.execute(['scrapy', 'crawl', spider_name])


#process_index_attr_is_leaf()


spider_names = {1: 'zaih_index',
                2: 'zaih_index_detail',
                3: 'city',
                4: 'topic_list',
                5: 'item_test',
                6: 'ip_test',
                7: 'xicidaili',
                8: 'mentor',
                9: 'topic_sup'
                }

run_a_spider(spider_names[8])

