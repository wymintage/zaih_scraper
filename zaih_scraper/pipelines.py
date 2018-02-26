# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import csv
import pymysql
import logging
import logging.handlers
import time
import datetime
import zaih_scraper.settings


class SetLogger(object):
    @staticmethod
    def set_logger(name):
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        log_file = './zaih_data/zaih - %s.log' % date

        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=16 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8',
        )
        fmt = '[%(asctime)s][%(levelname)s][%(name)s]: %(message)s'

        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)

        logger = logging.getLogger(name)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger


class CSVProcessIsLeafPipeline(object):

    def __init__(self, spider):
        self.level_2_not_leaf = []
        self.items = []
        self.items_new = []
        self.i = 0
        self.fieldnames = []

        if os.path.exists('./zaih_data'):
            pass
        else:
            os.mkdir('./zaih_data')
        self.file = open('./zaih_data/%s.csv' % spider.name,
                         'w',
                         newline='',
                         encoding='utf-8')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def process_item(self, item):
        self.fieldnames = dict(item).keys()

        if item['level'] == 3 \
                and (item['parent'] not in self.level_2_not_leaf):
            self.level_2_not_leaf.append(item['parent'])

        self.items.append(item)

        return item

    def close_spider(self, spider):

        for sec_item in self.items:
            if sec_item['level'] == 2 and \
                    (sec_item['cat_idx_PK'] in self.level_2_not_leaf):
                sec_item['is_leaf'] = 0
            self.items_new.append(sec_item)

        line = csv.DictWriter(
            self.file,
            fieldnames=self.fieldnames,
            quoting=csv.QUOTE_ALL
        )
        line.writeheader()
        line.writerows(self.items_new)

        self.file.close()


class MySqlSavePipeline(object):

    def __init__(self, spider):
        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.logger = SetLogger.set_logger(spider.name)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def open_spider(self, spider):

        try:
            self.cursor.execute('USE zaih')
        except pymysql.err.InternalError:
            self.cursor.execute('CREATE DATABASE zaih')
            self.cursor.execute('USE zaih')

        table = ''
        primary_key = ''
        for k in spider.item.fields:
            table += "{0} VARCHAR(255) COLLATE utf8_bin NOT NULL DEFAULT 'None',\n"\
                .format(k)
            if '_PK' in k:
                primary_key = 'PRIMARY KEY ({0})'.format(k)
        if primary_key:
            table += primary_key
        else:
            raise Exception('Primary key not found.')

        # 不设主键时的创建table语句
        '''
        for i, k in enumerate(spider.item.fields):
            if i < (len(spider.item.fields) - 1):
                table += '{0} VARCHAR(255) COLLATE utf8_bin NOT NULL,\n'.format(k)
            elif i == (len(spider.item.fields) - 1):
                table += '{0} VARCHAR(255) COLLATE utf8_bin NOT NULL'.format(k)
            '''

        create_table = '''
        CREATE TABLE {0} (
        {1}
        )
        '''.format(spider.name, table)

        try:
            self.cursor.execute(create_table)
        except pymysql.err.InternalError:
            # table已创建，跳过此语句
            pass

    def process_item(self, item, spider):

        try:
            ks = ''
            vs = ''
            for k, v in item.items():
                ks += '{0}, '.format(k)
                vs += "'{0}', ".format(v)
            ks = ks[:-2]
            vs = vs[:-2]

            inserter = '''
            INSERT INTO {0} 
            ({1})
            VALUES
            ({2})
            '''.format(spider.name, ks, vs)
            self.cursor.execute(inserter)
            self.connect.commit()

            #self.logger.info(inserter)
            self.logger.info('data saved successfully, {0}'.format(dict(item)))

            return item

        except pymysql.err.IntegrityError as err:

            self.logger.error(err)
            self.logger.error('error in {0}'.format(dict(item)))
            return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()


class MySqlProcessIsLeafPipeline(object):

    def __init__(self, spider):
        self.level_2_not_leaf = []
        self.items = []
        self.items_new = []
        self.i = 0
        self.fieldname = []
        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.logger = SetLogger.set_logger(spider.name)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def open_spider(self, spider):

        # 选择数据库zaih
        try:
            self.cursor.execute('USE zaih')
        except pymysql.err.InternalError:
            self.cursor.execute('CREATE DATABASE zaih')
            self.cursor.execute('USE zaih')

        # 初始化table建立语句
        table = ''
        primary_key = ''
        for k in spider.item.fields:
            table += "{0} VARCHAR(255) COLLATE utf8_bin NOT NULL DEFAULT 'None',\n"\
                .format(k)
            if '_PK' in k:
                primary_key = 'PRIMARY KEY ({0})'.format(k)
        if primary_key:
            table += primary_key
        else:
            raise Exception('Primary key not found.')

        # 不设主键时的创建table语句
        '''
        for i, k in enumerate(spider.item.fields):
            if i < (len(spider.item.fields) - 1):
                table += '{0} VARCHAR(255) COLLATE utf8_bin NOT NULL,\n'.format(k)
            elif i == (len(spider.item.fields) - 1):
                table += '{0} VARCHAR(255) COLLATE utf8_bin NOT NULL'.format(k)
            '''

        create_table = '''
        CREATE TABLE {0} (
        {1}
        )
        '''.format(spider.name, table)

        # 若指定table不存在，则建立table；否则跳过
        try:
            self.cursor.execute(create_table)
        except pymysql.err.InternalError:
            # table已创建，跳过此语句
            pass

    def process_item(self, item):

        self.fieldname = dict(item).keys()

        if item['level'] == 3 \
                and (item['parent'] not in self.level_2_not_leaf):
            self.level_2_not_leaf.append(item['parent'])

        self.items.append(item)

        return item

    def close_spider(self, spider):

        for sec_item in self.items:
            if sec_item['level'] == 2 and \
                    (sec_item['cat_idx_PK'] in self.level_2_not_leaf):
                sec_item['is_leaf'] = 0

            try:
                ks = ''
                vs = ''
                for k, v in sec_item.items():
                    ks += '{0}, '.format(k)
                    vs += "'{0}', ".format(v)
                ks = ks[:-2]
                vs = vs[:-2]

                inserter = '''
                INSERT INTO {0} 
                ({1})
                VALUES
                ({2})
                '''.format(spider.name, ks, vs)
                self.cursor.execute(inserter)
                self.connect.commit()

                self.logger.info(inserter)
                self.logger.info('data saved successfully, {0}'.format(dict(sec_item)))

            except pymysql.err.IntegrityError as err:

                self.logger.error(err)
                self.logger.error('error in {0}'.format(dict(sec_item)))

        self.cursor.close()
        self.connect.close()


class TopicListPipeline(object):

    def __init__(self, spider):
        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

        self.start_time = datetime.datetime.now()
        self.end_time = datetime.datetime.now()
        self.duration = datetime.timedelta()

        #self.logger = logging.getLogger(spider.name)
        self.logger = SetLogger.set_logger(spider.name)

        self.table_exist = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def open_spider(self, spider):

        self.start_time = datetime.datetime.now()
        print('Start at: {0}'.format(self.start_time))
        self.logger.info('Start at: {0}'.format(self.start_time))

        self.cursor.execute('USE zaih')

        self.cursor.execute('SHOW TABLES')
        table_exist_dict = self.cursor.fetchall()

        for table_dict in table_exist_dict:
            self.table_exist.append(table_dict['Tables_in_zaih'])

        for table_name, data_item in spider.item.items():

            if table_name not in self.table_exist:

                table = ''
                primary_key = ''

                normal = 0
                long = 0
                for k in data_item.fields:
                    if '_LONG' in k:
                        long += 1
                    else:
                        normal += 1
                if long != 0:
                    long_length = int((65535 / 4 - normal * 255) / (50 * long)) * 50
                else:
                    long_length = 10000

                self.logger.info('{} {} {}'.format(long_length, normal, long))

                for k in data_item.fields:
                    if '_LONG' in k:
                        table += "{0} VARCHAR({1}) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULL DEFAULT 'No data',\n" \
                            .format(k, long_length)
                    elif 'updated_time' in k:
                        table += "{0} VARCHAR(255) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULL DEFAULT '2017-01-01 00:00:00',\n" \
                            .format(k)
                    else:
                        table += "{0} VARCHAR(255) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULL DEFAULT 'No data',\n" \
                            .format(k)
                    if '_PK' in k:
                        if primary_key:
                            primary_key = k
                        else:
                            primary_key = primary_key + ',' + k
                if primary_key:
                    primary_key = 'CONSTRAINT {0}_PK PRIMARY KEY ({1})'.format(table_name, primary_key)
                    table += primary_key
                else:
                    self.logger.warning('Primary key not found.')
                    table = table[:-2]

                create_table = '''
                            CREATE TABLE {0} (
                            {1}
                            ) DEFAULT CHARSET=utf8mb4;
                            '''.format(table_name, table)

                self.cursor.execute(create_table)

    def process_item(self, item, spider):

        def _updater(updater_self, updater_item, updater_spider,
                     updater_table_name, updater_k_vs, updater_cond, updater_pk):
            updater_k_vs = updater_k_vs[:-2]
            updater_cond = updater_cond[:-5]

            updater = '''
            UPDATE {}
            SET {}
            WHERE {}
            '''.format(updater_table_name, updater_k_vs, updater_cond)

            #print(updater)
            #inserter_self.logger.info(updater)

            updater_self.cursor.execute(updater)
            updater_self.connect.commit()

            '''
            updater_self.logger.info('data update successfully, {0}'.format(
                {
                    updater_item['topic']['topic_name']: updater_item['topic']['topic_link'],
                    updater_item['mentor']['mentor_name']: updater_item['mentor']['mentor_link'],
                    updater_item['mentor']['mentor_id_PK']: updater_item['topic']['topic_id_PK'],
                    updater_item['topic']['cat_idx']: updater_item['topic']['city_link']
                }))
            '''

        def _inserter(inserter_self, inserter_item, inserter_spider,
                      inserter_table_name, inserter_ks, inserter_vs, inserter_pk):

            inserter_ks = inserter_ks[:-2]
            inserter_vs = inserter_vs[:-2]

            inserter = '''
            INSERT INTO {0} 
            ({1})
            VALUES
            ({2})
            '''.format(inserter_table_name, inserter_ks, inserter_vs)

            #print(inserter)
            # inserter_self.logger.info(inserter)

            inserter_self.cursor.execute(inserter)
            inserter_self.connect.commit()

            inserter_self.logger.info('data insert successfully, {0}'.format(
                {
                    inserter_item['topic']['topic_name']: inserter_item['topic']['topic_link'],
                    inserter_item['mentor']['mentor_name']: inserter_item['mentor']['mentor_link'],
                    inserter_item['mentor']['mentor_id_PK']: inserter_item['topic']['topic_id_PK'],
                    inserter_item['topic']['cat_idx']: inserter_item['topic']['city_link']
                }))

        for table_name, data_item in item.items():
            pk = {}
            try:
                # inserter
                try:
                    ks = ''
                    vs = ''
                    pk = {}
                    for k, v in data_item.items():
                        ks += '{0}, '.format(k)
                        vs += "'{0}', ".format(v)
                        if '_PK' in k:
                            pk[k] = v

                    _inserter(self, item, spider, table_name, ks, vs, pk)

                except pymysql.err.ProgrammingError:
                    ks = ''
                    vs = ''
                    pk = {}
                    for k, v in data_item.items():
                        ks += '{0}, '.format(k)
                        vs += '"{0}", '.format(v)
                        if '_PK' in k:
                            pk[k] = v

                    _inserter(self, item, spider, table_name, ks, vs, pk)

            except (pymysql.err.ProgrammingError, pymysql.err.IntegrityError) as err:

                self.logger.info(err)
                self.logger.info(
                    'Duplicate at table [{0}]:{1}, need to be updated'
                    .format(table_name, pk)
                )

                '''
                # update other_cat
                if table_name == 'topic':
                    self.logger.debug('Topic already exists, need to be updated. Debug: %s', err)

                    self.cursor.execute("SELECT cat_idx,other_cat FROM topic WHERE topic_id_PK = {0}"
                                        .format(data_item['topic_id_PK'])
                                        )
                    select_result = self.cursor.fetchone()
                    other_cat = select_result['other_cat']
                    cat_id_find_yet = [select_result['cat_idx']]
                    # other_cat_new 格式 ‘(出现在不同分类的次数）:{（分类1）}{（分类2）}……’
                    #分类格式 {cat_name:cat_idx}
                    other_cat_new = ''
                    if other_cat == 'No other cat' or other_cat == 'None':
                        if data_item['cat_idx'] not in cat_id_find_yet:
                            other_cat_new = '2:{{{0}:{1}}}'\
                                .format(data_item['cat_name'], data_item['cat_idx'])
                    elif other_cat:
                        cat_id_find_yet += re.findall(':(\w+?)\}', other_cat)
                        if data_item['cat_idx'] not in cat_id_find_yet:
                            #print(other_cat)
                            other_cat_count = int(other_cat[0]) + 1
                            other_cat_new = str(other_cat_count) + other_cat[1:] \
                                + ',{{{0}:{1}}}'.format(data_item['cat_name'], data_item['cat_idx'])

                    if other_cat_new:
                        other_cat_updater = "UPDATE topic SET other_cat='{0}' WHERE topic_id_PK={1};"\
                            .format(other_cat_new, data_item['topic_id_PK'])
                        #print(updater)
                        self.cursor.execute(other_cat_updater)
                        self.connect.commit()
                        self.logger.debug('Topic cat updated.')
                    else:
                        self.logger.debug('Same item, duplicated parsing.')
                '''

                '''
                # updater
                try:
                    k_vs = ''
                    cond = ''  # condition
                    pk = {}
                    for k, v in data_item.items():
                        if '_PK' in k:
                            cond += '{0} = "{1}" AND '.format(k, v)
                            pk[k] = v
                        #elif k != 'other_cat' and k != 'published_date':
                        elif table_name == 'topic' and k in ['cat_name', 'cat_idx']:
                            k_vs += '{0} = "{1}", '.format(k, v)
                        elif table_name == 'mentor':
                            k_vs += '{0} = "{1}", '.format(k, v)
                    _updater(self, item, spider, table_name, k_vs, cond, pk)

                except pymysql.err.ProgrammingError:

                    k_vs = ''
                    cond = ''  # condition
                    pk = {}
                    for k, v in data_item.items():
                        if '_PK' in k:
                            cond += "{0} = '{1}' AND ".format(k, v)
                            pk[k] = v
                        #elif k != 'other_cat' and k != 'published_date':
                        elif table_name == 'topic' and k in ['cat_name', 'cat_idx']:
                            v = str(v)
                            if v.find("'") >= 0:
                                v = v.replace("'", "''")
                            k_vs += "{0} = '{1}', ".format(k, v)
                        elif table_name == 'mentor':
                            v = str(v)
                            if v.find("'") >= 0:
                                v = v.replace("'", "''")
                            k_vs += "{0} = '{1}', ".format(k, v)
                    _updater(self, item, spider, table_name, k_vs, cond, pk)

                    # return item
                    '''

                try:
                    other_cat = "INSERT INTO topic_cat (cat_idx_PK, topic_id_PK, cat_name) \
                                VALUES ('{0}','{1}','{2}')".format(item['topic']['cat_idx'],
                                                             item['topic']['topic_id_PK'],
                                                             item['topic']['cat_name']
                                                             )
                    self.cursor.execute(other_cat)
                    self.connect.commit()
                except pymysql.err.IntegrityError:
                    pass

    def close_spider(self, spider):

        print('proxies at close:', len(spider.proxies))
        with open('./zaih_data/Proxies.txt', 'wb') as proxy_file:
            # 此处是为了使文本格式与api返回的一致
            proxies_text = '\r\n'.join(spider.proxies)
            proxies_bytes = bytes(proxies_text, encoding="utf8")
            proxy_file.write(proxies_bytes)

        print('Start at: {0}'.format(self.start_time))
        self.logger.info('Start at: {0}'.format(self.start_time))

        self.end_time = datetime.datetime.now()
        print('End at: {0}'.format(self.end_time))
        self.logger.info('End at: {0}'.format(self.end_time))

        self.duration = self.end_time - self.start_time
        print('Duration: {0}'.format(self.duration))
        self.logger.info('Duration: {0}'.format(self.duration))

        self.cursor.close()
        self.connect.close()


class MentorMysqlPipeline(object):

    def __init__(self, spider):
        """
        self.connect = pymysql.Connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        """
        self.connect = pymysql.Connect(
            cursorclass=pymysql.cursors.DictCursor,
            **zaih_scraper.settings.CONNECT_INFO
        )

        self.cursor = self.connect.cursor()

        self.start_time = datetime.datetime.now()
        self.end_time = datetime.datetime.now()
        self.duration = datetime.timedelta()

        # self.logger = logging.getLogger(spider.name)
        self.logger = SetLogger.set_logger(spider.name)

        self.table_exist = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def open_spider(self, spider):

        self.start_time = datetime.datetime.now()
        print('Start at: {0}'.format(self.start_time))
        self.logger.info('Start at: {0}'.format(self.start_time))

        self.cursor.execute('USE zaih')

        self.cursor.execute('SHOW TABLES')
        table_exist_dict = self.cursor.fetchall()

        for table_dict in table_exist_dict:
            self.table_exist.append(table_dict['Tables_in_zaih'])

        for table_name, data_item in spider.item.items():

            if table_name not in self.table_exist:

                table = ''
                primary_key = ''

                normal = 0
                long = 0
                for k in data_item.fields:
                    if '_LONG' in k:
                        long += 1
                    else:
                        normal += 1
                if long != 0:
                    long_length = int((65535 / 4 - normal * 255) / (50 * long)) * 50
                else:
                    long_length = 10000

                self.logger.info('{} {} {}'.format(long_length, normal, long))

                for k in data_item.fields:
                    if '_LONG' in k:
                        table += "{0} VARCHAR({1}) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULLDEFAULT 'No data',\n" \
                            .format(k, long_length)
                    elif 'updated_time' in k:
                        table += "{0} VARCHAR(255) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULL DEFAULT '2017-01-01 00:00:00',\n" \
                            .format(k)
                    else:
                        table += "{0} VARCHAR(255) CHARACTER SET utf8mb4 " \
                                 "COLLATE utf8mb4_bin NOT NULL DEFAULT 'No data',\n" \
                            .format(k)
                    if '_PK' in k:
                        if primary_key:
                            primary_key = k
                        else:
                            primary_key = primary_key + ',' + k
                if primary_key:
                    primary_key = 'CONSTRAINT {0}_PK PRIMARY KEY ({1})'.format(table_name, primary_key)
                    table += primary_key
                else:
                    self.logger.warning('Primary key not found.')
                    table = table[:-2]

                create_table = '''
                            CREATE TABLE {0} (
                            {1}
                            ) DEFAULT CHARSET=utf8mb4;
                            '''.format(table_name, table)

                self.cursor.execute(create_table)

    def process_item(self, item, spider):

        def _updater(updater_self, updater_item, updater_spider,
                     updater_table_name, updater_k_vs, updater_cond, updater_pk):
            updater_k_vs = updater_k_vs[:-2]
            updater_cond = updater_cond[:-5]

            updater = '''
            UPDATE {}
            SET {}
            WHERE {}
            '''.format(updater_table_name, updater_k_vs, updater_cond)

            print(updater)
            # inserter_self.logger.info(updater)

            updater_self.cursor.execute(updater)
            updater_self.connect.commit()

            updater_self.logger.info(
                'Data update successfully, at [{0}]:{1}\n'
                .format(updater_table_name, updater_pk)
            )

        def _inserter(inserter_self, inserter_item, inserter_spider,
                      inserter_table_name, inserter_ks, inserter_vs, inserter_pk):

            inserter_ks = inserter_ks[:-2]
            inserter_vs = inserter_vs[:-2]

            inserter = '''
            INSERT INTO {0} 
            ({1})
            VALUES
            ({2})
            '''.format(inserter_table_name, inserter_ks, inserter_vs)

            print(inserter)
            # inserter_self.logger.info(inserter)

            inserter_self.cursor.execute(inserter)
            inserter_self.connect.commit()

            inserter_self.logger.info(
                'Data insert successfully, at [{0}]:{1}\n'
                .format(inserter_table_name, inserter_pk)
            )

        for table_name, data_items in item.items():
            for data_item in data_items:
                pk = {}
                try:
                    # inserter
                    try:
                        ks = ''
                        vs = ''
                        pk = {}
                        for k, v in data_item.item.items():
                            ks += '{0}, '.format(k)
                            vs += '"{0}", '.format(v)
                            if '_PK' in k:
                                pk[k] = v

                        _inserter(self, item, spider, table_name, ks, vs, pk)

                    except pymysql.err.ProgrammingError:

                        ks = ''
                        vs = ''
                        pk = {}
                        for k, v in data_item.item.items():
                            ks += '{0}, '.format(k)
                            v = str(v)
                            if v.find("'") >= 0:
                                v = v.replace("'", "''")
                            vs += "'{0}', ".format(v)
                            if '_PK' in k:
                                pk[k] = v

                        _inserter(self, item, spider, table_name, ks, vs, pk)

                except (pymysql.err.ProgrammingError,
                        pymysql.err.IntegrityError,
                        pymysql.err.InternalError
                        ) as err:

                    self.logger.info(err)
                    self.logger.info(
                        'Duplicate at table [{0}]:{1}, need to be updated'
                        .format(table_name, pk)
                    )

                    # updater
                    try:
                        k_vs = ''
                        cond = ''  # condition
                        pk = {}
                        for k, v in data_item.item.items():
                            if '_PK' in k:
                                cond += '{0} = "{1}" AND '.format(k, v)
                                pk[k] = v
                            else:
                                k_vs += '{0} = "{1}", '.format(k, v)
                        _updater(self, item, spider, table_name, k_vs, cond, pk)

                    except pymysql.err.ProgrammingError:

                        k_vs = ''
                        cond = ''  # condition
                        pk = {}
                        for k, v in data_item.item.items():
                            if '_PK' in k:
                                cond += "{0} = '{1}' AND ".format(k, v)
                                pk[k] = v
                            else:
                                v = str(v)
                                if v.find("'") >= 0:
                                    v = v.replace("'", "''")
                                k_vs += "{0} = '{1}', ".format(k, v)
                        _updater(self, item, spider, table_name, k_vs, cond, pk)

    def close_spider(self, spider):

        print('proxies at close:', len(spider.proxies))
        with open('./zaih_data/Proxies.txt', 'wb') as proxy_file:
            # 此处是为了使文本格式与api返回的一致
            proxies_text = '\r\n'.join(spider.proxies)
            proxies_bytes = bytes(proxies_text, encoding="utf8")
            proxy_file.write(proxies_bytes)

        print('Start at: {0}'.format(self.start_time))
        self.logger.info('Start at: {0}'.format(self.start_time))

        self.end_time = datetime.datetime.now()
        print('End at: {0}'.format(self.end_time))
        self.logger.info('End at: {0}'.format(self.end_time))

        self.duration = self.end_time - self.start_time
        print('Duration: {0}'.format(self.duration))
        self.logger.info('Duration: {0}'.format(self.duration))

        self.cursor.close()
        self.connect.close()


class TopicSupPipeline(object):

    def __init__(self, spider):
        self.connect = pymysql.Connect(
            cursorclass=pymysql.cursors.DictCursor,
            **zaih_scraper.settings.CONNECT_INFO
        )

        self.cursor = self.connect.cursor()

        self.logger = SetLogger.set_logger(spider.name)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(spider=crawler.spider)

    def open_spider(self, spider):
        self.cursor.execute('USE zaih')

    def process_item(self, item, spider):

        table_name = list(spider.item.keys())[0]

        def _updater(updater_self, updater_item, updater_spider,
                     updater_table_name, updater_k_vs, updater_cond, updater_pk):
            updater_k_vs = updater_k_vs[:-2]
            updater_cond = updater_cond[:-5]

            updater = '''
            UPDATE {}
            SET {}
            WHERE {};
            '''.format(updater_table_name, updater_k_vs, updater_cond)

            # 输出sql语句
            #print(updater)
            updater_self.logger.info(updater)

            updater_self.cursor.execute(updater)
            updater_self.connect.commit()

            updater_self.logger.info(
                'Data update successfully, at [{0}]:{1}\n'
                .format(updater_table_name, updater_pk)
            )

        # updater
        try:
            k_vs = ''
            cond = ''  # condition
            pk = {}
            for k, v in item.items():
                if '_PK' in k:
                    cond += '{0} = "{1}" AND '.format(k, v)
                    pk[k] = v
                else:
                    k_vs += '{0} = "{1}", '.format(k, v)
            _updater(self, item, spider, table_name, k_vs, cond, pk)

        except pymysql.err.ProgrammingError:

            k_vs = ''
            cond = ''  # condition
            pk = {}
            for k, v in item.items():
                if '_PK' in k:
                    cond += "{0} = '{1}' AND ".format(k, v)
                    pk[k] = v
                else:
                    v = str(v)
                    if v.find("'") >= 0:
                        v = v.replace("'", "''")
                    k_vs += "{0} = '{1}', ".format(k, v)
            _updater(self, item, spider, table_name, k_vs, cond, pk)


class ItemTestPipeline(object):

    @staticmethod
    def process_item(item):
        print(item.__class__, item.items())
        for v in item.values():
            print(v.__class__, v.items())
