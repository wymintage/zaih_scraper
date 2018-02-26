# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZaihScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ZaihCategoryItem(scrapy.Item):
    cat_name = scrapy.Field()
    cat_idx_PK = scrapy.Field()


class ZaihCategoryDetailItem(scrapy.Item):
    cat_name = scrapy.Field()
    cat_idx_PK = scrapy.Field()
    level = scrapy.Field()
    parent = scrapy.Field()
    root = scrapy.Field()
    is_leaf = scrapy.Field()


class ZaihCityItem(scrapy.Item):
    city_PK = scrapy.Field()
    city_link = scrapy.Field()


class ZaihTopicItem(scrapy.Item):

    """
      category（cat) 是网页中提取的 tag ，并对应于之前的cat;
      category（cat) is the tag we extracted from HTML,
    and is the cat we used before 
    """

    topic_link = scrapy.Field()
    topic_id_PK = scrapy.Field()
    topic_type = scrapy.Field()
    topic_name = scrapy.Field()

    mentor_id = scrapy.Field()
    mentor_name = scrapy.Field()
    mentor_title = scrapy.Field()
    mentor_image = scrapy.Field()

    city_link = scrapy.Field()
    city = scrapy.Field()

    rating = scrapy.Field()
    price = scrapy.Field()

    meet_num_topic = scrapy.Field()
    meet_time = scrapy.Field()

    topic_intro_LONG = scrapy.Field()
    comments_count_topic = scrapy.Field()  # reviews_count

    published_date = scrapy.Field()


class ZaihMentorItem(scrapy.Item):

    mentor_link = scrapy.Field()
    mentor_id_PK = scrapy.Field()
    updated_time = scrapy.Field()
    mentor_name = scrapy.Field()

    mentor_image = scrapy.Field()

    mentor_title = scrapy.Field()  # introduction
    mentor_intro_LONG = scrapy.Field()  # summary

    respond_time = scrapy.Field()
    meet_num_total = scrapy.Field()
    meet_num_online = scrapy.Field()
    heart = scrapy.Field()
    accept_rate = scrapy.Field()

    city = scrapy.Field()
    location = scrapy.Field()

    comments_count_total = scrapy.Field()


class ZaihRecommendItem(scrapy.Item):

    recommend_from_m_id_PK = scrapy.Field()
    recommend_from_m_name = scrapy.Field()

    recommend_target_m_id_PK = scrapy.Field()
    recommend_target_m_name = scrapy.Field()

    recommend_target_t_name = scrapy.Field()

    recommend_target_city = scrapy.Field()

    recommend_discription_LONG = scrapy.Field()


class ZaihCommentItem(scrapy.Item):

    comment_user_nick_name = scrapy.Field()
    comment_user_real_name = scrapy.Field()
    comment_user_id = scrapy.Field()

    comment_id_PK = scrapy.Field()  # data-review-id/id
    comment_content_LONG = scrapy.Field()

    comment_topic_id = scrapy.Field()
    comment_mentor_id = scrapy.Field()

    comment_date = scrapy.Field()
    comment_heart = scrapy.Field()  # likings_count

    have_reply = scrapy.Field()
    comment_reply_LONG = scrapy.Field()
    comment_reply_date = scrapy.Field()

    comment_order_id = scrapy.Field()


class ZaihUserItem(scrapy.Item):

    comment_user_nick_name = scrapy.Field()
    comment_user_real_name = scrapy.Field()

    comment_user_id_PK = scrapy.Field()

    comment_user_image = scrapy.Field()

    comment_user_location = scrapy.Field()

    comment_user_title = scrapy.Field()
    comment_user_is_mentor = scrapy.Field()  # is_tutor

    comment_user_followers_count = scrapy.Field()

    comment_user_industry = scrapy.Field()

    comment_user_labels = scrapy.Field()
    comment_user_label = scrapy.Field()
