# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class QuotesScrapeItem(scrapy.Item):
    text = scrapy.Field()
    scraped_date = scrapy.Field()
    tags = scrapy.Field()
    author = scrapy.Field()
