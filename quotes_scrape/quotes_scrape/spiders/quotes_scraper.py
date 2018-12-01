from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from quotes_scrape.items import QuotesScrapeItem
from datetime import datetime
import csv

PRESET_CATEGORIES = [
    'Depression',
    'Love',
    'Death',
    'Suffering',
    'Hope',
    'Success & Failure',
    'Choice',
    'Motivational',
    'Health',
    'Sports',
    'Sex',
    'Luck',
    'Honor',

]

PAGE_COUNTER = 0

class QuotesScraper(Spider):
    name = 'scrape_quotes'


    def start_requests(self):
        yield Request('http://www.quoteland.com/topic.asp', callback=self.parse_category)



    def parse_category(self, response):
        # import ipdb; ipdb.set_trace()
        categories = dict(zip(
            response.xpath('//nobr/a/font/text()').extract(),
            response.xpath('//nobr/a/@href').extract()
        ))
        # import ipdb; ipdb.set_trace()
        for category in PRESET_CATEGORIES:
            try:
                page_no = 1
                yield Request('http://www.quoteland.com{}?pg={}'.format(categories[category],
                                                                         page_no),
                              callback = self.parse_data,
                              meta={
                                  'page': page_no,
                                  'category': categories[category],
                              })
            except KeyError:
                pass

    def parse_data(self, response):
        # import ipdb; ipdb.set_trace()
        current_page = response.meta.get('page', '')
        total_pages = \
        int(response.xpath('//b[contains(.,"Page:")]/following-sibling::text()')[0].extract().split('of ')[1])
        data = response.xpath('//tr/td/font[./i and ./i/a/@href[contains(. , "author")]]')
        category = response.url.split('/')[4].split('-')[0]
        for row in data:
            output_data = {}
            output_data['author'] = row.xpath('./i/a/text()').extract()[0]
            output_data['quote'] = ' '.join(row.xpath('./preceding-sibling::font/text()').extract()).replace(';', '')
            output_data['category'] = category
            output_data['likes'] = 0
            output_data['date_crawled'] = datetime.strftime(datetime.now(),'%d.%m.%y')
            if len(output_data['quote']) < 150 and output_data['quote'] != '':
                yield output_data
            else:
                print("Quote with more than 150 characters found: \n {} by {}".format(output_data['quote'],
                                                                                      output_data['author']))
        if current_page < total_pages:
            page_no = current_page + 1
            yield Request('http://www.quoteland.com{}?pg={}'.format(response.meta['category'],
                                                                     page_no),
                          callback = self.parse_data,
                          meta={
                              'page': page_no,
                              'category': response.meta['category']
                          },
                          dont_filter=True)
