# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import log
import sqlite3
from langdetect import detect

class QuotesScrapePipeline(object):


    def open_spider(self, spider):
        self.connection = sqlite3.connect('./quotes_scrape/quotes_data')
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS authors (
                             author_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                             author TEXT NOT NULL UNIQUE )
                            """)

        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS categories (
                             category_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                             category TEXT NOT NULL UNIQUE )
                            """)

        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS quote_data
                              (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                              quote_text text NOT NULL,
                              author_id INTEGER NOT NULL,
                              category text NOT NULL,
                              scraped_date text NOT NULL,
                              is_favourite INTEGER NOT NULL,
                              was_qod INTEGER NOT NULL,
                              FOREIGN KEY (author_id) REFERENCES authors(author_id),
                              FOREIGN KEY (category) REFERENCES categories(category_id)
                            )""")

    def close_spider(self, spider):
        self.connection.commit()
        self.connection.close()

    def process_item(self, item, spider):

        # import ipdb; ipdb.set_trace()
        category_string = []
        for category in item["tags"].split(','):
            query = "SELECT * FROM categories WHERE category = '{}'".format(category)
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                category_string.append(result[0])
                log.msg('Category already exists! Continuing...')
        else:
            log.msg('Saving...')
            self.cursor.execute("""INSERT OR IGNORE INTO categories VALUES (?, ?);""",
                                (None, category))
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            category_string.append(result[0])
            log.msg('Got result from database')


        author_id = ''
        # import ipdb; ipdb.set_trace()
        query = 'SELECT author_id FROM authors WHERE author = "{}"'.format(item['author'])
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result :
            log.msg('Autor already exists! Skipping...')
            author_id = result[0]
        else:
            log.msg('Saving...')
            self.cursor.execute("""INSERT OR IGNORE INTO authors VALUES (?, ?);""",
                                (None, item['author']))
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            author_id = result[0]
            log.msg('Author saved!')


        if detect(item['text']) == 'en':
            self.cursor.execute('SELECT * FROM quote_data WHERE quote_text = ?' ,(item['text'],))
            result = self.cursor.fetchone()
            if result:
                log.msg('Item is already in the database')
            else:
                categories = ','.join(str(x) for x in category_string)
                log.msg('Saving...')
                print(author_id)
                self.cursor.execute(
                """INSERT INTO quote_data (id, quote_text, author_id, category, scraped_date, is_favourite, was_qod) VALUES
                (?, ?, ?, ?, ?, ?, ?) ;
                """,
                (None, item["text"], author_id, categories, item["scraped_date"], 0, 0))
                log.msg('Quote saved!')
        else:
            log.msg('Quote not in English! Skipping...')
            return item
#select * from quote_data where category like '%,169,%'
