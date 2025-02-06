# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ArticleItem(scrapy.Item):
    title = scrapy.Field()
    description = scrapy.Field()
    article_text = scrapy.Field()
    publication_datetime = scrapy.Field()
    header_photo_url = scrapy.Field()
    header_photo_base64 = scrapy.Field()
    keywords = scrapy.Field()
    authors = scrapy.Field()
    source_url = scrapy.Field()
