# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class GeneralItem(scrapy.Item):
    _id = scrapy.Field()
    type = scrapy.Field()
    info = scrapy.Field()
    stats = scrapy.Field()

# class PlayerItem(scrapy.Item):
#     _id = scrapy.Field()
#     type = scrapy.Field()
#     info = scrapy.Field()
#     stats = scrapy.Field()

# class ClubItem(scrapy.Item):
#     _id = scrapy.Field()
#     type = scrapy.Field()
#     info = scrapy.Field()
#     stats = scrapy.Field()

# class LeagueItem(scrapy.Item):
#     _id = scrapy.Field()
#     type = scrapy.Field()
#     info = scrapy.Field()
#     stats = scrapy.Field()