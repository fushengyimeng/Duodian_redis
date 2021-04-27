

import scrapy


class DuodianGoodsItem(scrapy.Item):
    store_id = scrapy.Field()
    goods_name = scrapy.Field()
    goods_price = scrapy.Field()
    goods_sales = scrapy.Field()
    update_time = scrapy.Field()
