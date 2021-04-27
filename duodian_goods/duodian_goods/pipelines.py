import pymysql


class DuodianGoodsPipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='123456', database='new_insert')
        self.cursor = self.conn.cursor()
        self.save_sql = None
        self.item_list = []
        self.save_sql = "insert into duodian_goods_data(id, store_id, goods_name, goods_price, goods_sales, update_time) value (%s,%s,%s,%s,%s,%s)"

    #创建一个对象，把获得的数据存储到该对象中，之后判断该对象的长度，如果长度达到条件执行插入
    def process_item(self, item, spider):
        save_data = (0,item['store_id'],  item['goods_name'], item['goods_price'], item['goods_sales'], item['update_time'])
        self.item_list.append(save_data)
        if len(self.item_list) == 500:
            self.bulk_insert_to_mysql(self.save_sql, self.item_list)
            del self.item_list[:]
        return item

    def bulk_insert_to_mysql(self, sql, item_list):
        try:
            self.cursor.executemany(sql, item_list)
            self.conn.commit()
            print('插入成功')
        except Exception as e:
            print(e)
            self.conn.rollback()




