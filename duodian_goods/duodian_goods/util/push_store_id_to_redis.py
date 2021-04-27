import redis
import json
import pymysql


class SaveData:
    def __init__(self):
        self.r = redis.Redis(host='localhost',port=6379,db=0)
        self.conn, self.cursor = self.conn_mysql()

    def conn_mysql(self):
        conn = pymysql.connect(host='localhost',user='root',password='123456',database='new_insert')
        cursor = conn.cursor()
        return conn, cursor

    def get_store_id_from_mysql(self):
        sql = 'select storeId, venderId from duodian limit 10'
        self.cursor.execute(sql)
        result_list = self.cursor.fetchall()
        return result_list
    #从mysql中获取想要数据并存储到redis 中
    def set_data_to_redis(self):
        result_tuple = self.get_store_id_from_mysql()
        for single_data in result_tuple:
            store_id = single_data[0]
            vender_id = single_data[1]
            data_dict = {
                'store_id': store_id,
                'vender_id': vender_id
            }
            self.r.lpush('duodian_shop', json.dumps(data_dict))
            print('插入成功')

    def main(self):
        self.set_data_to_redis()


if __name__ == '__main__':
    spider = SaveData()
    spider.main()