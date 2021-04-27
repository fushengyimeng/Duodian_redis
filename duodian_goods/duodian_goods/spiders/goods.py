import scrapy
import json
import datetime
from ..items import DuodianGoodsItem
from scrapy_redis.spiders import RedisSpider


class GoodsSpider(RedisSpider):
    name = 'goods'
    redis_key = 'duodian_shop'
    #从redis中获取store_id,vender_id的数据
    def make_request_from_data(self, data):
        json_data = json.loads(data)
        store_id = json_data['store_id']
        vender_id = json_data['vender_id']
        url = 'https://searchgw.dmall.com/app/new/wareCategory/list'
        headers = {
            'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'searchgw.dmall.com',
        }
        data = {
            'param':'{"from":1,"queryType":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"%s","timestamp":"","venderId":"%s"}}' % (store_id, vender_id)
        }
        meta = {'store_id': store_id}
        return scrapy.FormRequest(url,headers = headers,formdata=data,meta=meta,callback=self.parse)

    #解析分类页
    def parse(self, response):
        json_data = json.loads(response.text)
        data_list = json_data['data']['wareCategory'][0]['categoryList']
        goods_url = 'https://searchgw.dmall.com/app/new/search/wareSearch'
        page = 1
        for first_category_data in data_list:
            first_category_name = first_category_data['categoryName']
            first_category_id = first_category_data['categoryId']
            first_category_type = first_category_data['categoryType']
            second_category_list = first_category_data['childCategoryList']
            # 如果有二级分类
            if second_category_list:
                for second_category_data in second_category_list:
                    second_category_name = second_category_data['categoryName']
                    second_category_id = second_category_data['categoryId']
                    second_category_type = second_category_data['categoryType']
                    third_category_list = second_category_data['childCategoryList']
                    if third_category_list:
                        for third_category_data in third_category_list:
                            third_category_name = third_category_data['categoryName']
                            third_category_id = third_category_data['categoryId']
                            third_category_type = third_category_data['categoryType']
                            headers = {
                                'venderId': '1',
                                'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                                'screen': '2392*1440',
                                'deliveryLat': '39.88776',
                                'recommend': '1',
                                'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                                'appMode': 'online',
                                'platform': 'ANDROID',
                                'utmId': '',
                                'firstInstallTime': '1615382576972',
                                'deliveryLng': '116.462128',
                                'businessCode': '2',
                                'apiVersion': '4.8.4',
                                'xyz': 'ac',
                                'networkType': '1',
                                'channelId': 'dm010205000004',
                                'lat': '30.344666',
                                'oaid': '',
                                'androidId': 'e1fc6030270ea8a4',
                                'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                                'sysVersion': '8.1.0',
                                'utmSource': '',
                                'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                                'lng': '120.121133',
                                'appName': 'com.wm.dmall',
                                'tpc': 'category_400101',
                                'isOpenNotification': '1',
                                'wifiState': '1',
                                'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                                'storeId': '12440',
                                'env': 'app',
                                'userId': '',
                                'version': '4.8.4',
                                'token': '',
                                'storeGroupV4': '',
                                'currentTime': '1615639803361',
                                'lastInstallTime': '1615382576972',
                                'tdc': '',
                                'areaId': '110105',
                                'gatewayCache': '',
                                'platformStoreGroup': '',
                                'dSource': '',
                                'device': 'google Pixel XL OPM1.171019.014',
                                'ticketName': '',
                                'smartLoading': '1',
                                'cid': '13065ffa4ea1bf6f089',
                                'dnsSdkVersion': '1.0.0',
                                'appVersion': '4.8.4',
                                'ISPCode': '',
                                'platformType': 'ANDROID',
                                'appCode': '0',
                                'netStatus': 'wifi',
                                'deviceName': 'Pixel XL',
                                'manufacturer': 'Google',
                                'Host': 'searchgw.dmall.com',
                            }
                            meta = {'store_id': response.meta['store_id'], 'current_page': page, 'status': 3, 'second_id': second_category_id, 'second_type': second_category_type, 'third_id': third_category_id}
                            data = {
                                'param': '{"brandId":"","categoryId":"%s","categoryLevel":2,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[{"checked":true,"childPropertyId":"%s","propertyId":"2"}],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (second_category_id, second_category_type, third_category_id, page)
                            }
                            yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers,meta=meta, callback=self.get_goods)
                    else:
                        headers = {
                            'venderId': '1',
                            'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                            'screen': '2392*1440',
                            'deliveryLat': '39.88776',
                            'recommend': '1',
                            'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                            'appMode': 'online',
                            'platform': 'ANDROID',
                            'utmId': '',
                            'firstInstallTime': '1615382576972',
                            'deliveryLng': '116.462128',
                            'businessCode': '2',
                            'apiVersion': '4.8.4',
                            'xyz': 'ac',
                            'networkType': '1',
                            'channelId': 'dm010205000004',
                            'lat': '30.344666',
                            'oaid': '',
                            'androidId': 'e1fc6030270ea8a4',
                            'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                            'sysVersion': '8.1.0',
                            'utmSource': '',
                            'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                            'lng': '120.121133',
                            'appName': 'com.wm.dmall',
                            'tpc': 'category_400101',
                            'isOpenNotification': '1',
                            'wifiState': '1',
                            'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                            'storeId': '12440',
                            'env': 'app',
                            'userId': '',
                            'version': '4.8.4',
                            'token': '',
                            'storeGroupV4': '',
                            'currentTime': '1615639803361',
                            'lastInstallTime': '1615382576972',
                            'tdc': '',
                            'areaId': '110105',
                            'gatewayCache': '',
                            'platformStoreGroup': '',
                            'dSource': '',
                            'device': 'google Pixel XL OPM1.171019.014',
                            'ticketName': '',
                            'smartLoading': '1',
                            'cid': '13065ffa4ea1bf6f089',
                            'dnsSdkVersion': '1.0.0',
                            'appVersion': '4.8.4',
                            'ISPCode': '',
                            'platformType': 'ANDROID',
                            'appCode': '0',
                            'netStatus': 'wifi',
                            'deviceName': 'Pixel XL',
                            'manufacturer': 'Google',
                            'Host': 'searchgw.dmall.com',
                        }
                        meta = {'store_id': response.meta['store_id'], 'current_page': page, 'status': 2, 'second_id': second_category_id,
                                'second_type': second_category_type}
                        data = {
                            'param': '{"brandId":"","categoryId":"%s","categoryLevel":2,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (second_category_id, second_category_type, page)
                        }
                        yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers,meta=meta, callback=self.get_goods)
            else:
                headers = {
                'venderId': '1',
                'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                'screen': '2392*1440',
                'deliveryLat': '39.88776',
                'recommend': '1',
                'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                'appMode': 'online',
                'platform': 'ANDROID',
                'utmId': '',
                'firstInstallTime': '1615382576972',
                'deliveryLng': '116.462128',
                'businessCode': '2',
                'apiVersion': '4.8.4',
                'xyz': 'ac',
                'networkType': '1',
                'channelId': 'dm010205000004',
                'lat': '30.344666',
                'oaid': '',
                'androidId': 'e1fc6030270ea8a4',
                'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                'sysVersion': '8.1.0',
                'utmSource': '',
                'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                'lng': '120.121133',
                'appName': 'com.wm.dmall',
                'tpc': 'category_400101',
                'isOpenNotification': '1',
                'wifiState': '1',
                'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                'storeId': '12440',
                'env': 'app',
                'userId': '',
                'version': '4.8.4',
                'token': '',
                'storeGroupV4': '',
                'currentTime': '1615639803361',
                'lastInstallTime': '1615382576972',
                'tdc': '',
                'areaId': '110105',
                'gatewayCache': '',
                'platformStoreGroup': '',
                'dSource': '',
                'device': 'google Pixel XL OPM1.171019.014',
                'ticketName': '',
                'smartLoading': '1',
                'cid': '13065ffa4ea1bf6f089',
                'dnsSdkVersion': '1.0.0',
                'appVersion': '4.8.4',
                'ISPCode': '',
                'platformType': 'ANDROID',
                'appCode': '0',
                'netStatus': 'wifi',
                'deviceName': 'Pixel XL',
                'manufacturer': 'Google',
                'Host': 'searchgw.dmall.com',
            }
                meta = {'store_id': response.meta['store_id'], 'current_page': page, 'status': 1, 'first_id': first_category_id, 'first_type': first_category_type}
                data = {
                    'param': '{"brandId":"","categoryId":"%s","categoryLevel":1,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (first_category_id, first_category_type, page)
                }
                yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers,meta=meta, callback=self.get_goods)

    def get_goods(self, response):
        goods_url = 'https://searchgw.dmall.com/app/new/search/wareSearch'
        current_page = response.meta['current_page']
        item = DuodianGoodsItem()
        json_data = json.loads(response.text)
        data_list = json_data['data']['wareList']
        all_pages = json_data['data']['pageInfo']['pageCount']
        for data in data_list:
            item['store_id'] = response.meta['store_id']
            item['goods_name'] = data['wareName']
            item['goods_price'] = data['onlinePrice']
            item['goods_sales'] = data['monthSales']
            item['update_time'] = str(datetime.datetime.now())
            yield item
        if current_page == all_pages:
            pass
        # 没到最后一页
        else:
            current_page += 1
            status = response.meta['status']
            if status == 3:
                headers = {
                    'venderId': '1',
                    'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                    'screen': '2392*1440',
                    'deliveryLat': '39.88776',
                    'recommend': '1',
                    'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                    'appMode': 'online',
                    'platform': 'ANDROID',
                    'utmId': '',
                    'firstInstallTime': '1615382576972',
                    'deliveryLng': '116.462128',
                    'businessCode': '2',
                    'apiVersion': '4.8.4',
                    'xyz': 'ac',
                    'networkType': '1',
                    'channelId': 'dm010205000004',
                    'lat': '30.344666',
                    'oaid': '',
                    'androidId': 'e1fc6030270ea8a4',
                    'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                    'sysVersion': '8.1.0',
                    'utmSource': '',
                    'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                    'lng': '120.121133',
                    'appName': 'com.wm.dmall',
                    'tpc': 'category_400101',
                    'isOpenNotification': '1',
                    'wifiState': '1',
                    'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                    'storeId': '12440',
                    'env': 'app',
                    'userId': '',
                    'version': '4.8.4',
                    'token': '',
                    'storeGroupV4': '',
                    'currentTime': '1615639803361',
                    'lastInstallTime': '1615382576972',
                    'tdc': '',
                    'areaId': '110105',
                    'gatewayCache': '',
                    'platformStoreGroup': '',
                    'dSource': '',
                    'device': 'google Pixel XL OPM1.171019.014',
                    'ticketName': '',
                    'smartLoading': '1',
                    'cid': '13065ffa4ea1bf6f089',
                    'dnsSdkVersion': '1.0.0',
                    'appVersion': '4.8.4',
                    'ISPCode': '',
                    'platformType': 'ANDROID',
                    'appCode': '0',
                    'netStatus': 'wifi',
                    'deviceName': 'Pixel XL',
                    'manufacturer': 'Google',
                    'Host': 'searchgw.dmall.com',
                }
                meta = {'store_id': response.meta['store_id'],'current_page': current_page, 'status': 3, 'second_id': response.meta['second_id'],
                        'second_type': response.meta['second_type'], 'third_id': response.meta['third_id']}
                data = {
                    'param': '{"brandId":"","categoryId":"%s","categoryLevel":2,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[{"checked":true,"childPropertyId":"%s","propertyId":"2"}],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (
                    response.meta['second_id'], response.meta['second_type'], response.meta['third_id'], current_page)
                }
                yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers, meta=meta, callback=self.get_goods)
            elif status == 2:
                headers = {
                    'venderId': '1',
                    'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                    'screen': '2392*1440',
                    'deliveryLat': '39.88776',
                    'recommend': '1',
                    'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                    'appMode': 'online',
                    'platform': 'ANDROID',
                    'utmId': '',
                    'firstInstallTime': '1615382576972',
                    'deliveryLng': '116.462128',
                    'businessCode': '2',
                    'apiVersion': '4.8.4',
                    'xyz': 'ac',
                    'networkType': '1',
                    'channelId': 'dm010205000004',
                    'lat': '30.344666',
                    'oaid': '',
                    'androidId': 'e1fc6030270ea8a4',
                    'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                    'sysVersion': '8.1.0',
                    'utmSource': '',
                    'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                    'lng': '120.121133',
                    'appName': 'com.wm.dmall',
                    'tpc': 'category_400101',
                    'isOpenNotification': '1',
                    'wifiState': '1',
                    'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                    'storeId': '12440',
                    'env': 'app',
                    'userId': '',
                    'version': '4.8.4',
                    'token': '',
                    'storeGroupV4': '',
                    'currentTime': '1615639803361',
                    'lastInstallTime': '1615382576972',
                    'tdc': '',
                    'areaId': '110105',
                    'gatewayCache': '',
                    'platformStoreGroup': '',
                    'dSource': '',
                    'device': 'google Pixel XL OPM1.171019.014',
                    'ticketName': '',
                    'smartLoading': '1',
                    'cid': '13065ffa4ea1bf6f089',
                    'dnsSdkVersion': '1.0.0',
                    'appVersion': '4.8.4',
                    'ISPCode': '',
                    'platformType': 'ANDROID',
                    'appCode': '0',
                    'netStatus': 'wifi',
                    'deviceName': 'Pixel XL',
                    'manufacturer': 'Google',
                    'Host': 'searchgw.dmall.com',
                }
                meta = {'store_id': response.meta['store_id'],'current_page': current_page, 'status': 2, 'second_id': response.meta['second_id'],
                        'second_type': response.meta['second_type']}
                data = {
                    'param': '{"brandId":"","categoryId":"%s","categoryLevel":2,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (
                    response.meta['second_id'], response.meta['second_type'], current_page)
                }
                yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers, meta=meta,
                                         callback=self.get_goods)
            elif status == 1:
                headers = {
                    'venderId': '1',
                    'User-Agent': 'dmall/4.8.4 Dalvik/2.1.0 (Linux; U; Android 8.1.0; Pixel XL Build/OPM1.171019.014)',
                    'screen': '2392*1440',
                    'deliveryLat': '39.88776',
                    'recommend': '1',
                    'uuid': '00000000-6d7c-d0a4-b744-b3c10033c587',
                    'appMode': 'online',
                    'platform': 'ANDROID',
                    'utmId': '',
                    'firstInstallTime': '1615382576972',
                    'deliveryLng': '116.462128',
                    'businessCode': '2',
                    'apiVersion': '4.8.4',
                    'xyz': 'ac',
                    'networkType': '1',
                    'channelId': 'dm010205000004',
                    'lat': '30.344666',
                    'oaid': '',
                    'androidId': 'e1fc6030270ea8a4',
                    'storeGroupKey': '032cb9bfa9b73ac640977cb3e93a6c69@MS0xMjQ0MC0x',
                    'sysVersion': '8.1.0',
                    'utmSource': '',
                    'platformStoreGroupKey': 'f4d38afa32a811fb4b7377d602118689@Mjg4LTE4MDI4',
                    'lng': '120.121133',
                    'appName': 'com.wm.dmall',
                    'tpc': 'category_400101',
                    'isOpenNotification': '1',
                    'wifiState': '1',
                    'sessionId': '8245ee03b7ae4d1c9bb573f842229e77',
                    'storeId': '12440',
                    'env': 'app',
                    'userId': '',
                    'version': '4.8.4',
                    'token': '',
                    'storeGroupV4': '',
                    'currentTime': '1615639803361',
                    'lastInstallTime': '1615382576972',
                    'tdc': '',
                    'areaId': '110105',
                    'gatewayCache': '',
                    'platformStoreGroup': '',
                    'dSource': '',
                    'device': 'google Pixel XL OPM1.171019.014',
                    'ticketName': '',
                    'smartLoading': '1',
                    'cid': '13065ffa4ea1bf6f089',
                    'dnsSdkVersion': '1.0.0',
                    'appVersion': '4.8.4',
                    'ISPCode': '',
                    'platformType': 'ANDROID',
                    'appCode': '0',
                    'netStatus': 'wifi',
                    'deviceName': 'Pixel XL',
                    'manufacturer': 'Google',
                    'Host': 'searchgw.dmall.com',
                }
                meta = {'store_id': response.meta['store_id'],'current_page': current_page, 'status': 1, 'first_id': response.meta['first_id'], 'first_type': response.meta['first_type']}
                data = {
                    'param': '{"brandId":"","categoryId":"%s","categoryLevel":1,"categoryType":%s,"from":1,"noResultSearch":0,"pos":1,"queryType":0,"selectOption":[],"sortKey":0,"sortRule":0,"src":0,"storeInfo":{"businessCode":99,"defaultChosed":false,"name":"","storeId":"12440","timestamp":"","venderId":"1"},"pageNum":"%s","pageSize":"20"}' % (
                    response.meta['first_id'], response.meta['first_type'], current_page)
                }
                yield scrapy.FormRequest(url=goods_url, formdata=data, headers=headers, meta=meta,
                                         callback=self.get_goods)






