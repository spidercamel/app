import requests
import json
from pymongo import *
from queue import Queue
from threading import Thread
import time
import logging
import json

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')

# logging.debug('This is debug message')
# logging.info('This is info message')
# logging.warning('This is warning message')
#
Thread_count = 5
class Downloader():
    def __init__(self):
        mongo_client = MongoClient(host='localhost', port=27017)
        db = mongo_client['app']
        # self.collectione01 = db['﻿Total_Raw']
        self.collectione02 = db['dingdingpai_total']
        self.downloaded_db = db['downloaded_db']
        self.storage_queue = Queue(8)
        self.num01 = 0

    def filter(self):
        data_all = self.collectione01.find()
        # a = [i['resobjs'][0]['remotePath'] for i in data_all]
        # for k,v in i.items():
        # print(k)
        # print(a)
        # print(len(data_all))
        data_all02 = []
        for i in data_all:
            data_all02.append(i)
        print(len(data_all02))
        num = 0
        for i in data_all02:
            num += 1
            for u in data_all02[num::]:
                if i['resobjs'][0]['remotePath'] == u['resobjs'][0]['remotePath']:
                    data_all02.remove(u)
        print(len(data_all02))
        return data_all02

    def put_in_mongo(self, items):
        self.collectione02.insert_many(items)

    def download_file(self):

        curser01 = self.collectione02.find()
        print(curser01.count())
        items = []
        for i in curser01:
            items.append(i)
        Thread01 = Thread(target=self.que_put,args=(items,))
        Thread01.start()
        for i in range(Thread_count):
            c = Thread(target=self.down_f)
            c.start()


    def down_f(self):
        while True:
            try:
                self.num01+=1
                print(self.num01)

                item = self.storage_queue.get()
                re01 = self.check_item(item)
                if not re01:
                    print('已存在，跳过')
                    continue
                try:
                    url = item['resobjs'][0]['remotePath']
                except:
                    print('erro')
                    self.num01 -=1
                    print(self.num01)

                    continue
                name_f = '/Volumes/ElementsPA/traffic_video/' + item['trafficEvt']["wzdes"] + '_' + str(item['id']) + '.mp4'
                print('{} 正在下载:{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),name_f))
                print(url)
                response01 = requests.get(url, stream=True)

                with open(name_f, 'wb')as f:
                    f.write(response01.content)
                self.downloaded_db.insert_one(item)
                print('***{} 下载完成{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),name_f))
            except BaseException as e :
                logging.error(e)
                logging.error(json.dumps(item.pop('_id')))
                print(e)
                pass
    def check_item(self,item):
        re01 = self.downloaded_db.find({'id':item['id']})
        if not re01.count():
            return 1
        else:
            return 0

    def que_put(self,itmes):
        for i in itmes:
            self.storage_queue.put(i)


def main_file():
    a = Downloader()
    data_all02 = a.filter()
    a.put_in_mongo(data_all02)

def main_down():
    a = Downloader()
    a.download_file()



if __name__ == '__main__':
    # time.sleep(6600)
    main_down()
