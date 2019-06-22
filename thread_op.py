# -*- coding: utf-8 -*-

"""
生产者消费者问题:
    生产者负责产生产品，并向仓库(queue队列)中添加产品(消息)
    消费者负责消费产品，从仓库(queue队列)中取出产品(消息)
    只要仓库没有满，生产者就可以继续生产；如果仓库满了，生产者就要等待
    只要仓库不为空，消费者就可以取出产品消费；如果仓库空了，消费者就需要等待
"""

import random
import time
from queue import Queue
from threading import Thread


# 用于存储产品的队列(缓冲区)
storage_queue = Queue(10)


class Producer(Thread):
    """生产者线程,向队列中添加消息(产品)"""
    def __init__(self, item):
        Thread.__init__(self)
        self.item = item

        # 线程生产产品序列号

    def run(self):
        while True:
            # 如果队列没有满
            if not storage_queue.full():
                # 向队列中添加消息
                storage_queue.put(self.item)
                # 随机等待0~3秒,模拟生产需要时间


class Consumer(Thread):
    """消费者，用于从队列中取出消息"""
    def __init__(self, name_f):
        Thread.__init__(self)
        self.name_f = name_f

    def run(self):
        while True:
            # 如果队列不为空
            if not storage_queue.empty():
                # 从队列中取出一个产品
                msg = storage_queue.get()

                # \t 让消费者输出前面空出一段
                print("\t%s 线程消费:[%s] qsize:%d" %
                      (self.name, msg, storage_queue.qsize()))

                # 随机等待0~2秒,模拟消费需要时间
                time.sleep(random.random() * 10)


def main():

    # 3个生产者线程
    for i in range(3):
        p = Producer('Producer-'+str(i))
        p.start()

    # 2个消费者线程
    for i in range(2):
        c = Consumer('Consumer-'+str(i))
        c.start()


if __name__ == "__main__":
    main()