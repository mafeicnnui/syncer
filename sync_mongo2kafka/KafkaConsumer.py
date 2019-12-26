#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/12/14 11:02
# @Author : 马飞
# @File : sync_mongo_oplog.py
# @Software: PyCharm

from kafka import KafkaConsumer
import json,datetime


'''
  功能：使用Kafka—python的消费模块
'''
class Kafka_consumer():

    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
        self.kafkaHost  = kafkahost
        self.kafkaPort  = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid    = groupid
        self.consumer   = KafkaConsumer(self.kafkatopic, group_id=self.groupid,
                                        bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                        kafka_host=self.kafkaHost,
                                        kafka_port=self.kafkaPort))

    def consume_data(self):
        try:
            for message in self.consumer:
                #print json.loads(message.value)
                yield message
        except KeyboardInterrupt as e:
            print(str(e))

'''
  功能：Kafka连接配置
'''
KAFKA_SETTINGS = {
    "host"  : "172.17.194.79",
    "port"  :  9092,
    "topic" : 'sync_mongo2kafka_hopson'
}

#主函数
def main():
   #获取消费者对象
   consumer = Kafka_consumer(KAFKA_SETTINGS['host'], KAFKA_SETTINGS['port'], KAFKA_SETTINGS['topic'], 'test-python-mongo')
   message  = consumer.consume_data()

   #从topic中获取消息
   for i in message:
       #print('topic={0},json={1}',i.topic,i.value)
       print(i.value.decode())

if __name__ == "__main__":
     main()

