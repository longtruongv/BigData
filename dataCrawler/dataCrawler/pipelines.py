# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import kafka

import json
import logging

class KafkaPipeline(object):
    def __init__(self, kafka_server, kafka_topic):
        self.kafka_producer = kafka.KafkaProducer(
            bootstrap_servers = kafka_server, 
            value_serializer = lambda x: json.dumps(x).encode('utf-8')
        )

        self.topic = kafka_topic

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            kafka_server = settings.get('KAFKA_SERVER'),
            kafka_topic = settings.get('KAFKA_TOPIC'),
        )
        
    def process_item(self, item, spider):
        data = dict(item)
    
        try:
            self.kafka_producer.send(self.topic, data).get(timeout=10)
            logging.info(f"Sent data to topic {self.topic}: {data['_id']}")
        except Exception as e:
            logging.error(f"Error while sending data to topic {self.topic}: {data['_id']}")
            logging.error(e)
        
        return item
