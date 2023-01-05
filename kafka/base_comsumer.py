# CODE NÀY LÀM MẪU CHO CÁC PHẦN KHÁC

from kafka import KafkaConsumer
import json
import logging

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

class BasedConsumer:
    def __init__(self, bootstrap_server='localhost:9092', topic='test'):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_server,
            value_deserializer = json_deserializer,
            # auto_offset_reset='earliest'
        )

        self.subscribe(topic)

    def subscribe(self, topics):
        self.consumer.subscribe(topics)
        logging.info(f"Subscribed to topics: {topics}")

    def unsubscribe(self):
        self.consumer.unsubscribe()

    def consume(self, timeout: float = 0.1):
        return self.consumer.poll(timeout)

if __name__ == '__main__':
    consumer = BasedConsumer(topic='news_crawled')
    while True:
        msg = consumer.consume()
        if msg:
            print(msg)