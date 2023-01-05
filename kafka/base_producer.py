# CODE NÀY LÀM MẪU CHO CÁC PHẦN KHÁC

from kafka import KafkaProducer, KafkaConsumer
import logging
import json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

class BasedProducer:
    def __init__(self, bootstrap_server='localhost:9092', topic='test'):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers = bootstrap_server,
            value_serializer = json_serializer
        )

        self.topic = topic

    def send(self, data):
        try:
            self.kafka_producer.send(self.topic, data).get(timeout=10)
            logging.info(f"Sent data to topic {self.topic}\n\t{data}")
            print(f"Sent data to topic {self.topic}\n\t{data}")
        except Exception:
            logging.error(f"Error while sending data to topic {self.topic}\n\t{data}")
            print(f"Error while sending data to topic {self.topic}\n\t{data}")

if __name__ == '__main__':
    import random
    data = {"hello": random.randint(0,90)}

    producer = BasedProducer(topic='news_crawled')
    producer.send(data)
    