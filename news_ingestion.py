import pymongo
import kafka

import json
import logging

class MongoConnection:
    def __init__(self, mongo_server, mongo_port, mongo_db):
        connection = pymongo.MongoClient(
            mongo_server,
            mongo_port,
        )
        self.db = connection[mongo_db]

    def insert(self, data: dict, collection_name):
        try: 
            collection = self.db[collection_name]
            collection.insert_one(data)
            logging.debug(f"Inserted data to collection {collection}: {data}")
        except Exception as e:
            logging.error(e)


class KafkaConnection:
    def __init__(self, bootstrap_server, group_id, topic):
        self.consumer = kafka.KafkaConsumer(
            bootstrap_servers = bootstrap_server,
            # group_id = group_id,
            # auto_offset_reset='earliest',
            value_deserializer = lambda x: json.loads(x.decode('utf-8')),
        )

        self.subscribe(topic)

    def subscribe(self, topics):
        self.consumer.subscribe(topics)
        logging.debug(f"Subscribed to topics: {topics}")

    def unsubscribe(self):
        self.consumer.unsubscribe()

    def consume(self, timeout: float = 0.1):
        return self.consumer.poll(timeout)

    def commit(self, message):
        self.consumer.commit(message)

class DataIngestion:
    def __init__(self,
        kafka_server, kafka_group_id, 
        mongo_server, mongo_port
    ):
        self.kafka = KafkaConnection(
            bootstrap_server = kafka_server, 
            group_id = kafka_group_id,
            topic = "fbref",
        )
        
        self.mongo = MongoConnection(
            mongo_server = mongo_server, 
            mongo_port = mongo_port, 
            mongo_db = "football_data"
        )

    def ingest(self):
        while True:
            data = self.kafka.consume()
            if not data:
                continue

            records = self.process_kafka(data)
            self.process_mongo(records)

    def process_kafka(self, data):
        # self.kafka.commit(data)

        records = []
        for topic_data, consumer_records in data.items():
            for consumer_record in consumer_records:
                record = consumer_record.value
                records.append(record)
                logging.debug(f"Comsumed a new record: {record}")
        return records

    def process_mongo(self, records):
        for record in records:
            collection = record['type']
            self.mongo.insert(record, collection)

def main():
    ingestion = DataIngestion(
        kafka_server = "localhost:9092", 
        kafka_group_id = "group_test",
        mongo_server = "localhost",
        mongo_port = 27017,
    )

    ingestion.ingest()

if __name__ == "__main__":
    main()

