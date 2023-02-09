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

    def iterate_by_chunks(self, 
        collection_name, 
        chunk_size=10, 
        start_from=0, 
        query={}, 
        projection={}
    ):
        '''
        query: câu truy vấn. VD: {"name": "Long"}
        projection: phép select. VD: {"_id": 1, "info": 1}
        '''

        collection = self.db[collection_name]

        chunks = range(start_from, collection.count_documents(query), int(chunk_size))
        num_chunks = len(chunks)

        for i in range(1, num_chunks + 1):
            if i < num_chunks:
                yield list(collection.find(query, projection=projection)[chunks[i-1]:chunks[i]])
            else:
                yield list(collection.find(query, projection=projection)[chunks[i-1]:chunks.stop])
    
class KafkaConnection:
    def __init__(self, bootstrap_server):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers = bootstrap_server,
            value_serializer = lambda x: json.dumps(x).encode('utf-8')
        )

    def send(self, topic, data):    
        try:
            self.producer.send(topic, data).get(timeout=10)
            logging.info(f"Sent data to topic {topic}: {data}")
        except Exception as e:
            logging.error(f"Error while sending data to topic {topic}: {data}")
            logging.error(e)


class MongoToKafka:
    def __init__(self,
        kafka_server, 
        mongo_server, mongo_port
    ):
        self.kafka = KafkaConnection(
            bootstrap_server = kafka_server, 
        )
        
        self.mongo = MongoConnection(
            mongo_server = mongo_server, 
            mongo_port = mongo_port, 
            mongo_db = "football_data_new"
        )

    def writeData(self, 
        collection_name,
        topic, 
        chunk_size=10, 
        start_from=0, 
        query={}, 
        projection={}
    ):
        chunk_iter = self.mongo.iterate_by_chunks(collection_name, chunk_size, start_from, query, projection)
        for chunk in chunk_iter:
            self.kafka.send(topic, chunk)

def main():
    writer = MongoToKafka(
        kafka_server = "localhost:9092", 
        mongo_server = "localhost",
        mongo_port = 27017,
    )

    writer.writeData("player", "football_player") 

if __name__ == "__main__":
    main()

