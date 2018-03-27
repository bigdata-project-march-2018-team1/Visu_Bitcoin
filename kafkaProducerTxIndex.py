from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from BitcoinTxIndex_streaming import getRealTimeTx
import time

def post_Tx_index_to_kafka(producer,topic,json):
    # bootstrap_servers=['localhost:9092'] par default
    #producer = KafkaProducer(client_id='lmfao',acks=1,value_serializer=lambda m: json.dumps(m).encode('ascii'))

    # boucle to send to kafka server
    producer.send(topic, json)

if __name__ == "__main__":
    #producer = KafkaProducer(acks=1,value_serializer=lambda m: json.dumps(m).encode('ascii'))
    producer = KafkaProducer(acks=1)

    while true:
        time.sleep(20)
        for tx in getRealTimeTx(sc):
            producer.send("elasticDB",tx.encode())
