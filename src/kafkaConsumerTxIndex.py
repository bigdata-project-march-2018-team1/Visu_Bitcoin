from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from BitcoinTxIndex_streaming import filter_tx

def get_Tx_Index_from_Kafka(topic):
    con=KafkaConsumer('json-topic',client_id='Evrim',auto_offset_reset='earliest',group_id='Alone In The Dark',value_deserializer=lambda m: json.loads(m.decode('ascii')))
    # con.committed(TopicPartition('jason-topic',0))
    for msg in con:
        print(msg)

def remplace_to_json_critere(a):
    return a.replace("\"","&").replace("\'","\"").replace("&","\'")

if __name__ == "__main__":
    print("la")
    con=KafkaConsumer('elasticDB',client_id='consumer',auto_offset_reset='earliest',group_id='Alone In The Dark')
    #con.subscribe(('elasticDB',))
    for tx in con:
        #print(filter_tx(tx.value.decode()))
        #print(json.loads(tx.value.decode()))
        print(filter_tx(json.loads(tx.value.decode())['x']))
