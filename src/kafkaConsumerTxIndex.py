from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def get_Tx_Index_from_Kafka(topic):
    con=KafkaConsumer('json-topic',client_id='Evrim',auto_offset_reset='earliest',group_id='Alone In The Dark',value_deserializer=lambda m: json.loads(m.decode('ascii')))
    # con.committed(TopicPartition('jason-topic',0))
    for msg in con:
        print(msg)

def filter_tx(data):
    tx_filter = []
    if 'inputs' in data.keys():
        time = datetime.datetime.fromtimestamp(int(data['time'])).strftime('%Y-%m-%dT%H:%M:%S')
        for json in data['inputs']:
            if 'prev_out' in json.keys():
                current = {}
                current['date'] = str(time)
                current['id_tx'] = json['prev_out']['tx_index']
                current['value'] = json['prev_out']['value']/100000000
                tx_filter.append(current)
    return tx_filter

if __name__ == "__main__":
    print("la")
    con=KafkaConsumer('elasticDB',client_id='consumer',auto_offset_reset='earliest',group_id='Alone In The Dark')
    con.subscribe(('elasticDB',))
    for tx in con:
        #print(filter_tx(tx))
        print(json.loads(tx.value.decode()))
