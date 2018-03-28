from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers
import datetime
import ast


def get_Tx_Index_from_Kafka(topic):
    con=KafkaConsumer('json-topic',client_id='Evrim',auto_offset_reset='earliest',group_id='Alone In The Dark',value_deserializer=lambda m: json.loads(m.decode('ascii')))
    # con.committed(TopicPartition('jason-topic',0))
    for msg in con:
        print(msg)

def remplace_to_json_critere(a):
    return a.replace("\"","&").replace("\'","\"").replace("&","\'")

def add_historical_tx(historicalDataset):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_tx",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source": {
                "type": "historical",
                "value": data['value'],
                "time": {'path': data['date'], 'format':'%Y-%m-%dT%H:%M:%S'}
            }
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def filter_tx(data,satochiToBitcoin=100000000):
    """ Filter the transactions information to keep only date, value and id
    
    Arguments:
        data {list} -- [description]
    
    Returns:
        list -- [description]
    """

    tx_filter = []
    for json_tx in data['tx']:
        if 'inputs' in json_tx.keys():
            time = timestampToDate(json_tx['time'])
            for json_inputs in json_tx['inputs']:
                if 'prev_out' in json_inputs.keys():
                    current = {}
                    current['date'] = time
                    current['id_tx'] = json_inputs['prev_out']['tx_index']
                    current['value'] = float(
                        json_inputs['prev_out']['value'])/satochiToBitcoin
                    tx_filter.append(current)
    return tx_filter

def timestampToDate(timestamp):
    time = datetime.datetime.fromtimestamp(
                int(timestamp)).strftime("%Y-%m-%d"'T'"%H:%M:%S")
    return time

if __name__ == "__main__":
    print("la")
    sc = SparkContext(master="local[2]",appName="Test")
    ssc = StreamingContext(sc,batchDuration=5)
    dstream = KafkaUtils.createStream(ssc,"localhost:2181",'Alone-In-The-Dark',{'app_topic':1})\
    .map(lambda v: v[1])
    dstream.pprint()
    ssc.start()
    ssc.awaitTermination()
    print("OK")  
    '''cons = KafkaConsumer("app_topic",client_id='consumer',auto_offset_reset='earliest',group_id='Alone-In-The-Dark')
    for tx in cons:
        rdd = sc.parallelize(str(tx.value.decode()))\
                .collect()
        print(rdd)'''
        
        #recv = ast.literal_eval(tx.value.decode())
        #recv_fil = filter_tx(recv)
        #print(recv_fil)
        #add_historical_tx(recv_fil)
