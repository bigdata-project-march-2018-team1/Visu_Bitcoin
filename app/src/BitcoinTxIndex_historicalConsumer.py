from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers
import datetime
import ast

from config import config

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
    if data:
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

def send(rdd, host_db="localhost"):
    data_tx = rdd.collect()
    if data_tx:
        connections.create_connection(hosts=[host_db])
        add_historical_tx(data_tx[0])

def HisticalTx(master="local[2]", appName="Historical Transaction", group_id='Alone-In-The-Dark', topicName='test', producer_host="localhost", producer_port='2181', db_host="db"): 
    sc = SparkContext(master,appName)
    ssc = StreamingContext(sc,batchDuration=5)
    dstream = KafkaUtils.createStream(ssc,producer_host+":"+producer_port,group_id,{topicName:1},kafkaParams={"fetch.message.max.bytes":"1000000000"})\
                        .map(lambda v: ast.literal_eval(v[1]))\
                        .map(filter_tx)
    dstream.foreachRDD(lambda rdd: send(rdd, host_db=db_host))
    dstream.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    HisticalTx(db_host=config['hosts'])
    print("OK")