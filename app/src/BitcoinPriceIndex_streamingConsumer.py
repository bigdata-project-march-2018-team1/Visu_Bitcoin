from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elastic_storage import storeData, BitCoin, eraseData
from elastic_helper import http_auth

def send(rdd, conf):
    data_tx=rdd.collect()
    if data_tx:
        date=data_tx[0][0]
        value=data_tx[0][1]
        connections.create_connection(hosts=conf['elasticsearch']['hosts'], http_auth=http_auth(conf['elasticsearch']))
        storeData(date, float(value), "real-time")

def streamingPrice(master="local[2]", appName="CurrentPrice" , producer_host="localhost", db_host="db", port=9002):
    """
    Create a streaming who listening in hostname:port, get a text from a socket server every 60 secondes.
    """
    sc = SparkContext(master, appName)
    sc.setLogLevel("INFO")
    strc = StreamingContext(sc, 50)
    dstream = strc.socketTextStream(producer_host, port)
    dstream_map = dstream.map(lambda line: line.strip("{}"))\
                        .map(lambda str: str.split(","))\
                        .map(lambda line: (line[0].split(":",1)[1].strip('\" '), line[1].split(":")[1].strip('\" ')))\
                        .map(lambda tuple: (tuple[0].split("+")[0],tuple[1]))

    dstream_map.foreachRDD(lambda rdd: send(rdd, db_host))

    strc.start()
    strc.awaitTermination()

def streamingPriceDict(conf):   
    streamingPrice(db_host=conf['hosts'][0])

if __name__ == "__main__":
    streamingPrice(db_host="localhost")
