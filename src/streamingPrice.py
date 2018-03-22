from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elastic_storage import storeData,BitCoin,eraseData

def send(rdd, host='localhost'):
    data_tx=rdd.collect()
    if data_tx:
        date=data_tx[0][0]
        value=data_tx[0][1]
        print (date, value)
        connections.create_connection(hosts=[host])
        storeData(date, float(value), "real-time")

def streamingPrice(master="local[2]", appName="CurrentPrice" , hostname="localhost", port=9002):
    """
    Create a streaming who listening in hostname:port, get a text from a socket server and print it every 60 secondes.
    """
    sc = SparkContext(master ,appName)
    ssc = StreamingContext(sc, 60)

    dstream = ssc.socketTextStream(hostname, port)
    lines = dstream.map(lambda line: line.strip("{}"))\
        .map(lambda str: str.split(","))\
        .map(lambda line: (line[0].split(":",1)[1].strip('\" '), line[1].split(":")[1].strip('\" ')))\
        .map(lambda tuple: (tuple[0].split("+")[0],tuple[1]))

    lines.foreachRDD(lambda rdd: send(rdd, host=hostname))

    ssc.start()

    ssc.awaitTermination()
    #ssc.stop()

def streamingPriceDict(conf):
    streamingPrice(hostname=conf['hostname'])

if __name__ == "__main__":
    streamingPrice()