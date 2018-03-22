from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elastic_storage import storeData,BitCoin,eraseData

def send(rdd, host):
    data_tx=rdd.collect()
    if data_tx:
        date=data_tx[0][0]
        value=data_tx[0][1]
        connections.create_connection(hosts=[host])
        storeData(date, float(value), "real-time")

def streamingPrice(master="local[2]", appName="CurrentPrice" , producer_host="localhost", db_host="localhost", port=9002):
    """
    Create a streaming who listening in hostname:port, get a text from a socket server and print it every 60 secondes.
    """
    sc = SparkContext( master , appName )
    #sc.setLogLevel("INFO")
    ssc = StreamingContext( sc , 10)
    lines = ssc.socketTextStream( producer_host , port )
    words = lines.map(lambda line: line.strip("{}"))\
    .map(lambda str: str.split(","))\
    .map(lambda line: (line[0].split(":",1)[1].strip('\" '), line[1].split(":")[1].strip('\" ')))\
    .map(lambda tuple: (tuple[0].split("+")[0],tuple[1]))


    #conf = {"es.resource" : "index/type"}   # assume Elasticsearch is running on localhost defaults
    #rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat","org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
    #rdd.first()         # the result is a MapWritable that is converted to a Python dict

    words.foreachRDD(lambda rdd: send(rdd, db_host))
    words.pprint()

    ssc.start()
    ssc.awaitTermination()
    #ssc.stop()

def streamingPriceDict(conf):   
    streamingPrice(db_host=conf['hostname'])

if __name__ == "__main__":
    streamingPrice()
