from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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

def streamingPrice(master="local[2]", appName="CurrentPrice" , producer_host="localhost", db_host="db", port=9002):
    """
    Create a streaming who listening in hostname:port, get a text from a socket server and print it every 60 secondes.
    """
    sc = SparkContext(master, appName)
    strc = StreamingContext(sc, 10)
    dstream = strc.socketTextStream(producer_host, port)
    dstream_map = dstream.map(lambda line: line.strip("{}"))\
                        .map(lambda str: str.split(","))\
                        .map(lambda line: (line[0].split(":",1)[1].strip('\" '), line[1].split(":")[1].strip('\" ')))\
                        .map(lambda tuple: (tuple[0].split("+")[0],tuple[1]))

    dstream_map.foreachRDD(lambda rdd: send(rdd, db_host))
    sc.setLogLevel("INFO")
    dstream_map.pprint()

    strc.start()
    strc.awaitTermination()

def streamingPriceDict(conf):   
    streamingPrice(db_host=conf['hosts'][0])

if __name__ == "__main__":
    streamingPrice()