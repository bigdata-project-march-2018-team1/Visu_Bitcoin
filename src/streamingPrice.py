from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
from elastic_storage import storeData,BitCoin,eraseData


def streamingPrice(master="local[2]", appName="CurrentPrice" , hostname="localhost", port=9002):
    """
    Create a streaming who listening in hostname:port, get a text from a socket server and print it every 60 secondes.
    """
    sc = SparkContext( master , appName )
    #sc.setLogLevel("INFO")
    ssc = StreamingContext( sc , 10)
    lines = ssc.socketTextStream( hostname , port )
    words = lines.map(lambda line: line.strip("{}"))\
    .map(lambda str: str.split(","))\
    .map(lambda line: (float(line[0].split(":")[1]),line[1].split(":",1)[1].strip('\" '))).collect()\
    .cache()


    #conf = {"es.resource" : "index/type"}   # assume Elasticsearch is running on localhost defaults
    #rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat","org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
    #rdd.first()         # the result is a MapWritable that is converted to a Python dict

    words.pprint()
    storeData(words[1], words[0], "real_time")
    print(words[1])

    ssc.start()
    #ssc.stop(stopSparkContext=True, stopGraceFully=False)

    ssc.awaitTermination()
    #ssc.stop()
    #ssc.stop(stopSparkContext=True, stopGraceFully=False)

if __name__ == "__main__":
    streamingPrice()
