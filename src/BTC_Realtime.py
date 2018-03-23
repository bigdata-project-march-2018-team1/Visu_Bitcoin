from pyspark.streaming import StreamingContext
from pyspark import SparkContext

DEFAULT_CONFIG = {
    'appName': "BPI-RT",
    'master': "local[1]"
}

def create_context(config=DEFAULT_CONFIG):
    sc = SparkContext(config['master', config['appName']])
    ssc = StreamingContext(sc)
