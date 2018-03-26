# -*- coding: utf-8 -*-
# TODO main script as java app?
import logging
from threading import Thread

from pyspark import SparkContext
from BitcoinPriceIndex_historical import insertHistoricalDataInBase as batchFunc 
from BitcoinPriceIndex_streamingConsumer import streamingPriceDict as streamFunction
from BitcoinPriceIndex_streamingProducer import produce_stream_current
from BitcoinTxIndex_streaming import insert_real_time_tx

from config import config

logging.basicConfig(**config['logger'])

def callLogger(fn, *arg):
    logging.info("%s start", fn.__name__)
    fn(*arg)
    logging.info("%s end", fn.__name__)

def streamFunc():
    print("TODO call func")

callLogger(batchFunc, config)

#sc = SparkContext(master="local[2]",appName="Bitcoin Transactions Real-time")
#producer_tx = Thread(target=insert_real_time_tx, args=[sc, config])
#producer_tx.start()

producer_price = Thread(target=produce_stream_current)
producer_price.start()

callLogger(streamFunction, config)
