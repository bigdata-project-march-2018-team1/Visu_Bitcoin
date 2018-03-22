# -*- coding: utf-8 -*-
# TODO main script as java app?
import logging
import time

from BTC_testing import insertHistoricalDataInBase as batchFunc 
from streamingPrice import streamingPriceDict as streamFunction
from produce_stream_current_price import produce_stream_current

from config import config

from threading import Thread

logging.basicConfig(**config['logger'])

def callLogger(fn, *arg):
    logging.info("%s start", fn.__name__)
    fn(*arg)
    logging.info("%s end", fn.__name__)

def streamFunc():
    print("TODO call func")

callLogger(batchFunc, config)

producer = Thread(target=produce_stream_current)
producer.start()
while(True):
    callLogger(streamFunction, config)
    time.sleep(60)

