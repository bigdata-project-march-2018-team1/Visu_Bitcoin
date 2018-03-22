# -*- coding: utf-8 -*-
# TODO main script as java app?
import logging

from BTC_testing import insertHistoricalDataInBase as batchFunc 
#from tba import tba as streamFunction

from config import config

logging.basicConfig(**config['logger'])

def callLogger(fn, *arg):
    logging.info("%s start", fn.__name__)
    fn(*arg)
    logging.info("%s end", fn.__name__)

def streamFunc():
    print("TODO call func")

callLogger(batchFunc, config)
while(True):
    time.sleep(60)
    callLogger(streamFunction, config)

