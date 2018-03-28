import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../src'))

import datetime
import time
from pyspark import SparkContext

from BitcoinTxIndex_historical import getListBlocks_1day, getListBlocks_2dates, stringToDatetime, filterHash_listBlocks, getList_txBlock, filter_tx

def teststringToDatetime():
    date = "2012-01-01"
    assert stringToDatetime(date) == datetime.datetime(2012, 1, 1)

def testFilterHash_listBlocks():
    listBlocks = ['blocks', [{'hash': 'unit_test'}]]
    assert filterHash_listBlocks(listBlocks) == [{'id_block': 'unit_test'}]

def testFilter_tx():
    date = "2012-01-01"
    timestamp = int(time.mktime(
        datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    data = [{'inputs':
             [{'prev_out': {
                 'tx_index': 'unit_test_tx_index',
                 'value': 10*100000000
             }}],
             'time': timestamp
             }]
    results = [{'date': date+"T00:00:00",
                'id_tx': 'unit_test_tx_index',
                'value': 10
                }]
    assert filter_tx(data) == results

