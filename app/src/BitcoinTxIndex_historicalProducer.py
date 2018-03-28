import json
import pprint
import time
import datetime
import logging
from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaProducer
from kafka.errors import KafkaError

from pyspark import SparkContext

from elastic_storage import eraseData

DEFAULT_HOST = "blockchain.info"
URI_BLOCKS = "/fr/blocks/"
URI_TRANSACTIONS = "/fr/rawblock/"

def connectionToAPI(host, path):
    """ Connexion to the Blockchain API
    
    Arguments:
        sc {SparkContext} -- [description]
        host {string} -- [description]
        path {string} -- [description]
    
    Returns:
        [rdd] -- [description]
    """

    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    else:
        time.sleep(10)
        connectionToAPI(host, path)
    connection.close()
    return result

def getListBlocks_1day(date, host=DEFAULT_HOST, uri=URI_BLOCKS):
    """ Get the list of blocks created for a date
    
    Arguments:
        sc {SparkContext} -- [description]
        date {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        uri {string} -- [description] (default: {URI_BLOCKS})
    
    Returns:
        list -- [description]
    """

    timestemp = int(time.mktime(datetime.datetime.strptime(
        date, "%Y-%m-%d").timetuple()))*1000
    path = uri + str(timestemp) + "?format=json"
    all_infos_blocks = connectionToAPI(host, path)
    return filter_listBlocks(all_infos_blocks)

def getListBlocks_Ndays(start, end, host=DEFAULT_HOST, uri=URI_BLOCKS):
    """ Get the list of blocks created between two date
    
    Arguments:
        sc {SparkContext} -- [description]
        start {string} -- [description]
        end {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        uri {string} -- [description] (default: {URI_BLOCKS})
    
    Returns:
        list -- [description]
    """

    blocks_list = []
    blocks_list += getListBlocks_1day(start)
    start_datetime = stringToDatetime(start)
    current_dateTime = start_datetime + datetime.timedelta(days=1)
    end_dateTime = stringToDatetime(end)
    while current_dateTime <= end_dateTime:
        blocks_list += getListBlocks_1day(current_dateTime.strftime('%Y-%m-%d'))
        current_dateTime += datetime.timedelta(days=1)
    return blocks_list

def stringToDatetime(date):
    """ Convert a string date to a datetime date
    
    Arguments:
        date {string} -- [description]
    
    Returns:
        datetime -- [description]
    """

    timestemp = int(time.mktime(
        datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    return datetime.datetime.fromtimestamp(timestemp)

def filter_listBlocks(listBlocks):
    """ Filter the blocks information to keep only hash
    
    Arguments:
        listBlocks {list} -- [description]
    
    Returns:
        list -- [description]
    """

    res = []
    for js in listBlocks['blocks']:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        currentblock['time'] = js['time']
        res.append(currentblock)
    return res

def getListTx_Block(block, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
    """ Get transactions for a block
    
    Arguments:
        sc {SparkContext} -- [description]
        block {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        path {string} -- [description] (default: {URI_TRANSACTIONS})
    
    Returns:
        list -- [description]
    """

    return connectionToAPI(host, path + str(block))

def block_test():
    test_blk = getListTx_Block('0000000000000bae09a7a393a8acded75aa67e46cb81f7acaa5ad94f9eacd103')
    del test_blk['tx'][5:]
    return test_blk

def send_to_consumer(start,end,producer):
    list_blocks = getListBlocks_Ndays(start, end)
    for block in list_blocks:
        txs = getListTx_Block(block['id_block'])
        producer.send('test',str(txs).encode())
        print("Send...")
    producer.close()

if __name__ == "__main__":
    producer = KafkaProducer(acks=1,max_request_size=10000000,bootstrap_servers='localhost:9092')
    send_to_consumer("2018-01-01","2018-02-01",producer)