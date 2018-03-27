import json
import pprint
import time
import datetime
import logging
from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers

from pyspark import SparkContext

from elastic_storage import eraseData

DEFAULT_HOST = "blockchain.info"
URI_BLOCKS = "/fr/blocks/"
URI_TRANSACTIONS = "/fr/rawblock/"

def connectionToAPI(sc,host, path):
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
    connection.close()
    return sc.parallelize(result.items())

def getListBlocks_1day(sc, date, host=DEFAULT_HOST, uri=URI_BLOCKS):
    """ Get the list of blocks created for a date
    
    Arguments:
        sc {SparkContext} -- [description]
        date {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        uri {string} -- [description] (default: {URI_BLOCKS})
    
    Returns:
        lisr -- [description]
    """

    timestemp = int(time.mktime(datetime.datetime.strptime(
        date, "%Y-%m-%d").timetuple()))*1000
    path = uri + str(timestemp) + "?format=json"
    rdd = connectionToAPI(sc, host, path).map(filterHash_listBlocks).collect()[0]
    return rdd

def getListBlocks_2dates(sc, start, end, host=DEFAULT_HOST, uri=URI_BLOCKS):
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
    blocks_list += getListBlocks_1day(sc, start)
    start_datetime = stringToDatetime(start)
    current_dateTime = start_datetime + datetime.timedelta(days=1)
    end_dateTime = stringToDatetime(end)
    while current_dateTime <= end_dateTime:
        blocks_list += getListBlocks_1day(sc, current_dateTime.strftime('%Y-%m-%d'))
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

def filterHash_listBlocks(listBlocks):
    """ Filter the blocks information to keep only hash
    
    Arguments:
        listBlocks {list} -- [description]
    
    Returns:
        list -- [description]
    """

    res = []
    for js in listBlocks[1]:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        res.append(currentblock)
    return res


def getList_txBlock(sc, block, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
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

    rdd = connectionToAPI(sc, host, path + block).filter(lambda lines: lines[0] == 'tx')\
        .map(lambda line: line[1])\
        .map(filter_tx)
    return rdd.collect()[0]


def filter_tx(data):
    """ Filter the transactions information to keep only date, value and id
    
    Arguments:
        data {list} -- [description]
    
    Returns:
        list -- [description]
    """

    tx_filter = []
    for json_tx in data:
        if 'inputs' in json_tx.keys():
            time = datetime.datetime.fromtimestamp(
                int(json_tx['time'])).strftime("%Y-%m-%d"'T'"%H:%M:%S")
            for json_inputs in json_tx['inputs']:
                if 'prev_out' in json_inputs.keys():
                    current = {}
                    current['date'] = time
                    current['id_tx'] = json_inputs['prev_out']['tx_index']
                    current['value'] = float(
                        json_inputs['prev_out']['value'])/100000000
                    tx_filter.append(current)
    return tx_filter

def add_historical_tx(historicalDataset):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_tx",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source": {
                "type": "historical",
                "value": data['value'],
                "time": {'path': data['date'], 'format':'%Y-%m-%dT%H:%M:%S'}
            }
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def insert_historical_tx(sc, start, end, conf):
    """ Puts the historical transactions into elasticsearch
    
    Arguments:
        sc {SparkContext} -- [description]
        start {string} -- [description]
        end {string} -- [description]
        conf {dict} -- [description]
    """

    connections.create_connection(hosts=conf['hosts'])
    list_hash_tx = getListBlocks_2dates(sc, start, end)
    try:
        eraseData("historical", ind="bitcoin_tx")
    except:
        logging.info("no data to erase!")
    for block in list_hash_tx:
        current_list_tx = getList_txBlock(sc, block['id_block'])
        add_historical_tx(current_list_tx)

if __name__ == "__main__":
    sc = SparkContext(master="local[2]", appName="BitcoinTransactionPrice")
    insert_historical_tx(sc, "2018-01-01", "2018-01-03", {"hosts": ["localhost"]})
