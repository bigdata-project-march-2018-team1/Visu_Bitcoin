import json
import pprint
import time
import datetime
import logging

from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import timedelta as td
from elastic_storage import eraseData

'''ws = create_connection("ws://ws.blockchain.info/inv")
ws.send(json.dumps({
    "op": "unconfirmed_sub"
}))'''

CURRENT_TIMESTEMP = time.mktime(datetime.datetime.strptime(str(datetime.date.today()), "%Y-%m-%d").timetuple())
DEFAULT_HOST = "blockchain.info"
URI_BLOCKS = "/fr/blocks/"
URI_TRANSACTIONS = "/fr/rawblock/"
URI_SINGLE_TRANSACTION = "/fr/rawtx/"

# TODO find better names!
ES_TYPE = "bitcoin_tx"
ES_INDEX = "bitcoin_tx"
ES_SUBTYPE = "historical"

def getListBlocks_1day(date, host = DEFAULT_HOST, uri = URI_BLOCKS):
    """Get blocks information for a date
    
    Arguments:
        date {[string]} -- [date]
    
    Keyword Arguments:
        host {[string]} -- [API host] (default: {DEFAULT_HOST})
        uri {[string]} -- [API uri] (default: {URI_BLOCKS})
    
    Returns:
        [dic] -- [dictionnary contains blocks information]
    """

    timestemp = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))*1000
    path = uri + str(timestemp) + "?format=json"
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def dateToDateTime(date):
    """ Convert string date to timestamp date
    
    Arguments:
        date {string} -- date
    
    Returns:
        timestamp -- date
    """
    timestemp = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    return datetime.datetime.fromtimestamp(timestemp)

def getListBlocks_between2dates(start, end, host = DEFAULT_HOST, uri = URI_BLOCKS):
    """Get blocks information between two dates
    
    Arguments:
        start {[string]} -- [date start]
        end {[string]} -- [date start]
    
    Keyword Arguments:
        host {[string]} -- [API host] (default: {DEFAULT_HOST})
        uri {[string]} -- [API uri] (default: {URI_BLOCKS})
    
    Returns:
        [dic] -- [dictionnary contains blocks information]
    """

    blocks_list = []
    blocks_list.append(getListBlocks_1day(start))
    start_datetime = dateToDateTime(start)
    current_dateTime = start_datetime + td(days=1)
    end_datetime = dateToDateTime(end)
    while current_dateTime <= end_datetime:
        blocks_list.append(getListBlocks_1day(current_dateTime.strftime('%Y-%m-%d')))
        current_dateTime += td(days=1)
    return blocks_list

def filterHash_listBlocks(listBlocks):
    """ Filter blocks information to keep hash only
    
    Arguments:
        listBlocks {list} -- [description of blocks]
    
    Returns:
        [type] -- [hash blocks list]
    """

    res = []
    for js in listBlocks['blocks']:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        res.append(currentblock)
    return res

def getList_txBlock(block, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
    """ Get all informations transaction for a hash block
    
    Arguments:
        block {string} -- [hash block]
    
    Keyword Arguments:
        host {[string]} -- [API host] (default: {DEFAULT_HOST})
        path {[string]} -- [API path] (default: {URI_TRANSACTIONS})
    
    Returns:
        [list] -- [all informations transaction]
    """

    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path + block)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def filter_tx(data):
    """ Filter of transactions information to keep only date, identification and value
    
    Arguments:
        data {[list]} -- [all informations transaction]
    
    Returns:
        [list] -- [transactions date, identification and value for a block]
    """

    tx_filter = []
    for js in data:
        if 'inputs' in js.keys():
            time = datetime.datetime.fromtimestamp(js['time']).strftime('%Y-%m-%dT%H:%M:%S')
            for json in js['inputs']:
                current = {}
                current['date'] = time
                current['id_tx'] = json['prev_out']['tx_index']
                current['value'] = json['prev_out']['value']
                tx_filter.append(current)
    return tx_filter


def add_historical_tx(historicalDataset):
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": ES_INDEX, 
            "_type": ES_TYPE,
            "date": data['date'],
            "value": data['value'],
            "id_tx": data['id_tx'],
            "type": ES_SUBTYPE
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def unroll(gen):
    for items in gen:
        for item in items:
            yield item

def filter_listeBlocks(blocks):
    list_blocks = []
    for blk in blocks:
        list_blocks.append(filterHash_listBlocks(blk))
    hash_tx = []
    for tx in unroll(list_blocks):
        hash_tx.append(tx)
    return hash_tx

def insert_historical_tx(start, end, conf):
    connections.create_connection(hosts=conf['hosts'])
    try:
        eraseData(ES_SUBTYPE, ES_INDEX)
    except:
        pass
    list_blocks_2dates = getListBlocks_between2dates(start,end)
    list_hash_tx = filter_listeBlocks(list_blocks_2dates)
    for block in list_hash_tx:
        logging.info(block)
        tx = getList_txBlock(block['id_block']).get("tx",{})
        if tx != {}:
            hist_tx = filter_tx(tx)
        add_historical_tx(hist_tx)

if __name__ == "__main__":
    from sys import argv

    logging.basicConfig(level=logging.INFO)

    start_date = argv[0]
    end_date = argv[1]

    insert_historical_tx(start_date, end_date, {"hosts": ["db"]})
