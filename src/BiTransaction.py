import json
import pprint
import time
import datetime
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

def getListBlocks_1day(date, host = DEFAULT_HOST, uri = URI_BLOCKS):
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
    timestemp = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    return datetime.datetime.fromtimestamp(timestemp)

def getListBlocks_between2dates(start, end, host = DEFAULT_HOST, uri = URI_BLOCKS):
    blocks_list = []
    blocks_list.append(getListBlocks_1day(start))
    start_datetime = dateToDateTime(start)
    current_dateTime = start_datetime + td(days=1)
    end_timestemp = dateToDateTime(end)
    while current_dateTime != end_timestemp:
        blocks_list.append(getListBlocks_1day(current_dateTime.strftime('%Y-%m-%d')))
        current_dateTime += td(days=1)
    return blocks_list

def filterHash_listBlocks(listBlocks):
    res = []
    for js in listBlocks['blocks']:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        res.append(currentblock)
    return res

def getList_txBlock(block, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path + block)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def filter_tx(data):
    tx_filter = []
    for js in data[1:]:
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
            "_index": "bitcoin_tx",
            "_type": "doc",
            "date": data['date'],
            "value": data['value'],
            "id_tx": data['id_tx'],
            "type": "historical"
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
    eraseData()
    list_blocks_2dates = getListBlocks_between2dates(start,end)
    list_hash_tx = filter_listeBlocks(list_blocks_2dates)
    for block in list_hash_tx:
        tx = getList_txBlock(block['id_block'])['tx']
        hist_tx = filter_tx(tx)
        add_historical_tx(hist_tx)

if __name__ == "__main__":
    insert_historical_tx("2018-01-01", "2018-03-22", {"hosts": ["localhost"]})