import json
import pprint
import time
import datetime
from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch import helpers

'''ws = create_connection("ws://ws.blockchain.info/inv")
ws.send(json.dumps({
    "op": "unconfirmed_sub"
}))'''

CURRENT_TIMESTEMP = time.mktime(datetime.datetime.strptime("2018-03-21", "%Y-%m-%d").timetuple())
DEFAULT_HOST = "blockchain.info"
URI_BLOCKS = "/fr/blocks/"
URI_TRANSACTIONS = "/fr/rawblock/"
URI_SINGLE_TRANSACTION = "/fr/rawtx/"

def getDateListBlocks(date, host = DEFAULT_HOST, uri = URI_BLOCKS):
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

def filterDateListBlocks(listBlocks):
    res = []
    for js in listBlocks['blocks']:
        currentblock = {}
        currentblock['id_block'] = js['hash']
        res.append(currentblock)
    return res

def getTransactionsBlock(block, host=DEFAULT_HOST, path=URI_TRANSACTIONS):
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path + block)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def getTransaction(transac, host = DEFAULT_HOST, path = URI_SINGLE_TRANSACTION):
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path+str(transac))
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def transactionsFilter(data):
    tx_filter = []
    for js in data:
        time = datetime.datetime.fromtimestamp(js['time']).strftime('%Y-%m-%dT%H:%M:%S')
        for json in js['out']:
            current = {}
            current['time'] = time
            current['id_tx'] = json['tx_index']
            current['value'] = json['value']
            tx_filter.append(current)
    return tx_filter


def add_historical_tr(historicalDataset):
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin",
            "_type": "doc",
            "date": data['date'],
            "value": data['value'],
            "id_tr": data['id_tx'],
            "type": "historical"
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def main():
    
    pp = pprint.PrettyPrinter(indent=4)

    list_blocks_date = getDateListBlocks("2014-03-02")
    list_blocks_date_filter = filterDateListBlocks(list_blocks_date)
    for block in list_blocks_date_filter:
        tr = getTransactionsBlock(block['id_block'])['tx']
        hist_tr = transactionsFilter(tr)
        add_historical_tr(hist_tr)
        break

if __name__ == "__main__":
    main()
