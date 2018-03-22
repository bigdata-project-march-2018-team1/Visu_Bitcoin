from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import logging
from elastic_storage import storeData, eraseData, BitCoin

DEFAULT_HOST = "api.coindesk.com"
DEFAULT_URI = "/v1/bpi/currentprice/EUR.json"
DEFAULT_PP_INDENT = 4

def getCurrentPrice(host = DEFAULT_HOST, path = DEFAULT_URI):
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def getDatePrice(start, end, host = DEFAULT_HOST, path = DEFAULT_URI):
    connection = httpClient.HTTPConnection(host)
    path = "/v1/bpi/historical/EUR.json?start="+start+"&end="+end
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def createHistoricalDataset(jsonData):
    """Creates a list from the json data"""
    list = []
    for key,val in jsonData['bpi'].items():
        tempDic = {}
        tempDic['date'] = key+"T23:59:00"
        #print(tempDic['date'])
        tempDic['value'] = val
        list.append(tempDic)
    return list

def createCurrentDataset(jsonDataStream):
    currentDic = {}
    currentDic['date'] = jsonDataStream['time']['updatedISO']
    currentDic['value'] = jsonDataStream['bpi']['EUR']['rate_float']
    return currentDic

def add_historical_data(start, end):
    ''' Get data from the API between two dates '''
    # TODO use head request
    try:
        eraseData()
    except:
        logging.info("no data to erase! :(")
    jsonDataH = getDatePrice(start,end)
    historicalDataset = createHistoricalDataset(jsonDataH)

    '''call to bulk api to store the data'''
    actions=[
      {
        "_index": "bitcoin",
        "_type": "doc",
        "date": data['date'],
        "value": data['value']
      }
      for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def insertHistoricalDataInBase(conf):
    ''' Initializes the connection'''
    connections.create_connection(hosts=conf['hosts'])
   
    ''' Puts the historical data into elasticsearch '''
    add_historical_data("2010-07-17","2018-03-20")

if __name__ == "__main__":
    insertHistoricalDataInBase({"hosts": ["localhost"]})
