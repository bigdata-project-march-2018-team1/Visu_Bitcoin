import json
import datetime
import logging

from http import client as httpClient
from http import HTTPStatus

from elasticsearch_dsl.connections import connections
from elasticsearch import helpers

from elastic_storage import storeData, eraseData, BitCoin
from elastic_helper import http_auth

DEFAULT_HOST = "api.coindesk.com"
DEFAULT_URI = "/v1/bpi/currentprice/EUR.json"
DEFAULT_URI_DATE = "/v1/bpi/historical/close.json?currency=EUR"
DEFAULT_PP_INDENT = 4

def connectionToAPI(host, path):
    """ Connection to the API
    
    Arguments:
        host {string} -- [description]
        path {string} -- [description]
    
    Returns:
        [json] -- [File with Bitcoin value and other informations]
    """

    connection = httpClient.HTTPConnection(host)
    connection.request("GET", path)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def getCurrentPrice(host=DEFAULT_HOST, path=DEFAULT_URI):
    """ Get the current Bitcoin price 
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        path {string} -- [description] (default: {DEFAULT_URI})
    
    Returns:
        [json] -- [description]
    """

    return connectionToAPI(host, path)

def getHistoricalPrice(start, end, host=DEFAULT_HOST, path=DEFAULT_URI_DATE):
    """ Call the API to get all the bitcoin values between two dates
    
    Arguments:
        start {string} -- [description]
        end {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        path {string} -- [description] (default: {DEFAULT_URI_DATE})
    
    Returns:
        json -- [description]
    """

    return connectionToAPI(host, path+"&start="+start+"&end="+end)

def createHistoricalDataset(jsonData):
    """ Creates a list from the json data
    
    Arguments:
        jsonData {json} -- [description]
    
    Returns:
        list -- [description]
    """

    list = []
    for key, val in jsonData['bpi'].items():
        tempDic = {}
        tempDic['date'] = key+"T23:59:00"
        tempDic['value'] = val
        list.append(tempDic)
    return list

def createCurrentDataset(jsonDataStream):
    """ Creates a list from the json data
    
    Arguments:
        jsonDataStream {json} -- [description]
    
    Returns:
        json -- [description]
    """

    currentDic = {}
    currentDic['date'] = jsonDataStream['time']['updatedISO']
    currentDic['value'] = jsonDataStream['bpi']['EUR']['rate_float']
    return currentDic

def addHistoricalDataset(start, end):
    """ Add data from the API between two dates to Elastic
    
    Arguments:
        start {string} -- [description]
        end {string} -- [description]
    """

    # TODO use head request
    try:
        eraseData("historical", ind="bitcoin_price")
    except:
        logging.info("no data to erase!")
    jsonDataH = getHistoricalPrice(start, end)
    historicalDataset = createHistoricalDataset(jsonDataH)
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_price",
            "_type": "doc",
            "date": data['date'],
            "value": data['value'],
            "type": "historical"
        }
        for data in historicalDataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def insertHistoricalDataInBase(conf):
    ''' Initializes the connection'''
    #TODO password
    connections.create_connection(hosts=conf['elasticsearch']['hosts'], http_auth=http_auth(conf['elasticsearch']))
    ''' Puts the historical data into elasticsearch '''
    addHistoricalDataset("2010-07-17", str(datetime.date.today()))

if __name__ == "__main__":
    import config
    insertHistoricalDataInBase(config)