from http import client as httpClient
from http import HTTPStatus
from elasticsearch_dsl.connections import connections
import json
import pprint 

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
    list =[]
    for key,val in jsonData['bpi'].items():
        tempDic = {}
        tempDic['date'] = key
        tempDic['value'] = val
        list.append(tempDic)
    return list

def createCurrentDataset(jsonDataStream):
    currentDic = {}
    currentDic['date'] = jsonDataStream['time']['updatedISO']
    currentDic['value'] = jsonDataStream['bpi']['EUR']['rate_float']
    return currentDic

def main():
    #pp = pprint.PrettyPrinter(indent=DEFAULT_PP_INDENT)

    ''' Get data from the API between two dates '''
    jsonDataH = getDatePrice("2018-01-01","2018-02-01")
    historicalDataset = createHistoricalDataset(jsonDataH)
    
    ''' Get the current value from the API '''
    jsonDataStream = getCurrentPrice()
    currentDataset = createCurrentDataset(jsonDataStream)

    ''' Put the data into elasticsear '''
    connections.create_connection(hosts=['localhost'])
    #eraseData()
    storeData(historicalDataset)
    storeData(currentDataset)

if __name__ == "__main__":
    main()   