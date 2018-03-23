from http import client as httpClient
from http import HTTPStatus
import json
import pprint 

DEFAULT_HOST = "api.coindesk.com"
DEFAULT_URI = "/v1/bpi/currentprice/EUR.json"
DEFAULT_PP_INDENT = 4

def getCurrentPrice(host = DEFAULT_HOST, uri = DEFAULT_URI):
    connection = httpClient.HTTPConnection(host)
    connection.request("GET", uri)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read())
    connection.close()
    return result

def main():
    pp = pprint.PrettyPrinter(indent=DEFAULT_PP_INDENT)
    jsonData = getCurrentPrice()
    pp.pprint(jsonData)

if __name__ == "__main__":
    main()    
