from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, Object, Integer, Date, Float
from elasticsearch_dsl import Search

class BitCoin(DocType):
    """ Defines the mapping for ElasticSearch """
    date=Date()
    value=Float()

    class Meta:
        index = 'bitcoin'
    
    def save(self, ** kwargs):
        return super().save(** kwargs)

def storeData(data):
    """ Store data into the ElasticSearch db """
    BitCoin.init()
    b=BitCoin(hist=data)
    b.save()

def eraseData():
    """ Erase date in the database """
    s = Search(index='bitcoin').query("match", _index="bitcoin")
    response = s.delete()
    print(response)

def main():
    
    # Defines a default Elasticsearch client
    connections.create_connection(hosts=['localhost'])

    # Data are in a dictionary
    bitcoinDict = [{'date':'2018-01-01','valeur':6000.0},{'date':'2018-01-02','valeur':6030.0}]

    #eraseData()

    # Calls the storage function
    storeData(bitcoinDict)

if __name__=='__main__':
    main()