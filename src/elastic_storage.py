from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, Object, Integer, Date, Float, Text
from elasticsearch_dsl import Search

class BitCoin(DocType):
    """ Defines the mapping for ElasticSearch """
    date=Date()
    value=Float()
    type=Text

    class Meta:
        index = 'bitcoin'
    
    def save(self, ** kwargs):
        return super().save(** kwargs)

def storeData(d, v, t):
    """ Store data into the ElasticSearch db """
    BitCoin.init()
    b=BitCoin(date=d,value=v,type=t)
    b.save()

def eraseData(t, ind="bitcoin"):
    """ Erase date in the database by taking 2 args : type and index"""
    s = Search(index=ind).query("match", type=t)
    response = s.delete()
    print(response)

def main():
    
    # Defines a default Elasticsearch client
    connections.create_connection(hosts=['localhost'])


if __name__=='__main__':
    main()