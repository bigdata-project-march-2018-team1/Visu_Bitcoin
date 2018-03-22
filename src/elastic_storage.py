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

<<<<<<< HEAD
def eraseData(t, ind="bitcoin"):
    """ Erase date in the database by taking 2 args : type and index"""
    s = Search(index=ind).query("match", type=t)
=======
def eraseData(index):
    """ Erase date in the database """
    s = Search(index=index).query("match", _index=index)
>>>>>>> 92a3b980f3b899d54e5d8f0f9689135481984d4d
    response = s.delete()
    print(response)

def main():
    
    # Defines a default Elasticsearch client
    connections.create_connection(hosts=['localhost'])
<<<<<<< HEAD

=======
    # Data are in a dictionary
    bitcoinDict = [{'date':'2018-01-01','valeur':6000.0},{'date':'2018-01-02','valeur':6030.0}]
    eraseData()
    # Calls the storage function
    storeData(bitcoinDict)
>>>>>>> 92a3b980f3b899d54e5d8f0f9689135481984d4d

if __name__=='__main__':
    main()
