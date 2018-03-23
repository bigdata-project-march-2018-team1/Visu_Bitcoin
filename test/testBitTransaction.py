import sys, os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../src'))

import datetime

from BiTransaction import *

def testListBlocks_1day():
    """Testing the API
    """

    assert getListBlocks_1day("2012-01-01") != {}

def testListBlocks_between2dates():
    start = "2012-01-01"
    end = "2012-01-02"
    assert getListBlocks_between2dates(start,end) != {}

def testdateToDateTime():
    date = "2012-01-01"
    assert dateToDateTime(date) == datetime.datetime(2012,1,1)

def testFilterHash_listBlocks():
    listBlocks = { 'blocks': [{ 'hash': 'unit_test' }] }
    assert filterHash_listBlocks(listBlocks) == [{'id_block':'unit_test'}]

def testList_txBlock():
    block = '0000000000000bae09a7a393a8acded75aa67e46cb81f7acaa5ad94f9eacd103'
    assert getList_txBlock(block) != {}

def testFilter_tx():
    date = "2012-01-01"
    timestamp = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    data = [{ 'inputs':
        [{'prev_out': {
            'tx_index':'unit_test_tx_index',
            'value':'unit_test_value'
        }}],
        'time':timestamp
    }]
    results = [{ 'date':date+"T00:00:00",
                'id_tx':'unit_test_tx_index',
                'value':'unit_test_value'
    }]
    assert filter_tx(data) == results

def testFilter_listeBlocks():
    blocks = [ { 'blocks': [{ 'hash': 'unit_test1' }] }, { 'blocks': [{ 'hash': 'unit_test2' }] }]
    results = [{'id_block':'unit_test1'},{'id_block':'unit_test2'}]
    assert filter_listeBlocks(blocks) == results