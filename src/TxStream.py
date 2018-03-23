from websocket import create_connection
import datetime
import json
from BiTransaction import add_historical_tx

def filter_tx(data):
    """ Filter of transactions information to keep only date, identification and value
    
    Arguments:
        data {[list]} -- [all informations transaction]
    
    Returns:
        [list] -- [transactions date, identification and value for a block]
    """
    if 'inputs' in data['x'].keys():
        tx_filter = []
        time = datetime.datetime.fromtimestamp(data['x']['time']).strftime('%Y-%m-%dT%H:%M:%S')
        for json in data['x']['inputs']:
            if 'prev_out' in json.keys():
                current = {}
                current['date'] = time
                current['id_tx'] = json['prev_out']['tx_index']
                current['value'] = json['prev_out']['value']
                tx_filter.append(current)       
    return tx_filter

def unroll(gen):
    for items in gen:
        for item in items:
            yield item

ws = create_connection("ws://ws.blockchain.info/inv")
while True:
    ws.send(json.dumps({
        "op": "unconfirmed_sub"
    }))
    print("INFO")
    lines = ws.recv()
    obj = json.loads(lines)
    fil = filter_tx(obj)
    add_historical_tx(fil)