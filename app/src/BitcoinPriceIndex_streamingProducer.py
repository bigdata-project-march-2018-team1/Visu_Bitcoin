import socket
import sys
import json
import time
import random
from json import JSONEncoder

from BitcoinPriceIndex_historical import getCurrentPrice,createCurrentDataset

def send_to_spark(current, tcp_connection,s):
    """
    Send current price to SparkStreaming and is "client-crush"-proof.

    Arguments:
        current {string} -- [description]
        tcp_connection {string} -- [description]
        s {socket} -- socket of connection

    Returns:
        [void] -- []
    """
    current_json = JSONEncoder().encode(current)

    try:
        tcp_connection.send((current_json+'\n').encode())
    except ConnectionAbortedError:
        print("Connection failed!")
        print("Waiting for a new TCP connection...")
        time.sleep(10)
        conn, address = s.accept()
        logging.info("Connection from client {0}".format(address))
        send_to_spark(current,conn,s)
        print("Connected... Starting getting current price.")

def produce_stream_current(bind_address, bind_port):
    """
    Create a socket which is listening on hostname:port and send the current price to SparkStreaming client.

    Arguments:
        bind_address {string} -- [description]
        bind_port {int} -- [description]

    Returns:
        [void] -- []
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((bind_address, bind_port))
    s.listen(1)

    print("Waiting for TCP connection...")
    conn, _ = s.accept()

    print("Connected... Starting getting current price.")
    while True:
        time.sleep(50)
        last_current = createCurrentDataset(getCurrentPrice())
        send_to_spark(last_current,conn,s)

if __name__ == "__main__":
    from config import config
    produce_stream_current(bind_address='0', bind_port=config['price_streaming']['port'])

