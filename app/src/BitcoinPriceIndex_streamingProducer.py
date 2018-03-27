import logging
import socket
import sys
import json
import time
import random
from json import JSONEncoder

from BitcoinPriceIndex_historical import getCurrentPrice, createCurrentDataset

def send_to_spark(current, tcp_connection,s):
    """
    Send current price to SparkStreaming and is "client-crush"-proof.
    """
    current_json = JSONEncoder().encode(current)

    try:
        tcp_connection.send((current_json+'\n').encode())
    except ConnectionAbortedError:
        logging.error("Connection failed!")
        logging.info("Waiting for a new TCP connection...")
        time.sleep(10)
        conn, _ = s.accept()
        send_to_spark(current,conn,s)
        logging.info("Connected... Starting getting current price.")

def produce_stream_current(tcp_ip = "localhost",tcp_port = 9002):
    """
    Create a socket which is listening on hostname:port and send the current price to SparkStreaming client.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((tcp_ip, tcp_port))
    s.listen(1)

    logging.info("Waiting for TCP connection...")
    conn, _ = s.accept()
    
    logging.info("Connected... Starting getting current price.")
    while True:
        time.sleep(50)
        last_current = createCurrentDataset(getCurrentPrice())
        send_to_spark(last_current,conn,s)

if __name__ == "__main__":
    produce_stream_current()
