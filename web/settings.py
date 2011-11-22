import socket
import os

hostname = socket.gethostname()
HOST, PORT = "localhost", 9999

if hostname == 'alpha':
    hostname = 'production'
    INDEX_DIR = os.path.join('/','data-large', 'lucene', 'index')
    DEBUG=False
else:
    INDEX_DIR = os.path.join('c:\\','lucene-3.4.0','index')
    DEBUG=True