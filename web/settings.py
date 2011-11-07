import socket
import os

HOSTNAME = socket.gethostname()
HOST, PORT = "localhost", 9999

if HOSTNAME == 'alpha':
    HOSTNAME = 'production'
    INDEX_DIR = os.path.join('/','data-large', 'lucene', 'index')
else:
    INDEX_DIR = os.path.join('c:\\','lucene-3.4.0','index')
