import socket
import os

hostname = socket.gethostname()
HOST, PORT = "localhost", 9999

if hostname == 'alpha':
    INDEX_DIR = os.path.join('/','data-large', 'lucene', 'index')
else:
    INDEX_DIR = os.path.join('c:\\','lucene-3.4.0','index')
