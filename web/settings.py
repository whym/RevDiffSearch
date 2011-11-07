<<<<<<< HEAD
import socket
import os

hostname = socket.gethostname()
HOST, PORT = "localhost", 9999

if hostname == 'alpha':
    hostname = 'production'
    INDEX_DIR = os.path.join('/','data', 'lucene', 'index')
else:
    INDEX_DIR = os.path.join('c:\\','lucene-3.4.0','index')
=======
import socket
import os

hostname = socket.gethostname()
HOST, PORT = "localhost", 9999

if hostname == 'alpha':
    hostname = 'production'
    INDEX_DIR = os.path.join('/','data-large', 'lucene', 'index')
else:
    INDEX_DIR = os.path.join('c:\\','lucene-3.4.0','index')
>>>>>>> 5e19f843dcb117296a65a5df26fd381cff84762c
