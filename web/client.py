import socket
import sys
import cPickle

HOST, PORT = "localhost", 9999
data = " ".join(sys.argv[1:])

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data
    data = '0.1'
    sock.connect((HOST, PORT))
    sock.send(data)

    # Receive data from the server and shut down
    received = sock.recv(1024)
    result = cPickle.loads(received)
finally:
    sock.close()

print "Sent:     {}".format(data)
print "Received: {}".format(result)