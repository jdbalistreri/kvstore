import os
import sys

from kvstore.handlers import make_server

if __name__ == '__main__':
    try:
        node_number = int(sys.argv[1])
    except IndexError:
        node_number = 1

    server, sockfd = make_server(node_number)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
        os.unlink(sockfd)
