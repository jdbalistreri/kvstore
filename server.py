import atexit
import os
import sys

from kvstore.server import make_server, get_socket_fd

def unlink(sockfd):
    if os.path.exists(sockfd):
        os.unlink(sockfd)

if __name__ == '__main__':
    try:
        node_number = int(sys.argv[1])
    except IndexError:
        node_number = 1

    sockfd = get_socket_fd(node_number)
    atexit.register(unlink, sockfd)
    server = make_server(node_number)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
        os.unlink(sockfd)
