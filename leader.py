import socketserver
import os

from kvstore.handlers import make_server

if __name__ == '__main__':
    server, sockfd = make_server(node_number=1, leader=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
        os.unlink(sockfd)
