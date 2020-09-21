import atexit

from kvstore.loadbalancer import LoadBalancer
from kvstore.server import get_socket_fd
from kvstore.transport import get_socket_fd, unlink

if __name__ == '__main__':
    node_number = 0
    sockfd = get_socket_fd(node_number)
    atexit.register(unlink, sockfd)

    lb = LoadBalancer(node_number)

    try:
        lb.serve()
    except KeyboardInterrupt:
        lb.shutdown()
        unlink(sockfd)
