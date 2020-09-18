import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.store import KVStore
from kvstore.handlers import EntryPointHandler
from kvstore.encoding import BinaryEncoderDecoder
import socket


leader_node_num = 1

def make_server(node_number):
    leader = node_number == leader_node_num
    sockFd = get_socket_fd(node_number)

    server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
    server.kvstore = KVStore(node_number)
    server.en = BinaryEncoderDecoder()
    print("Starting server on node %s" % node_number)
    if leader:
        print("Starting as leader")
    else:
        print("Starting as follower")
        snapshot, socket = call_node_with_command(GetSnapshot(), 1)
        socket.close()
        server.kvstore.start_from_snapshot(snapshot)


    return server


def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)


def call_node_with_command(command, node_number):
    en = BinaryEncoderDecoder()
    sockFd = get_socket_fd(node_number)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sockFd)

    encoded = en.encode(command)
    print("sending %s" % encoded)
    s.send(encoded)

    total = b''
    while True:
        response = s.recv(1024)
        if len(response) == 0:
            break
        total += response

    return en.decode(total), s
