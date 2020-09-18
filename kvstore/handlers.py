import socketserver

from kvstore.input import InputValidationError
from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.store import KVStore
from kvstore.encoding import *
import socket


leader_node_num = 1

def make_server(node_number, leader):
    sockFd = get_socket_fd(node_number)

    server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
    server.kvstore = KVStore(node_number)
    server.en = BinaryEncoderDecoder()
    print("Starting server on node %s" % node_number)
    if leader:
        print("Starting as leader")
    else:
        print("Starting as follower")
        snapshot, socket = get_command_from_command(GetSnapshot(), 1)
        socket.close()
        server.kvstore.start_from_snapshot(snapshot)


    return server, sockFd


def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)


def get_command_from_command(command, node_number):
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

class EntryPointHandler(socketserver.BaseRequestHandler):
    def getResult(self, data):
        try:
            command = self.server.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = StringResponse(self.server.kvstore.get(command.key))
        elif command.enum == CommandEnum.SET:
            result = StringResponse(self.server.kvstore.set(command.key, command.value))
        elif command.enum == CommandEnum.GET_SNAPSHOT:
            store, logSequenceNumber = self.server.kvstore.get_snapshot()
            print("got snapshot!")
            result = Snapshot(store, logSequenceNumber)

        return result

    def handle(self):
        data = self.request.recv(1024)
        result = self.getResult(data)
        encoded = self.server.en.encode(result)
        self.request.send(encoded)
        return
