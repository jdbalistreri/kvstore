import socketserver

from kvstore.input import InputValidationError
from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.store import KVStore
from kvstore.encoding import *


def make_server(node_number, leader):
    sockFd = ENTRYPOINT_SOCKET + str(node_number)

    server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
    server.kvstore = KVStore(node_number)
    server.en = BinaryEncoderDecoder()
    print("Starting server on node %s" % node_number)
    if leader:
        print("Starting as leader")
    else:
        print("Starting as follower")


    return server, sockFd


class EntryPointHandler(socketserver.BaseRequestHandler):
    def getResult(self, data):
        try:
            command = self.server.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = self.server.kvstore.get(command.key)
        elif command.enum == CommandEnum.SET:
            result = self.server.kvstore.set(command.key, command.value)

        return result

    def handle(self):
        data = self.request.recv(1024)
        result = self.getResult(data)
        self.request.send(result.encode(encoding='UTF-8'))
        return
