import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.encoding import *
from kvstore.sockets import EntryPointHandler
import socket


class LoadBalancer:
    def __init__(self, node_number):
        self.node_number = node_number
        self.en = BinaryEncoderDecoder()
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        print("Starting loadbalancer")

    def handle(self, data):
        try:
            command = self.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = self.get(command)
        elif command.enum == CommandEnum.SET:
            result = self.set(command)
        elif command.enum == CommandEnum.REGISTER_FOLLOWER:
            result = self.registerFollower(command)
        elif command.enum == CommandEnum.WRITE_LOG:
            result = self.receiveWriteLog(command)

        return result

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.socket.close()
