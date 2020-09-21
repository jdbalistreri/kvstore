import os
import socket
import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.encoding import *

class EntryPointHandler(socketserver.BaseRequestHandler):
    def _handle(self, data):
        try:
            try:
                command = self.server.server.en.decode(data)
            except InputValidationError as e:
                return str(e)

            if command.enum == CommandEnum.GET:
                result = self.server.server.get(command)
            elif command.enum == CommandEnum.SET:
                result = self.server.server.set(command)
            elif command.enum == CommandEnum.REGISTER_FOLLOWER:
                result = self.server.server.registerFollower(command)
            elif command.enum == CommandEnum.WRITE_LOG:
                result = self.server.server.receiveWriteLog(command)

            return result
        except Exception as e:
            return StringResponse(str(e))

    def handle(self):
        data = self.request.recv(1024)
        result = self._handle(data)
        encoded = self.server.server.en.encode(result)
        self.request.send(encoded)
        return

def call_node_with_command(command, node_number):
    en = BinaryEncoderDecoder()
    sockFd = get_socket_fd(node_number)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sockFd)

    encoded = en.encode(command)
    s.send(encoded)

    total = b''
    while True:
        response = s.recv(1024)
        if len(response) == 0:
            break
        total += response

    return en.decode(total), s

def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)

def unlink(sockfd):
    if os.path.exists(sockfd):
        os.unlink(sockfd)
