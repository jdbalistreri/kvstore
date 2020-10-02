import io
import os
import socket
import socketserver
import sys

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
            if command.enum == CommandEnum.PING:
                result = EmptyResponse()
            elif command.enum == CommandEnum.SET:
                result = self.server.server.set(command)
            elif command.enum == CommandEnum.REGISTER_FOLLOWER:
                result = self.server.server.registerFollower(command)
            elif command.enum == CommandEnum.WRITE_LOG:
                result = self.server.server.receiveWriteLog(command)
            elif command.enum == CommandEnum.SHUTDOWN:
                result = self.server.server.receive_shutdown_instruction(command)
            elif command.enum == CommandEnum.LB_REGISTRATION_INFO:
                result = self.server.server.receive_registration_info(command)
            elif command.enum == CommandEnum.ADD_NODE:
                result = self.server.server.add_node(command)
            elif command.enum == CommandEnum.REMOVE_NODE:
                result = self.server.server.remove_node(command)
            elif command.enum == CommandEnum.LIST_NODES:
                result = self.server.server.list_nodes(command)
            elif command.enum == CommandEnum.SNAPSHOT:
                result = self.server.server.receive_partial_update(command)

            return result
        except Exception as e:
            raise e
            return StringResponse(str(e))

    def handle(self):
        t = receive_data(self.server.server.en, self.request)
        result = self._handle(t)
        encoded = self.server.server.en.encode(result)
        send_data(self.server.server.en, self.request, encoded)
        return

def receive_data(en, sock):
    l_bytes = sock.recv(12)
    total_bytes, _ = en.decode_num(io.BytesIO(l_bytes), 12)

    total = b''
    while len(total) < total_bytes:
        response = sock.recv(1024)
        total += response
    return total

def send_data(en, sock, data):
    l = en.encode_num(len(data), 12)
    sock.sendall(l + data)

def call_node_with_command(command, node_number):
    en = BinaryEncoderDecoder()
    sockFd = get_socket_fd(node_number)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sockFd)

    encoded = en.encode(command)
    send_data(en, s, encoded)

    total = receive_data(en, s)

    return en.decode(total), s

def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)

def unlink(sockfd):
    if os.path.exists(sockfd):
        os.unlink(sockfd)
