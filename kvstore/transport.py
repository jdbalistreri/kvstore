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

            return result
        except Exception as e:
            raise e
            return StringResponse(str(e))

    def handle(self):
        print("handling incoming connection")
        # TODO: this is a hack, but i was having trouble recv-ing multiple times
        # on the request
        import pdb; pdb.set_trace()
        t = self.request.recv(999999999)
        print("decoded incoming binary")
        result = self._handle(t)
        print("handled incoming binary")
        encoded = self.server.server.en.encode(result)
        print("sending response")
        self.request.sendall(encoded)
        print("done sending response")
        return

def receive_data(sock):
    total = b''
    while True:
        print("aaaa")
        response = sock.recv(1024)
        if len(response) == 0:
            break
        total += response
    return total

def call_node_with_command(command, node_number):
    print("calling with command")
    en = BinaryEncoderDecoder()
    sockFd = get_socket_fd(node_number)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sockFd)

    encoded = en.encode(command)
    print("sending all data")
    s.sendall(encoded)

    print("receiving response")
    total = receive_data(s)

    print("returning")
    return en.decode(total), s

def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)

def unlink(sockfd):
    if os.path.exists(sockfd):
        os.unlink(sockfd)
