import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.store import KVStore
from kvstore.handlers import EntryPointHandler
from kvstore.encoding import *
import socket


LEADER = 1

class Server:
    def __init__(self, node_number):
        self.node_number = node_number
        self.kvstore = KVStore(node_number)
        self.en = BinaryEncoderDecoder()
        self.is_leader = node_number == LEADER
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        print("Starting server on node %s" % node_number)
        if self.is_leader:
            print("Starting as leader")
        else:
            print("Starting as follower")
            self.follower_startup()

    def follower_startup(self):
        snapshot, socket = call_node_with_command(RegisterFollower(self.node_number), LEADER)
        socket.close()
        self.kvstore.start_from_snapshot(snapshot)

    def handle(self, data):
        try:
            command = self.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = StringResponse(self.kvstore.get(command.key))
        elif command.enum == CommandEnum.SET:
            result = StringResponse(self.kvstore.set(command.key, command.value))
        elif command.enum == CommandEnum.REGISTER_FOLLOWER:
            # store, logSequenceNumber = self.kvstore.get_snapshot()
            print(f"received follow registration from node {command.node_number}")
            # result = Snapshot(store, logSequenceNumber)
            result = Snapshot({}, 1)

        return result

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        server.socket.close()


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
