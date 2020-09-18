import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.store import KVStore
from kvstore.encoding import *
import socket


LEADER = 1

class Server:
    def __init__(self, node_number):
        self.node_number = node_number
        self.store = KVStore(node_number)
        self.en = BinaryEncoderDecoder()
        self.is_leader = node_number == LEADER
        self.followers = set()
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
        self.store.start_from_snapshot(snapshot)

    def get(self, command):
        return StringResponse(self.store.get(command.key))

    def set(self, command):
        write_log = self.store.set(command.key, command.value)
        for f_id in list(self.followers):
            try:
                call_node_with_command(write_log, f_id)
                print(f"shipping write log {write_log} to follower {f_id}")
            except FileNotFoundError:
                # TODO: currently assuming an unreachable node is dead. could add
                # more sophisticated health-check handling here
                print(f"unable to reach node {f_id}. marking as dead")
                self.followers.remove(f_id)

        return StringResponse(write_log.value)

    def registerFollower(self, command):
        if not self.is_leader:
            raise Exception("Only the leader can register followers")

        self.followers.add(command.node_number)
        print(f"added follower node {command.node_number}")

        store, logSequenceNumber = self.store.get_snapshot()
        return Snapshot(store, logSequenceNumber)

    def receiveWriteLog(self, command):
        print(f"received write log {command}")
        self.store.receive_write_log(command)

        return StringResponse("ack")

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


def get_socket_fd(node_num):
    return ENTRYPOINT_SOCKET + str(node_num)


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


class EntryPointHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        result = self.server.server.handle(data)
        encoded = self.server.server.en.encode(result)
        self.request.send(encoded)
        return
