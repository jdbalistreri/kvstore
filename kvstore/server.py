import socketserver

from kvstore.store import KVStore
from kvstore.constants import LEADER_NODE
from kvstore.encoding import *
from kvstore.transport import get_socket_fd, EntryPointHandler, call_node_with_command
import socket


class Server:
    def __init__(self, node_number):
        self.node_number = node_number
        self.store = KVStore(node_number)
        self.en = BinaryEncoderDecoder()
        self.is_leader = node_number == LEADER_NODE
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
        command, socket = call_node_with_command(RegisterFollower(self.node_number, self.store.log_sequence_number), LEADER_NODE)
        socket.close()
        if command.enum == CommandEnum.SNAPSHOT:
            self.store.start_from_snapshot(command)
        else:
            self.store.start_from_write_logs(command)

    def get(self, command):
        return StringResponse(self.store.get(command.key))

    def set(self, command):
        write_log = self.store.set(command.key, command.value)
        # TODO: make these calls asynchronous
        for f_id in list(self.followers):
            try:
                call_node_with_command(write_log, f_id)
                print(f"shipping write log to follower {f_id}")
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

        if command.log_sequence_number == self.store.log_sequence_number:
            print("follower already caught up")
            return WriteLogs([])
        elif command.log_sequence_number + 5 >= self.store.log_sequence_number:
            print("follower slightly behind: shipping write logs")
            write_logs = self.store.get_write_logs_since(command.log_sequence_number)
            return WriteLogs(write_logs)
        else:
            print("follower far behind: sending snapshot")
            store, log_sequence_number = self.store.get_snapshot()
            return Snapshot(store, log_sequence_number)

    def receiveWriteLog(self, command):
        print(f"received write log {command}")
        self.store.receive_write_log(command)

        return StringResponse("ack")

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.socket.close()
