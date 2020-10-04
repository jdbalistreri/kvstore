import socket
import socketserver
import sys

from kvstore.store import KVStore
from kvstore.constants import LB_NODE
from kvstore.encoding import *
from kvstore.partitioning import PartitionManager
from kvstore.transport import get_socket_fd, EntryPointHandler, call_node_with_command


class Server:
    def __init__(self, node_number):
        self.node_number = node_number
        self.store = KVStore(node_number)
        self.en = BinaryEncoderDecoder()
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        print("starting server on node %s" % node_number)
        print("registering with load balancer")
        try:
            command, s = call_node_with_command(self._register_command(), LB_NODE)
            s.close()
        except FileNotFoundError:
            print("could not reach the load balancer")
            self.shutdown()

        self.receive_registration_info(command, startup=True)

    def _register_command(self):
        return RegisterFollower(self.node_number, self.store.log_sequence_number)

    def follower_startup(self):
        command, socket = call_node_with_command(self._register_command(), self.leader_node)
        socket.close()
        if command.enum == CommandEnum.SNAPSHOT:
            self.store.start_from_snapshot(command)
        else:
            self.store.start_from_write_logs(command)

    def get(self, command):
        return StringResponse(self.store.get(command.key))

    def receive_shutdown_instruction(self, command):
        print("received instruction to shut down. Initiating shut down...")
        self.shutdown()

    def receive_partial_update(self, command):
        print(f"receiving partial update of {command.store}")
        self.store.add_partial_update(command.store)
        return EmptyResponse()

    def receive_registration_info(self, command, startup=False):
        print("received routing info")
        self.pm = PartitionManager(is_lb=False, ring=command.ring)
        if startup:
            return

        to_send = {}
        for key in self.store.store.keys():
            destinations = self.pm.lookup(key)
            primary = destinations[0]
            send_to_node = to_send.get(primary, {})
            send_to_node[key] = self.store.store[key]
            to_send[primary] = send_to_node

        to_keep = {}

        for k, v in to_send.items():
            if k == self.node_number:
                print(f"keeping {v}")
                to_keep = v
            else:
                print(f"sending {v} to node {k}")
                _, s = call_node_with_command(Snapshot(v), k)
                s.close()

        # remove values we've send to other nodes once those requests succeed
        self.store.reset_store_to_subset(to_keep)

        # TODO: handle catching up on any missed writes
        # self.follower_startup()

        return StringResponse("")

    def set(self, command):
        write_log = self.store.set(command.key, command.value)
        # TODO: make these calls asynchronous

        # TODO: make this replicate based on the ring

        # for f_id in [x for x in self.followers if x != self.node_number]:
        #     try:
        #         _, s = call_node_with_command(write_log, f_id)
        #         s.close()
        #
        #         print(f"shipping write log {self.store.log_sequence_number} to follower {f_id}")
        #     except FileNotFoundError:
        #         # TODO: currently assuming an unreachable node is dead. could add
        #         # more sophisticated health-check handling here
        #         print(f"unable to reach node {f_id}. marking as dead")
        #         self.followers.remove(f_id)

        return StringResponse(write_log.value)

    def registerFollower(self, command):
        # if not self.is_leader:
        #     raise Exception("Only the leader can register followers")

        # self.followers.add(command.node_number)
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
            return Snapshot(store)

    def receiveWriteLog(self, command):
        print(f"received write log {command.log_sequence_number}")
        self.store.receive_write_log(command)

        return EmptyResponse()

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        print("shutting down server")
        self.server.socket.close()
        sys.exit(0)
