import random
import socket
import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.encoding import *
from kvstore.partitioning import PartitionManager
from kvstore.transport import EntryPointHandler, call_node_with_command, get_socket_fd


class LoadBalancer:
    def __init__(self, node_number, automatic_failover, replica_count=3):
        self.node_number = node_number
        self.automatic_failover = automatic_failover
        self.leader = None
        self.pm = PartitionManager()
        self.followers = set()
        self.replica_count = replica_count

        self.en = BinaryEncoderDecoder()
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        print("starting loadbalancer")

    def registerFollower(self, command):
        if not self.leader:
            print(f"registered node {command.node_number} as the leader")
            self.leader = command.node_number
        else:
            print(f"registered node {command.node_number} as a follower")

        self.followers.add(command.node_number)
        return LBRegistrationInfo(self.leader, self.followers)

    def add_node(self, command):
        print(f"adding node {command.node_num}")
        return StringResponse(self.pm.add_node(command.node_num))

    def list_nodes(self, command):
        return StringResponse(f"{self.pm.nodes}")

    def remove_node(self, command):
        print(f"removing node {command.node_num}")
        return StringResponse(self.pm.remove_node(command.node_num))

    def get_routing_info(self, key):
        nodes = self.pm.lookup(key)
        print(f"would route to {nodes}")

    def get(self, command):
        self.get_routing_info(command.key)

        if len(self.followers) == 0:
            return StringResponse("no read nodes available")

        get_node = self._choose_read_node()
        print(f"reading from node {get_node}")
        try:
            resp, socket = call_node_with_command(command, get_node)
            socket.close()
            return resp
        except FileNotFoundError:
            print(f"unable to reach node {get_node}. marking as dead")
            self.followers.remove(get_node)
            return self.get(command)

    def _choose_read_node(self):
        # could add options for different types of load balancing here (e.g.
        # round robin, least loaded, etc.)
        return random.sample(self.followers, 1)[0]

    def set(self, command):
        self.get_routing_info(command.key)

        if self.leader == None:
            return StringResponse("no write nodes available")

        try:
            resp, socket = call_node_with_command(command, self.leader)
            socket.close()
        except FileNotFoundError:
            return self._handle_dead_leader(command)

        print(f"writing to node {self.leader}")
        return resp

    def _handle_dead_leader(self, command):
        print(f"unable to reach node {self.leader}. marking as dead")
        self.followers.remove(self.leader)
        self.leader = None
        if not self.automatic_failover or len(self.followers) == 0:
            return StringResponse("No write nodes available")

        print(f"starting automatic failover")

        new_leader = random.sample(self.followers, 1)[0]
        print(f"node {new_leader} elected as new leader")

        self.leader = new_leader
        updated_info = LBRegistrationInfo(new_leader, self.followers)
        for f_id in self.followers:
            print(f"notifying node {f_id} of leadership change")
            _, s = call_node_with_command(updated_info, f_id)
            s.close()

        print("automatic failover complete")

        return self.set(command)

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        print("beginning load balancer shutdown")
        for f_id in self.followers:
            print(f"shutting down node {f_id}")
            _, s = call_node_with_command(Shutdown(), f_id)
            s.close()
        self.server.socket.close()

        print("completed load balancer shutdown")
