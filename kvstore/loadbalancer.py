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
        self.pm = PartitionManager(is_lb=True)
        self.replica_count = replica_count

        self.en = BinaryEncoderDecoder()
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        print("starting loadbalancer")

    def registerFollower(self, command):
        print(f"node {command.node_number} is online")
        return LBRegistrationInfo(self.pm.ring)

    def add_node(self, command):
        print(f"adding node {command.node_num}")
        try:
            resp, socket = call_node_with_command(Ping(), command.node_num)
            socket.close()
        except FileNotFoundError:
            return StringResponse(f"Node {command.node_num} must be available before it can be added")

        try:
            resp = self.pm.add_node(command.node_num)
        except ValueError as e:
            return StringResponse(str(e))

        self.distribute_routing_info()

        return StringResponse(resp)

    def distribute_routing_info(self, removed_node=None):
        print("distributing routing info")
        routing_command = LBRegistrationInfo(self.pm.ring)
        removed_node_ls = [removed_node] if removed_node else []
        for node in list(self.pm.nodes) + removed_node_ls:
            try:
                print(f"updating node {node}")
                _, socket = call_node_with_command(routing_command, node)
                socket.close()
            except (FileNotFoundError):
                print(f"couldn't reach node {node}")

    def list_nodes(self, command):
        return StringResponse(f"{self.pm.nodes}")

    def remove_node(self, command):
        print(f"removing node {command.node_num}")

        # remove the node from the partition manager
        try:
            resp = self.pm.remove_node(command.node_num)
        except ValueError as e:
            return StringResponse(str(e))

        # distribute the update to all nodes
        self.distribute_routing_info(removed_node = command.node_num)

        # shutdown the removed node
        self.shutdown_node(command.node_num)

        return StringResponse(resp)

    def get_routing_info(self, key):
        nodes = self.pm.lookup(key)
        print(f"would route to {nodes}")
        return nodes

    def get(self, command):
        routing_info = self.get_routing_info(command.key)
        if len(routing_info) == 0:
            return StringResponse("no nodes in the ring")

        primary = routing_info[0]
        print(f"reading from node {primary}")
        # TODO: check the replicas if the primary isn't available
        try:
            resp, socket = call_node_with_command(command, primary)
            socket.close()
            return resp
        except FileNotFoundError:
            print(f"unable to reach node {primary}")
            return StringResponse(f"Node {primary} not available")

    def set(self, command):
        routing_info = self.get_routing_info(command.key)
        if len(routing_info) == 0:
            return StringResponse("no nodes in the ring")

        primary = routing_info[0]

        # TODO: handle a dead primary here
        try:
            resp, socket = call_node_with_command(command, primary)
            socket.close()
        except FileNotFoundError:
            return StringResponse("primary node is not available")

        print(f"writing to node {primary}")
        return resp

    def serve(self):
        self.server.serve_forever()

    def shutdown_node(self, num):
        print(f"shutting down node {num}")
        try:
            _, s = call_node_with_command(Shutdown(), num)
            s.close()
        except FileNotFoundError:
            return

    def shutdown(self):
        print("beginning load balancer shutdown")
        for f_id in self.pm.nodes:
            self.shutdown_node(f_id)

        self.server.socket.close()

        print("completed load balancer shutdown")
