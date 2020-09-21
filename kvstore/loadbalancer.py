import random
import socketserver

from kvstore.constants import ENTRYPOINT_SOCKET, LEADER_NODE
from kvstore.encoding import *
from kvstore.transport import EntryPointHandler, call_node_with_command, get_socket_fd
import socket


class LoadBalancer:
    def __init__(self, node_number):
        self.node_number = node_number
        self.en = BinaryEncoderDecoder()
        sockFd = get_socket_fd(node_number)
        self.server = socketserver.UnixStreamServer(sockFd, EntryPointHandler)
        self.server.server = self

        self.leader = None
        self.followers = set()

        print("Starting loadbalancer")

    def registerFollower(self, command):
        if command.node_number == LEADER_NODE:
            print(f"Registered node {command.node_number} as the leader")
            self.leader = command.node_number
            self.followers.add(command.node_number)
        else:
            print(f"Registered node {command.node_number} as a follower")
            self.followers.add(command.node_number)

        return StringResponse("ack")

    def get(self, command):
        if len(self.followers) == 0:
            return StringResponse("No nodes available")

        # TODO: handle dead nodes
        get_node = random.sample(self.followers, 1)[0]
        print(f"Reading from node {get_node}")
        resp, socket = call_node_with_command(command, get_node)
        socket.close()
        return resp

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.socket.close()
