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
            return StringResponse("No read nodes available")

        get_node = random.sample(self.followers, 1)[0]
        print(f"Reading from node {get_node}")
        try:
            resp, socket = call_node_with_command(command, get_node)
            socket.close()
            return resp
        except FileNotFoundError:
            print(f"unable to reach node {get_node}. marking as dead")
            self.followers.remove(get_node)
            return self.get(command)

    def set(self, command):
        if self.leader == None:
            return StringResponse("No write nodes available")

        try:
            resp, socket = call_node_with_command(command, self.leader)
            socket.close()
            return resp
        except FileNotFoundError:
            print(f"unable to reach node {self.leader}. marking as dead")
            self.leader = None
            return StringResponse("No write nodes available")

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        print("Beginning load balancer shutdown")
        for f_id in self.followers:
            print(f"Shutting down node {f_id}")
            _, s = call_node_with_command(Shutdown(), f_id)
            s.close()
        self.server.socket.close()

        print("Completed load balancer shutdown")
