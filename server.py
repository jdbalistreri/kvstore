import socket
import sys
import socketserver

from kvstore.constants import GET_OP, SET_OP, SERVER_ADDR
from kvstore.input import parse_input, InputValidationError
from kvstore.store import KVStore


class EchoRequestHandler(socketserver.BaseRequestHandler):
    def setup(self):
        self.kvstore = KVStore()

    def getResult(self, i):
        try:
            op, key, value = parse_input(i)
        except InputValidationError as e:
            return str(e)

        if op == GET_OP:
            result = self.kvstore.get(key)
        elif op == SET_OP:
            result = self.kvstore.set(key, value)

        return result


    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024)
        result = self.getResult(data.decode(encoding='UTF-8'))
        self.request.send(result.encode(encoding='UTF-8'))
        return

if __name__ == '__main__':

    address = ('localhost', 65432) # let the kernel give us a port
    server = socketserver.TCPServer(address, EchoRequestHandler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()

    # try:
    #     os.unlink(SERVER_ADDR)
    # except OSError:
    #     if os.path.exists(SERVER_ADDR):
    #         raise
    #
    # # create a UDS socket
    # sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    #
    # # bind the socket to the port
    # print("starting up on %s" % SERVER_ADDR)
    # sock.bind(SERVER_ADDR)
    #
    # # listen for incoming connections
    # sock.listen(1)
    #
    #
    # while True:
    #     # wait for a connection
    #     print("waiting for a connection")
    #     connection, client_addr = sock.accept()
    #
    #     print("connection from %s" % client_addr)
    #
    #     while True:
    #         try:
    #             data = connection.recv(1024)
    #             print("received %s" % data)
    #             if data:
    #                 print("sending back %s" % data)
    #                 connection.sendall(data)
    #             else:
    #                 print("no more data from %s", client_addr)
    #                 break
    #
    #         finally:
    #             # clean up the connection
    #             connection.close()


# if __name__ == "__main__":
#     main()
