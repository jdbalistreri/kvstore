import socketserver

from kvstore.constants import GET_OP, SET_OP
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
        data = self.request.recv(1024)
        result = self.getResult(data.decode(encoding='UTF-8'))
        self.request.send(result.encode(encoding='UTF-8'))
        return

if __name__ == '__main__':
    address = ('localhost', 65432)
    server = socketserver.TCPServer(address, EchoRequestHandler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
