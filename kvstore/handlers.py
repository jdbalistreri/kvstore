import socketserver

from kvstore.input import InputValidationError
from kvstore.store import KVStore
from kvstore.encoding import *

class EntryPointHandler(socketserver.BaseRequestHandler):
    def setup(self):
        self.kvstore = KVStore()
        self.en = BinaryEncoderDecoder()

    def getResult(self, data):
        try:
            command = self.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = self.kvstore.get(command.key)
        elif command.enum == CommandEnum.SET:
            result = self.kvstore.set(command.key, command.value)

        return result

    def handle(self):
        data = self.request.recv(1024)
        result = self.getResult(data)
        self.request.send(result.encode(encoding='UTF-8'))
        return
