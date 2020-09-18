import socketserver

from kvstore.input import InputValidationError
from kvstore.encoding import *

class EntryPointHandler(socketserver.BaseRequestHandler):
    def getResult(self, data):
        try:
            command = self.server.en.decode(data)
        except InputValidationError as e:
            return str(e)

        if command.enum == CommandEnum.GET:
            result = StringResponse(self.server.kvstore.get(command.key))
        elif command.enum == CommandEnum.SET:
            result = StringResponse(self.server.kvstore.set(command.key, command.value))
        elif command.enum == CommandEnum.GET_SNAPSHOT:
            store, logSequenceNumber = self.server.kvstore.get_snapshot()
            print("got snapshot!")
            result = Snapshot(store, logSequenceNumber)

        return result

    def handle(self):
        data = self.request.recv(1024)
        result = self.getResult(data)
        encoded = self.server.en.encode(result)
        self.request.send(encoded)
        return
