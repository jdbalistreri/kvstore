import socketserver

from kvstore.input import InputValidationError
from kvstore.encoding import *

class EntryPointHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        result = self.server.server.handle(data)
        encoded = self.server.server.en.encode(result)
        self.request.send(encoded)
        return
