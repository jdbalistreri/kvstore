import socketserver
import os

from kvstore.constants import ENTRYPOINT_SOCKET
from kvstore.handlers import EntryPointHandler

if __name__ == '__main__':
    server = socketserver.UnixStreamServer(ENTRYPOINT_SOCKET, EntryPointHandler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
        os.unlink(ENTRYPOINT_SOCKET)
