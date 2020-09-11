import socket
import sys

from kvstore.constants import INPUT_PROMPT, SERVER_ADDR

def main():
    while True:
        i = input(INPUT_PROMPT)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 65432))
        s.send(i.encode(encoding='UTF-8',errors='strict'))

        response = s.recv(1024)
        print(response.decode(encoding='UTF-8') + "\n")

        s.close()


    # HOST = "127.0.0.1"
    # PORT = 65432
    #
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #     s.connect((HOST, PORT))
    #     s.sendall(b'Hello, world!')
    #     data = s.recv(1024)
    #
    # print('received', repr(data))

    # # create a UDS socket
    # sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
#
    # # connect to the socket on the port where the server is listening
    # print("connecting to %s" % SERVER_ADDR)
    #
    # try:
    #     sock.connect(SERVER_ADDR)
    # except socket.error as e:
    #     print(e)
    #     sys.exit(1)
    #
    # try:
    #     # send data
    #     message = "This is a message, it will be repeated"
    #     print("sending %s", message)
    #     encoded = message.encode(encoding='UTF-8',errors='strict')
    #     sock.sendall(encoded)
    #
    #     amount_received = 0
    #     amount_expected = len(message)
    #
    #     while amount_received < amount_expected:
    #         data = sock.recv(1024)
    #         amount_received += len(data)
    #         print("received %s" % data)
    # finally:
    #     print("closing socket")
    #     # sock.close()


if __name__ == "__main__":
    main()
