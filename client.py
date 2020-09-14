import socket
import sys

from kvstore.constants import INPUT_PROMPT
from kvstore.encoding import BinaryEncoderDecoder
from kvstore.input import input_to_command, InputValidationError

def main():
    en = BinaryEncoderDecoder()

    while True:
        i = input(INPUT_PROMPT)
        try:
            command = input_to_command(i)
        except InputValidationError as e:
            print(str(e))
            continue

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 65432))
        encoded = en.encode(command)
        print("sending %s" % encoded)
        s.send(encoded)

        response = s.recv(1024)
        print(response.decode(encoding='UTF-8') + "\n")

        s.close()

if __name__ == "__main__":
    main()
