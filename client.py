import atexit
import os
import socket
import sys
import readline

from kvstore.constants import INPUT_PROMPT
from kvstore.encoding import BinaryEncoderDecoder
from kvstore.input import input_to_command, InputValidationError

def configure_readline():
    histfile = ".python_history"
    try:
        readline.read_history_file(histfile)
        readline.set_history_length(1000)
    except FileNotFoundError:
        pass

    atexit.register(readline.write_history_file, histfile)


def main():
    configure_readline()
    en = BinaryEncoderDecoder()

    try:
        while True:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect('test_file.sock')

            i = input(INPUT_PROMPT)

            try:
                command = input_to_command(i)
            except InputValidationError as e:
                print(str(e))
                continue

            encoded = en.encode(command)
            print("sending %s" % encoded)
            s.send(encoded)

            total = b''
            while True:
                response = s.recv(1024)
                if len(response) == 0:
                    break
                total += response

            print(total.decode(encoding='UTF-8') + "\n")
            s.close()
            
    except KeyboardInterrupt:
        pass
    finally:
        s.close()

if __name__ == "__main__":
    main()
