import atexit
import os
import socket
import sys
import readline

from kvstore.constants import INPUT_PROMPT, ENTRYPOINT_SOCKET
from kvstore.encoding import BinaryEncoderDecoder
from kvstore.input import input_to_command, InputValidationError
from kvstore.handlers import get_command_from_command

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

    CONNECT_TO_NODE = 1

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        while True:
            i = input(INPUT_PROMPT)

            try:
                command = input_to_command(i)
            except InputValidationError as e:
                print(str(e))
                continue

            response, s = get_command_from_command(command, CONNECT_TO_NODE)

            print(response.value + "\n")
            s.close()

    except KeyboardInterrupt:
        pass
    finally:
        s.close()

if __name__ == "__main__":
    main()
