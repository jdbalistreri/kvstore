import os
import socket
import sys

from kvstore.constants import INPUT_PROMPT
from kvstore.input import configure_readline, input_to_command, InputValidationError
from kvstore.server import call_node_with_command


def main():
    configure_readline()

    try:
        connect_to_node = sys.argv[1]
    except IndexError:
        connect_to_node = 1

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        while True:
            i = input(INPUT_PROMPT)

            try:
                command = input_to_command(i)
            except InputValidationError as e:
                print(str(e))
                continue

            response, s = call_node_with_command(command, connect_to_node)

            print(response.value + "\n")
            s.close()

    except KeyboardInterrupt:
        pass
    finally:
        s.close()

if __name__ == "__main__":
    main()
