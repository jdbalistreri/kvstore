import os
import socket
import sys

from kvstore.constants import INPUT_PROMPT, LB_NODE
from kvstore.input import configure_readline, input_to_command, InputValidationError
from kvstore.server import call_node_with_command


def main():
    configure_readline()

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        while True:
            i = input(INPUT_PROMPT)

            try:
                command = input_to_command(i)
            except InputValidationError as e:
                print(str(e))
                continue

            response, s = call_node_with_command(command, LB_NODE)

            print(response.value + "\n")
            s.close()

    except KeyboardInterrupt:
        pass
    finally:
        s.close()

if __name__ == "__main__":
    main()
