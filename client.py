import socket
import sys

from kvstore.constants import INPUT_PROMPT

def main():
    while True:
        i = input(INPUT_PROMPT)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 65432))
        s.send(i.encode(encoding='UTF-8',errors='strict'))

        response = s.recv(1024)
        print(response.decode(encoding='UTF-8') + "\n")

        s.close()

if __name__ == "__main__":
    main()
