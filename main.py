from kvstore.constants import GET_OP, SET_OP, NULL
from kvstore.input import get_input, InputValidationError


class KVStore:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key, NULL)

    def set(self, key, value):
        self.store[key] = value
        return value


def main():
    kvstore = KVStore()

    while True:
        try:
            op, key, value = get_input()

        except InputValidationError as e:
            print(str(e))

        if op == GET_OP:
            result = kvstore.get(key)
        elif op == SET_OP:
            result = kvstore.set(key, value)

        print(result + "\n")


if __name__ == "__main__":
    main()
