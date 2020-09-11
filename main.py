from kvstore.constants import GET_OP, SET_OP
from kvstore.input import get_input, InputValidationError
from kvstore.store import KVStore

def main():
    kvstore = KVStore()

    while True:
        try:
            op, key, value = get_input()

        except InputValidationError as e:
            print(str(e))
            continue

        if op == GET_OP:
            result = kvstore.get(key)
        elif op == SET_OP:
            result = kvstore.set(key, value)

        print(result + "\n")


if __name__ == "__main__":
    main()
