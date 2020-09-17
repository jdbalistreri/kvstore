from enum import Enum
import io

class CommandEnum(Enum):
    GET = 1
    SET = 2
    MSET = 3
    CREATE_TABLE = 4
    LIST_TABLES = 5
    REMOVE_TABLE = 6


class Command():
    def encode(self):
        raise NotImplementedError


class BinaryEncoderDecoder:
    def __init__(self):
        self.byte_order = 'big'
        self.string_encoding = 'utf-8'

        self.command_bytes = 2
        self.command_signed = False

        # TODO: differentiate between str8, str16, and str32
        self.key_size_bytes = 2
        self.value_size_bytes = 2
        self.string_size_signed = False

    def decode(self, binary_input):
        buf = io.BytesIO(binary_input)
        command_no = self.decode_command(buf)

        if command_no == CommandEnum.GET.value:
            key = self.decode_string(buf, self.key_size_bytes)
            return Get(key)

        elif command_no == CommandEnum.SET.value:
            key = self.decode_string(buf, self.key_size_bytes)
            value = self.decode_string(buf, self.value_size_bytes)
            return Set(key, value)

        raise ValueError("Invalid command number")

    def encode(self, command):
        buf = io.BytesIO()
        buf.write(self.encode_command(command.enum.value))

        if command.enum == CommandEnum.GET:
            buf.write(self.encode_string(command.key, self.key_size_bytes))
        elif command.enum == CommandEnum.SET:
            buf.write(self.encode_string(command.key, self.key_size_bytes))
            buf.write(self.encode_string(command.value, self.value_size_bytes))
        else:
            raise ValueError("Invalid command")

        return buf.getvalue()

    def decode_string(self, buf, size_bytes):
        size = int.from_bytes(
            buf.read(size_bytes),
            byteorder=self.byte_order,
            signed=self.string_size_signed
        )
        return buf.read(size).decode(self.string_encoding)

    def encode_string(self, str, size_bytes):
        encoded = str.encode(self.string_encoding)
        size = len(encoded).to_bytes(
            size_bytes,
            byteorder=self.byte_order,
            signed=self.string_size_signed
        )
        return size + encoded

    def encode_command(self, i):
        return i.to_bytes(
            self.command_bytes,
            byteorder=self.byte_order,
            signed=self.command_signed
        )

    def decode_command(self, buf):
        return int.from_bytes(
            buf.read(self.command_bytes),
            byteorder=self.byte_order,
            signed=self.command_signed
        )


class Get(Command):
    def __init__(self, key):
        self.key = key
        self.enum = CommandEnum.GET

    def __repr__(self):
        return f'GET({self.key})'

    def __eq__(self, other):
        if not isinstance(other, Get):
            return NotImplemented
        return self.key == other.key


class Set(Command):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.enum = CommandEnum.SET

    def __repr__(self):
        return f'SET({self.key}: {self.value})'

    def __eq__(self, other):
        if not isinstance(other, Set):
            return NotImplemented
        return self.key == other.key and self.value == other.value
