from enum import Enum
import io

class CommandEnum(Enum):
    GET = 1
    SET = 2
    WRITE_LOG = 3

class Command():
    def encode(self):
        raise NotImplementedError


class BinaryEncoderDecoder:
    def __init__(self):
        self.byte_order = 'big'
        self.string_encoding = 'utf-8'
        self.size_bytes = 4
        self.signed = False
        self.signed = False

    def decode(self, binary_input):
        buf = io.BytesIO(binary_input)
        command_no = self.decode_num(buf)

        if command_no == CommandEnum.GET.value:
            key = self.decode_string(buf)
            return Get(key)

        elif command_no == CommandEnum.SET.value:
            key = self.decode_string(buf)
            value = self.decode_string(buf)
            return Set(key, value)

        elif command_no == CommandEnum.WRITE_LOG.value:
            total_len = self.decode_num(buf)
            logSequenceNumber = self.decode_num(buf)
            key = self.decode_string(buf)
            value = self.decode_string(buf)
            return WriteLog(key, value, logSequenceNumber)

        raise ValueError("Invalid command number")

    def encode(self, command):
        buf = io.BytesIO()
        buf.write(self.encode_num(command.enum.value))

        if command.enum == CommandEnum.GET:
            buf.write(self.encode_string(command.key))
        elif command.enum == CommandEnum.SET:
            buf.write(self.encode_string(command.key))
            buf.write(self.encode_string(command.value))
        elif command.enum == CommandEnum.WRITE_LOG:
            log_seq_no = self.encode_num(command.logSequenceNumber)
            key_encoded = self.encode_string(command.key)
            val_encoded = self.encode_string(command.value)

            partial_total = log_seq_no + key_encoded + val_encoded
            total_len = self.size_bytes + len(partial_total)

            buf.write(self.encode_num(total_len) + partial_total)
        else:
            raise ValueError("Invalid command")

        return buf.getvalue()

    def encode_num(self, num):
        return num.to_bytes(
            self.size_bytes,
            byteorder=self.byte_order,
            signed=self.signed
        )

    def decode_string(self, buf):
        size = self.decode_num(buf)
        return buf.read(size).decode(self.string_encoding)

    def decode_num(self, buf):
        return int.from_bytes(
            buf.read(self.size_bytes),
            byteorder=self.byte_order,
            signed=self.signed
        )

    def encode_string(self, str):
        encoded = str.encode(self.string_encoding)
        size = len(encoded).to_bytes(
            self.size_bytes,
            byteorder=self.byte_order,
            signed=self.signed
        )
        return size + encoded


class Get(Command):
    def __init__(self, key):
        self.key = key
        self.enum = CommandEnum.GET

    def __repr__(self):
        return f'GET({self.key})'

    def __eq__(self, other):
        if not isinstance(other, Get):
            raise NotImplemented
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
            raise NotImplemented
        return self.key == other.key and self.value == other.value

class WriteLog(Command):
    def __init__(self, key, value, logSequenceNumber):
        self.key = key
        self.value = value
        self.logSequenceNumber = logSequenceNumber
        self.enum = CommandEnum.WRITE_LOG

    def __repr__(self):
        return f'WRITE_LOG([{self.logSequenceNumber}]{self.key}: {self.value})'

    def __eq__(self, other):
        if not isinstance(other, WriteLog):
            raise NotImplemented
        return self.key == other.key and self.value == other.value and self.logSequenceNumber == other.logSequenceNumber
