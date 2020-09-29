from enum import Enum
import io

class CommandEnum(Enum):
    GET = 1
    SET = 2
    WRITE_LOG = 3
    REGISTER_FOLLOWER = 4
    SNAPSHOT = 5
    STRING_RESPONSE = 6
    WRITE_LOGS = 7
    SHUTDOWN = 8
    EMPTY_RESPONSE = 9
    LB_REGISTRATION_INFO = 10
    ADD_NODE = 11
    REMOVE_NODE = 12
    LIST_NODES = 13
    PING = 14

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
        if len(binary_input) == 0:
            return EmptyResponse()

        buf = io.BytesIO(binary_input)
        command_no, _ = self.decode_num(buf)

        if command_no == CommandEnum.GET.value:
            key, _ = self.decode_string(buf)
            return Get(key)

        elif command_no == CommandEnum.SET.value:
            key, _ = self.decode_string(buf)
            value, _ = self.decode_string(buf)
            return Set(key, value)

        elif command_no == CommandEnum.WRITE_LOG.value:
            wl, _ = self.decode_wl(buf)
            return wl

        elif command_no == CommandEnum.REGISTER_FOLLOWER.value:
            node_number, _ = self.decode_num(buf)
            log_sequence_number, _ = self.decode_num(buf)
            return RegisterFollower(node_number, log_sequence_number)

        elif command_no == CommandEnum.SNAPSHOT.value:
            log_sequence_number, _ = self.decode_num(buf)
            num_keys, _ = self.decode_num(buf)

            store = {}
            for _ in range(num_keys):
                k, _ = self.decode_string(buf)
                v, _ = self.decode_string(buf)
                store[k] = v

            return Snapshot(store, log_sequence_number)

        elif command_no == CommandEnum.STRING_RESPONSE.value:
            value, _ = self.decode_string(buf)
            return StringResponse(value)

        elif command_no == CommandEnum.WRITE_LOGS.value:
            total_logs, _ = self.decode_num(buf)
            result = []
            for _ in range(total_logs):
                wl, _ = self.decode_wl(buf)
                result.append(wl)
            return WriteLogs(result)
        elif command_no == CommandEnum.SHUTDOWN.value:
            return Shutdown()
        elif command_no == CommandEnum.LIST_NODES.value:
            return ListNodes()
        elif command_no == CommandEnum.PING.value:
            return Ping()
        elif command_no == CommandEnum.LB_REGISTRATION_INFO.value:
            leader_id, _ = self.decode_num(buf)
            num_followers, _ = self.decode_num(buf)
            followers = set()
            for _ in range(num_followers):
                follower, _ = self.decode_num(buf)
                followers.add(follower)
            return LBRegistrationInfo(leader_id, followers)
        elif command_no == CommandEnum.ADD_NODE.value:
            node_num, _ = self.decode_num(buf)
            return AddNode(node_num)
        elif command_no == CommandEnum.REMOVE_NODE.value:
            node_num, _ = self.decode_num(buf)
            return RemoveNode(node_num)

        raise ValueError("Invalid command number")

    def decode_wl(self, buf):
        log_sequence_number, read1 = self.decode_num(buf)
        key, read2 = self.decode_string(buf)
        value, read3 = self.decode_string(buf)
        return WriteLog(key, value, log_sequence_number), read1 + read2 + read3

    def encode_wl(self, command):
        log_seq_no = self.encode_num(command.log_sequence_number)
        key_encoded = self.encode_string(command.key)
        val_encoded = self.encode_string(command.value)
        return log_seq_no + key_encoded + val_encoded

    def encode(self, command):
        if command.enum == CommandEnum.EMPTY_RESPONSE:
            return b''

        buf = io.BytesIO()
        buf.write(self.encode_num(command.enum.value))

        if command.enum == CommandEnum.GET:
            buf.write(self.encode_string(command.key))
        elif command.enum == CommandEnum.SET:
            buf.write(self.encode_string(command.key))
            buf.write(self.encode_string(command.value))
        elif command.enum == CommandEnum.WRITE_LOG:
            buf.write(self.encode_wl(command))
        elif command.enum == CommandEnum.REGISTER_FOLLOWER:
            buf.write(self.encode_num(command.node_number))
            buf.write(self.encode_num(command.log_sequence_number))
        elif command.enum == CommandEnum.SNAPSHOT:
            buf.write(self.encode_num(command.log_sequence_number))

            num_keys = self.encode_num(len(command.store.keys()))
            buf.write(num_keys)

            for k, v in command.store.items():
                buf.write(self.encode_string(k))
                buf.write(self.encode_string(v))
        elif command.enum == CommandEnum.STRING_RESPONSE:
            buf.write(self.encode_string(command.value))
        elif command.enum == CommandEnum.WRITE_LOGS:
            buf.write(self.encode_num(len(command.write_logs)))
            for wl in command.write_logs:
                log_seq_no = self.encode_num(wl.log_sequence_number)
                key_encoded = self.encode_string(wl.key)
                val_encoded = self.encode_string(wl.value)
                buf.write(log_seq_no + key_encoded + val_encoded)
        elif command.enum == CommandEnum.SHUTDOWN:
            pass
        elif command.enum == CommandEnum.LIST_NODES:
            pass
        elif command.enum == CommandEnum.PING:
            pass
        elif command.enum == CommandEnum.LB_REGISTRATION_INFO:
            buf.write(self.encode_num(command.leader_id))
            buf.write(self.encode_num(len(command.followers)))
            for f_id in command.followers:
                buf.write(self.encode_num(f_id))
        elif command.enum == CommandEnum.ADD_NODE or command.enum ==CommandEnum.REMOVE_NODE:
            buf.write(self.encode_num(command.node_num))
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
        size, read = self.decode_num(buf)
        return buf.read(size).decode(self.string_encoding), read + size

    def decode_num(self, buf):
        return int.from_bytes(
            buf.read(self.size_bytes),
            byteorder=self.byte_order,
            signed=self.signed
        ), self.size_bytes

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

class WriteLogs(Command):
    def __init__(self, write_logs):
        self.write_logs = write_logs
        self.enum = CommandEnum.WRITE_LOGS

    def __repr__(self):
        return f'WRITE_LOGS([{len(self.write_logs)}]'

    def __eq__(self, other):
        if not isinstance(other, WriteLogs):
            raise NotImplemented
        return all(x == y for x, y in zip(self.write_logs, other.write_logs))

class WriteLog(Command):
    def __init__(self, key, value, log_sequence_number):
        self.key = key
        self.value = value
        self.log_sequence_number = log_sequence_number
        self.enum = CommandEnum.WRITE_LOG

    def __repr__(self):
        return f'WRITE_LOG([{self.log_sequence_number}]{self.key}: {self.value})'

    def __eq__(self, other):
        if not isinstance(other, WriteLog):
            raise NotImplemented
        return self.key == other.key and self.value == other.value and self.log_sequence_number == other.log_sequence_number

class RegisterFollower(Command):
    def __init__(self, node_number, log_sequence_number):
        self.enum = CommandEnum.REGISTER_FOLLOWER
        self.node_number = node_number
        self.log_sequence_number = log_sequence_number

    def __repr__(self):
        return f'REGISTER_FOLLOWER({self.node_number})'

    def __eq__(self, other):
        return self.node_number == other.node_number

class Snapshot(Command):
    def __init__(self, store, log_sequence_number):
        self.enum = CommandEnum.SNAPSHOT
        self.store = store
        self.log_sequence_number = log_sequence_number

    def __repr__(self):
        return f'SNAPSHOT({self.store} {self.log_sequence_number})'

    def __eq__(self, other):
        if not isinstance(other, Snapshot):
            raise NotImplemented
        return self.store == other.store and self.log_sequence_number == other.log_sequence_number

class StringResponse(Command):
    def __init__(self, value):
        self.enum = CommandEnum.STRING_RESPONSE
        self.value = value

    def __repr__(self):
        return f'Str({self.value})'

    def __eq__(self, other):
        if not isinstance(other, StringResponse):
            raise NotImplemented
        return self.value == other.value

class Shutdown(Command):
    def __init__(self):
        self.enum = CommandEnum.SHUTDOWN

    def __repr__(self):
        return f'Shutdown'

    def __eq__(self, other):
        if not isinstance(other, Shutdown):
            raise NotImplemented
        return True

class EmptyResponse(Command):
    def __init__(self):
        self.enum = CommandEnum.EMPTY_RESPONSE

    def __repr__(self):
        return f'EmptyResponse'

    def __eq__(self, other):
        if not isinstance(other, EmptyResponse):
            raise NotImplemented
        return True

class LBRegistrationInfo(Command):
    def __init__(self, leader_id, followers):
        self.enum = CommandEnum.LB_REGISTRATION_INFO
        self.leader_id = leader_id
        self.followers = followers

    def __repr__(self):
        return f'EmptyResponse'

    def __eq__(self, other):
        if not isinstance(other, LBRegistrationInfo):
            raise NotImplemented
        return self.leader_id == other.leader_id and self.followers == other.followers

class AddNode(Command):
    def __init__(self, node_num):
        self.enum = CommandEnum.ADD_NODE
        self.node_num = node_num

    def __repr__(self):
        return f'AddNode({self.node_num})'

    def __eq__(self, other):
        if not isinstance(other, AddNode):
            raise NotImplemented
        return self.node_num == other.node_num

class RemoveNode(Command):
    def __init__(self, node_num):
        self.enum = CommandEnum.REMOVE_NODE
        self.node_num = node_num

    def __repr__(self):
        return f'RemoveNode({self.node_num})'

    def __eq__(self, other):
        if not isinstance(other, RemoveNode):
            raise NotImplemented
        return self.node_num == other.node_num

class ListNodes(Command):
    def __init__(self):
        self.enum = CommandEnum.LIST_NODES

    def __repr__(self):
        return f'ListNodes'

    def __eq__(self, other):
        if not isinstance(other, ListNodes):
            raise NotImplemented
        return True

class Ping(Command):
    def __init__(self):
        self.enum = CommandEnum.PING

    def __repr__(self):
        return f'Ping'

    def __eq__(self, other):
        if not isinstance(other, Ping):
            raise NotImplemented
        return True
