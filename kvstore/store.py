import io
import os
import pickle
from kvstore.constants import NULL
from kvstore.encoding import WriteLog, BinaryEncoderDecoder, Set, Snapshot

STORE_FILE_TMPL = "data/store%s.p"
WRITE_LOG_TMPL = "data/writelog%s.p"

class KVStore:
    def __init__(self, node_number, filename=STORE_FILE_TMPL):
        self.en = BinaryEncoderDecoder()

        self.node_number = node_number
        self.filename = filename % node_number
        self.writelogfilename = WRITE_LOG_TMPL % node_number

        read = self.read_from_disk()
        self.store = read["store"]
        self.log_sequence_number = read["log_sequence_number"]
        self.writelog = self.load_write_log()

    def load_write_log(self):
        try:
            with open(self.writelogfilename, 'rb') as f:
                content = f.read()
                total_to_read = len(content)
                buf = io.BytesIO(content)

            read = 0
            result = []
            while read < total_to_read:
                wl, bytes_read = self.en.decode_wl(buf)
                result.append(wl)
                read += bytes_read

            return result
        except FileNotFoundError:
            return []

    def write_to_disk(self, command):
        # update log sequence number
        self.log_sequence_number += 1
        print(f"log_sequence_number: {self.log_sequence_number}")

        self.dump_db()

        wl = WriteLog(command.key, command.value, self.log_sequence_number)
        # trim the in memory write log
        self.writelog.append(wl)
        if len(self.writelog) > 20:
            self.writelog = self.writelog[14:]

        # write to the on-disk write log
        encoded_wl = self.en.encode_wl(wl)
        with open(self.writelogfilename, 'ab') as f:
            f.write(encoded_wl)
        return wl

    def dump_db(self):
        pickle.dump(
            {"store": self.store, "log_sequence_number": self.log_sequence_number},
            open( self.filename, "wb+")
        )

    def read_from_disk(self):
        if os.path.exists(self.filename):
            fetched = pickle.load( open( self.filename, "rb+" ) )
            if fetched.get("log_sequence_number"):
                return fetched
            return { "store": fetched, "log_sequence_number": 0 }
        else:
            return {
                "store": {},
                "log_sequence_number": 0
            }

    def get(self, key):
        return self.store.get(key, NULL)

    def start_from_write_logs(self, write_logscommand):
        write_logs = write_logscommand.write_logs
        def byLogSeqNum(c):
            return c.log_sequence_number
        write_logs.sort(key=byLogSeqNum)

        for wl in write_logs:
            self.receive_write_log(wl)

    def start_from_snapshot(self, snapshot):
        print("loading from snapshot: ", snapshot)
        self.log_sequence_number = snapshot.log_sequence_number
        self.store = snapshot.store
        self.dump_db()

    def get_write_logs_since(self, log_sequence_number):
        diff = self.log_sequence_number - log_sequence_number
        return self.writelog[-1*diff:]

    def get_snapshot(self):
        return self.store, self.log_sequence_number

    def set(self, key, value):
        self.store[key] = value
        wl = self.write_to_disk(Set(key, value))
        return wl

    def receive_write_log(self, command):
        if command.log_sequence_number < self.log_sequence_number:
            print("received old write. ignoring...")
        elif command.log_sequence_number > self.log_sequence_number + 1:
            raise Exception("Missing write logs - replica will be inconsistent")
        else:
            self.set(command.key, command.value)
