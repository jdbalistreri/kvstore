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
        self.writelog = WRITE_LOG_TMPL % node_number

        read = self.read_from_disk()
        self.store = read["store"]
        self.logSequenceNumber = read["logSequenceNumber"]

    def write_to_disk(self, command):
        # update log sequence number
        self.logSequenceNumber += 1
        print(f"logSequenceNumber: {self.logSequenceNumber}")

        self.dump_db()

        # write to write log
        wl = WriteLog(command.key, command.value, self.logSequenceNumber)
        encoded_wl = self.en.encode(wl)
        with open(self.writelog, 'ab') as f:
            f.write(encoded_wl)
        return wl

    def dump_db(self):
        pickle.dump(
            {"store": self.store, "logSequenceNumber": self.logSequenceNumber},
            open( self.filename, "wb+")
        )

    def read_from_disk(self):
        if os.path.exists(self.filename):
            fetched = pickle.load( open( self.filename, "rb+" ) )
            if fetched.get("logSequenceNumber"):
                return fetched
            return { "store": fetched, "logSequenceNumber": 0 }
        else:
            return {
                "store": {},
                "logSequenceNumber": 0
            }

    def get(self, key):
        return self.store.get(key, NULL)

    def start_from_snapshot(self, snapshot):
        print("loading from snapshot: ", snapshot)
        self.logSequenceNumber = snapshot.logSequenceNumber
        self.store = snapshot.store
        self.dump_db()

    def get_snapshot(self):
        return self.store, self.logSequenceNumber

    def set(self, key, value):
        self.store[key] = value
        wl = self.write_to_disk(Set(key, value))
        return wl

    def receive_write_log(self, command):
        if command.logSequenceNumber < self.logSequenceNumber:
            print("received old write. ignoring...")
        elif command.logSequenceNumber > self.logSequenceNumber + 1:
            # TODO: handle writes that have bene received out of order
            raise NotImplementedError
        else:
            self.set(command.key, command.value)
