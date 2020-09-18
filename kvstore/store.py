import os
import pickle
from kvstore.constants import NULL
from kvstore.encoding import WriteLog, BinaryEncoderDecoder, Set

DEFAULT_FILE_NAME = "data/store.p"
WRITE_LOG = "data/writelog.p"

class KVStore:
    def __init__(self, filename=DEFAULT_FILE_NAME):
        self.en = BinaryEncoderDecoder()

        self.filename = filename

        read = self.read_from_disk()
        self.store = read["store"]
        self.logSequenceNumber = read["logSequenceNumber"]

    def write_to_disk(self, command):
        # update log sequence number
        self.logSequenceNumber += 1
        print(f"logSequenceNumber: {self.logSequenceNumber}")

        # dump entire db
        pickle.dump(
            {"store": self.store, "logSequenceNumber": self.logSequenceNumber},
            open( self.filename, "wb+")
        )

        # write to write log
        encoded_wl = self.en.encode(WriteLog(command.key, command.value, self.logSequenceNumber))
        with open(WRITE_LOG, 'ab') as f:
            f.write(encoded_wl)

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

    def set(self, key, value):
        self.store[key] = value
        self.write_to_disk(Set(key, value))
        return value
