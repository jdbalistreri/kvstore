import os
import pickle
from kvstore.constants import NULL

DEFAULT_FILE_NAME = "data/store.p"

class KVStore:
    def __init__(self, filename=DEFAULT_FILE_NAME):
        self.filename = filename
        read = self.read_from_disk()
        self.store = read["store"]
        self.logSequenceNumber = read["logSequenceNumber"]

    def write_to_disk(self):
        self.logSequenceNumber += 1
        print(f"logSequenceNumber: {self.logSequenceNumber}")
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

    def set(self, key, value):
        self.store[key] = value
        self.write_to_disk()
        return value
