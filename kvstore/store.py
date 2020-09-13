import os
import pickle
from kvstore.constants import NULL

DEFAULT_FILE_NAME = "data/store.p"

class KVStore:
    def __init__(self, filename=DEFAULT_FILE_NAME):
        self.filename = filename
        self.store = self.read_from_disk()

    def write_to_disk(self):
        pickle.dump(self.store, open( self.filename, "wb+"))

    def read_from_disk(self):
        if os.path.exists(self.filename):
            return pickle.load( open( self.filename, "rb+" ) )
        else:
            return {}

    def get(self, key):
        return self.store.get(key, NULL)

    def set(self, key, value):
        self.store[key] = value
        self.write_to_disk()
        return value
