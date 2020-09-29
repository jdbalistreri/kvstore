import bisect
import hashlib
import os
import pickle
import random

PARTITION_FILENAME = "data/partition_manager.p"

class PartitionManager:

    def __init__(self, is_lb=False, ring=None):
        self.is_lb = is_lb

        if is_lb:
            self.filename = PARTITION_FILENAME

            vals = self.load()

            self.nodes = vals.get('nodes', set([1,2,3]))
            self.ring_keys = vals.get('ring_keys', [])
            self.ring = vals.get('ring', [])
            self.tokens_per_node = vals.get('tokens_per_node', 100)

            self.construct_ring()
        else:
            self.ring = ring
            self.ring_keys = [r[0] for r in self.ring]
            self.nodes = set([x for (_, x) in self.ring])
            self.tokens_per_node = len(self.ring) / len(self.nodes)

    def lookup(self, key, replicas=3):
        key_hash = self._hash(key.encode('utf-8'))
        max_replicas = min(len(self.nodes), replicas)

        dest_nodes = []
        i = bisect.bisect(self.ring_keys, key_hash) % len(self.ring)
        while len(dest_nodes) < max_replicas:
            (_, node) = self.ring[i]
            if node not in dest_nodes:
                dest_nodes.append(node)

            i = (i + 1) % len(self.ring)

        return dest_nodes

    def save(self):
        if not self.is_lb:
            return

        vals = {
            'ring': self.ring,
            'ring_keys': self.ring_keys,
            'nodes': self.nodes,
            'tokens_per_node': self.tokens_per_node
        }
        pickle.dump(
            vals,
            open( self.filename, "wb+")
        )

    def load(self):
        if not self.is_lb:
            return

        if os.path.exists(self.filename):
            return pickle.load( open( self.filename, "rb+" ) )
        return {}

    def add_node(self, node):
        if node in self.nodes:
            raise ValueError(f"node {node} already in the ring")

        self.nodes.add(node)
        self._add_keys_for_node(node)

        self.save()
        return f"added node {node}"

    def remove_node(self, node):
        if node not in self.nodes:
            raise ValueError(f"node {node} not in the ring")

        self.nodes.remove(node)
        self.ring = [x for x in self.ring if x[1] != node]
        self.ring_keys = [r[0] for r in self.ring]

        self.save()
        return f"removed node {node}"

    def _add_keys_for_node(self, node):
        for _ in range(self.tokens_per_node):
            v = self._random_hash()
            insertion_point = bisect.bisect(self.ring_keys, v)
            self.ring_keys.insert(insertion_point, v)
            self.ring.insert(insertion_point, (v, node))

    def construct_ring(self):
        if len(self.ring) > 0:
            return

        for node in self.nodes:
            self._add_keys_for_node(node)

    def print(self):
        print(self.ring)
        print(self.ring_keys)

    def _random_hash(self):
        return self._hash(random.randint(0,100000000).to_bytes(100, 'big'))

    def _hash(self, bytes):
        return int.from_bytes(hashlib.md5(bytes).digest(), 'big')

if __name__ == "__main__":
    pm = PartitionManager()
    pm.add_node(1)
    pm.add_node(3)
    pm.add_node(2)
