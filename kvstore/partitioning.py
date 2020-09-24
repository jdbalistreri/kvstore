import bisect
import hashlib
import random


class PartitionManager:

    def __init__(self):
        self.nodes = set([1,2,3])
        self.ring_keys = []
        self.ring = []
        self.tokens_per_node = 10

        self.construct_ring()

    def lookup(self, key, replicas=3):
        key_hash = self._hash(key.encode('utf-8'))
        max_replicas = min(len(self.nodes), replicas)

        dest_nodes = []
        i = bisect.bisect(self.ring_keys, key_hash)
        while len(dest_nodes) < max_replicas:
            (_, node) = self.ring[i]
            if node not in dest_nodes:
                dest_nodes.append(node)
                
            i = (i + 1) % len(self.ring)

        return dest_nodes

    def add_node(self, node):
        if node in self.nodes:
            print(f"node {node} already in the ring")
            return

        self.nodes.add(node)
        self._add_keys_for_node(node)
        self.print()

    def remove_node(self, node):
        if node not in self.nodes:
            print(f"node {node} not in the ring")
            return

        self.nodes.remove(node)
        self.ring = [x for x in self.ring if x[1] != node]
        self.ring_keys = [r[0] for r in self.ring]
        self.print()

    def _add_keys_for_node(self, node):
        for _ in range(self.tokens_per_node):
            v = self._random_hash()
            insertion_point = bisect.bisect(self.ring_keys, v)
            self.ring_keys.insert(insertion_point, v)
            self.ring.insert(insertion_point, (v, node))

    def construct_ring(self):
        if len(self.ring) > 0:
            print("already constructed")
            return

        for node in self.nodes:
            self._add_keys_for_node(node)

        self.print()

    def print(self):
        print(self.ring)
        print(self.ring_keys)

    def _random_hash(self):
        return self._hash(random.randint(0,100000000).to_bytes(100, 'big'))

    def _hash(self, bytes):
        return int.from_bytes(hashlib.md5(bytes).digest(), 'big')
