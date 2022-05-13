import numpy
import time
import argparse
import sys
from collections import Counter
from tqdm import tqdm
import pprofile

class TrieNode:
    def __init__(self):
        self.keys = [] #as tuples (char, index)
        self.children = []
        self.weights = []

    def makeProbabilities(self):
        #if there are no weights varibel then it is an end node
        if self.weights:
            w_sum = sum(self.weights)
            self.prob = list(map(lambda x: x/w_sum, self.weights))
            #call recursively
            [n.makeProbabilities() for n in self.children]

        return

    def sample(self):
        #if weights exist then we have self.prob is we have ran makeProbabilities
        if self.weights:
            char,index = rng.choice(self.keys, replace=True, p=self.prob)
            return char + self.children[int(index)].sample()
        return ''


class SampleTrie:
    def __init__(self):
      self.root = TrieNode()

    def insert(self, item, weight):
        node = self.root
        for char in item:
            #insert new char
            if char not in list(map(lambda x: x[0], node.keys)):
                node.keys.append((char, len(node.keys)))
                node.children.append(TrieNode())
                node.weights.append(weight)
                node = node.children[-1]

            #increase weight of existing char
            else:
                index = list(map(lambda x: x[0], node.keys)).index(char)
                node.weights[index] += weight
                node = node.children[index]

    def makeProbabilities(self):
        self.root.makeProbabilities()

    def sample(self, n):
        samples = []
        for _ in range(n):
            samples.append(self.root.sample())
        return samples

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare sample speeds of numpy choice and trie for samples from file')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file to run the program on')
    parser.add_argument('-n',
                        type=int,
                        default=10000,
                        help='Number of samples to generate using each method')

    args = parser.parse_args()


    items = []
    prob = []

    trie = SampleTrie()

    #debug profile TODO remove
    profiler = pprofile.Profile()
    with profiler:
        startSetup = time.time()
        #build the datastructures
        with open(args.file, "r") as file:
            # Reading form a file
            content = file.readlines()
            for line in tqdm(content):
                data = line.split()
                item = data[0]
                weight = int(data[1])
                items.append(item)
                prob.append(weight)

                trie.insert(item, weight)

        all_sum = sum(prob)
        prob = list(map(lambda x: x/all_sum, prob))

        trie.makeProbabilities()
        endSetup = time.time()
        print(f'startup took {endSetup-startSetup}s')

        #the reference code
        rng = numpy.random.default_rng()
        start = time.time()
        samples = rng.choice(items, args.n, replace=True, p=prob)
        #print(samples)
        end = time.time()
        
        print(f' {args.n} samples using numpy choice in {end-start}s, {args.n/(end - start)} samples per second')

        #TODO remove
        print(trie.root.keys)

        #the trie code
        startTrie = time.time()
        samples_trie = trie.sample(args.n)
        #print(samples_trie )
        endTrie = time.time()
        print(f' {args.n} samples using trie in {endTrie-startTrie}s, {args.n/(endTrie - startTrie)} samples per second')

    #end profiling TODO remove
    profiler.dump_stats("./tmp/profiler_stats.txt")
