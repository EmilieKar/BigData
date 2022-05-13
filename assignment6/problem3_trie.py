import numpy
import time
import argparse

class TrieNode:
    def __init__(self, endNode=False):
        self.children = []
        self.weights = []
        self.endNode = endNode

class SampleTrie:
    def __init__(self):
      self.root = TrieNode()

    def insert(self, item, wheight):
        node = self.root
        for char in item:


    def sample(self):
        pass


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

    #build the datastructures
    with open(args.file, "r") as file:
        # Reading form a file
        content = file.readlines()
        for line in content:
            data = line.split()
            items.append(data[0])
            prob.append(int(data[1]))

    all_sum = sum(prob)
    prob = list(map(lambda x: x/all_sum, prob))
    print(f'length items {len(items)}')
    print(f'length prob {len(prob)}')
    
    #the reference code
    rng = numpy.random.default_rng()
    start = time.time()
    samples = rng.choice(items, args.n, replace=True, p=prob)
    end = time.time()

    print(f' {args.n} samples in {end-start}s, {args.n/(end - start)} samples per second')
    
    #cleanup
    del(items)
    del(prob)
    del(samples)


    #the trie code
