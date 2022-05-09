import time
import argparse
import pybloom_live
from collections import defaultdict

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate average common prefix for text using spark')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')

    args = parser.parse_args()

    start = time.time()
    bf = pybloom_live.BloomFilter(capacity=923_000_000, error_rate=0.0001)
    count_dict = defaultdict(lambda: 1)

    with open(args.file) as file:
        content = file.read().splitlines()
        for item in content:
            if bf.add(item):
                count_dict[item] += 1

    print(count_dict)

    end = time.time()
    print(f'Calculations finished in: {end-start}')
