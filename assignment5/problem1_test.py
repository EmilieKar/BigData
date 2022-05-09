import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import random
import math
import time
import numpy as np
import argparse
import re
from problem1 import ave_common_prefix_len

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate average common prefix for text using spark')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')

    args = parser.parse_args()

    with open(args.file) as file:
        content = file.read()

    print('Timing started')
    start = time.time()
    
    word_list = re.sub(r"[^a-z\s]+", "", content.lower()).split()
    word_len = [len(w) for w in word_list]
    words_sorted = sorted(list(dict.fromkeys(word_list)))
    num_unique = len(words_sorted)

    #print(words_sorted)
    print('average common prefix:', ave_common_prefix_len(words_sorted, debug=False))
    print('number of unique words:', num_unique)
    print('average word length:', sum(word_len)/len(word_len))

    end = time.time()
    print(f'Calculations finished in: {end-start}')
