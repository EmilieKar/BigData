import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import random
import math
import time
import numpy as np
import argparse
import re
from heapq import merge

def mapper (line):
    l = re.sub(r"[^a-z ]+", "", line.lower()).split()
    return sorted(set(l))

def combiner_reducer (a, b):
    return list(dict.fromkeys(merge(a, b)))

def chars_common(a, b):
    res = 0
    for (a_i, b_i) in zip(a,b):
        if a_i == b_i:
            res +=  1
        else: break

    return res

def ave_common_prefix_len(lst, debug=False):
    lst_len = len(lst)
    if lst_len < 2:
        return 0

    last_word = lst.pop(0)
    common_sum = 0

    for word in lst:
        if debug:
            print(last_word, word, chars_common(last_word, word))
        common_sum += chars_common(last_word, word)
        last_word = word

    if debug:
        print('\nlist length: ', lst_len, '\ncommon prefix sum:', common_sum)

    return common_sum/lst_len


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate average common prefix for text using spark')
    parser.add_argument('-w',
                        type=int,
                        default=4,
                        help='Set the number of workers')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')

    args = parser.parse_args()

    conf = SparkConf()
    conf.setMaster('local[' + str(args.w) + ']')
    conf.setAppName("ave-common-prefix")
    conf.set("spark.local.dir", "./runinfo")

    sc = SparkContext.getOrCreate(conf)
    
    print(sc.getConf().getAll())

    dataFile = sc.textFile(args.file, minPartitions=args.w)
    print('Timing started')
    start = time.time()

    data = dataFile.map(mapper).filter(lambda x: x != []).reduce(combiner_reducer)

    #print(data)
    print(ave_common_prefix_len(data))

    end = time.time()
    print(f'Calculations finished in: {end-start}')
