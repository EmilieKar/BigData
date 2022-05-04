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
    parser.add_argument('-w',
                        type=int,
                        default=4,
                        help='Set the number of workers')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')

    args = parser.parse_args()

    with open(args.file) as file:
        content = file.read()

    result = sorted(list(dict.fromkeys(re.sub(r"[^a-z\s]+", "", content.lower()).split())))
    print('Timing started')
    start = time.time()
    
    #print(result)
    print('average shared prefix:', ave_common_prefix_len(result, debug=False))

    end = time.time()
    print(f'Calculations finished in: {end-start}')
