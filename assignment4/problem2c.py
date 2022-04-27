import findspark
findspark.init()
from pyspark import SparkContext
import random
import math
import time
import numpy as np
import argparse


def binner(value, limits):
    for (n, (l_lim, u_lim)) in enumerate(zip(limits[:-1], limits[1:])):
        if l_lim <= value and value <= u_lim:
            return (n, 1)

    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate the statistics using spark')
    parser.add_argument('-w',
                        type=int,
                        default=4,
                        help='Set the number of workers')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')
    parser.add_argument('--built_in',
                        action='store_true',
                        help='Enable the built in functions for these stats also, used to compare speed and correctness')

    args = parser.parse_args()

    sc = SparkContext(master = 'local[' + str(args.w) + ']')

    print(sc.getConf().getAll())

    print('Timing started')
    start = time.time()
    distFile = sc.textFile(args.file)

    #Output: mean and standard deviation of the values, their minimal and maximal value, as well as the counts necessary for producing a histogram

    data = distFile.map(lambda l: float(l.split()[2]))

    data_terms = data.map(lambda a: 1).reduce(lambda a, b: a + b)

    data_sum = data.reduce(lambda a, b: a + b)

    data_sq = data.map(lambda x: x**2)
    data_sq_sum = data_sq.reduce(lambda a, b: a + b)

    data_min = data.reduce(lambda a, b: min(a,b))

    data_max = data.reduce(lambda a, b: max(a,b))

    data_mean = data_sum/data_terms

    std_dev = math.sqrt(data_sq_sum/data_terms - data_mean**2)

    bins = 10
    limits = np.linspace(data_min, data_max, bins+1)
    data_hist_indexes = data.map(lambda x: binner(x, limits)).filter(lambda x: x is not None)
    data_hist_part = data_hist_indexes.reduceByKey(lambda a, b: a + b).collect()
    data_hist = [0]*bins
    for (i, c) in data_hist_part:
        data_hist[i] = c


    print('The results')
    #print(f'sum {data_sum}')
    #print(f'sum of squares {data_sq_sum}')
    print(f'terms {data_terms}')
    print(f'min {data_min}')
    print(f'max {data_max}')
    print(f'mean {data_mean}')
    print(f'standard deviation {std_dev}')

    print(f'histogram {data_hist}')

    median_num = math.ceil(data_terms/2) #sest a fixed median number (index + 1), we do not use average in case of even number of datapoints
    bin_end = 0
    median_approx = None
    for (i, h) in enumerate(data_hist):
        bin_end += h
        if bin_end >= median_num:
            bin_start = bin_end - h + 1
            p = (median_num - bin_start + 1)/(bin_end - bin_start + 2)
            median_approx = limits[i] + p * (limits[i + 1] - limits[i])
            print(f'index found {i}') #TODO remove
            print(f'h  {h}') #TODO remove
            print(f'bin start {bin_start}') #TODO remove
            print(f'bin end {bin_end}') #TODO remove
            print(f'p {p}') #TODO remove
            print(f'numpy median {np.linspace(limits[i], limits[i+1], h + 2)[median_num - bin_start +1]}') #TODO remove
            break
    
    if median_approx is not None:
        print(f'limits {limits}') #TODO remove
        print(f'cumsum {np.cumsum(data_hist)}') #TODO remove
        print(f'median number {median_num}') #TODO remove
        print(f'approximated median {median_approx}')



    end = time.time()
    print(f'Calculations finished in: {end-start}')

    ##Used to check the answers
    if args.built_in:
        print('\nPyspark stats:')
        data_stats = data.stats()
        print(data_stats)

        data_hist = data.histogram(10)
        print(f'histogram {data_hist[1]}')

        end2 = time.time()
        print(f'Calculations for pyspark stats were: {end2-end}')
