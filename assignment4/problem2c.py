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

def mapper (line):
    l = line.split()
    return (int(l[1]), float(l[2]))

def stats_vector(a):
    return (1, a, a**2)

def remove_key (a):
    return a[1]

def add(a, b):
    return a + b

def tuple_add(a, b):
    return [sum(x) for x in zip(a,b)]

#imput is two tuples
#output is (min, max) tuple
def min_max(a,b):
    return (min(a[0],b[0]), max(a[1],b[1]))

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

    distFile = sc.textFile(args.file)
    print('Timing started')
    start = time.time()

    data = distFile.map(mapper)

    #calculate the number of rows, the sum of the values and the sum of the squares
    (data_terms, data_sum, data_sq_sum) = data.mapValues(stats_vector).reduceByKey(tuple_add).map(remove_key).reduce(tuple_add)

    #Calculate the mean and standard deviation
    data_mean = data_sum/data_terms
    std_dev = math.sqrt(data_sq_sum/data_terms - data_mean**2)

    #calculate the min and max
    (data_min, data_max) = data.mapValues(lambda a: (a,a)).reduceByKey(min_max).map(remove_key).reduce(min_max)

    #Calculate the histogram
    bins = 10
    limits = np.linspace(data_min, data_max, bins+1)
    data_hist_indexes = data.map(remove_key).map(lambda x: binner(x, limits)).filter(lambda x: x is not None)
    data_hist_part = data_hist_indexes.reduceByKey(lambda a, b: a + b).collect()
    data_hist = [0]*bins
    for (i, c) in data_hist_part:
        data_hist[i] = c

    #approximate the median as the center of the bin containing the median
    median_num = math.ceil(data_terms/2) #set a fixed median number (index + 1), we do not use average in case of even number of datapoints
    bin_end = 0
    median_approx = None
    for (i, h) in enumerate(data_hist):
        bin_end += h
        if bin_end >= median_num:
            #right bin found
            median_approx = (limits[i] + limits[i + 1])/2
            break
    
    print(f'The results\n terms {data_terms}\n mean {data_mean}\n standard deviation {std_dev}\n min {data_min}\n max {data_max}\n histogram {data_hist}')
    if median_approx is not None:
        print(f' approximated median {median_approx}')

    end = time.time()
    print(f'Calculations finished in: {end-start}')

    ##Used to check the answers
    if args.built_in:
        datapy = data.map(remove_key)
        print('\nPyspark stats:')
        data_stats = datapy.stats()
        print(data_stats)

        data_hist = datapy.histogram(10)
        print(f'histogram {data_hist[1]}')

        end2 = time.time()
        print(f'Calculations for pyspark stats were: {end2-end}')
