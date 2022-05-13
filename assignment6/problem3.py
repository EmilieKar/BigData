import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import time
import argparse



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sample keys according to frequencies')
    parser.add_argument('-n',
                        type=int,
                        default=10000,
                        help='Set the number of keys to generate')
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
    conf.setAppName("number-of-samples")
    conf.set("spark.local.dir", "./runinfo")

    sc = SparkContext.getOrCreate(conf)
    
    print(sc.getConf().getAll())

    dataFile = sc.textFile(args.file, minPartitions=args.w)
    print('Timing started')
    start = time.time()
    
    #sum frequencies

    # calculate cumulative p's [p1, p2, p3 .. pn]
    # e.g. [0.01, 0.02, 0.03 ... 99.9, 100]

    #samples = for n: uniform(0,1)
    # sort(samples) ascending

    # not parallel, one iteration of keys needed.
    # r = 0
    # iterate cumulative p's:
    #  if p < samples[r] < p+1: give key, r += 1

    end = time.time()
    print(f'\nCalculations finished in: {end-start}')
