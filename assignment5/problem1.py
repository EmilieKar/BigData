import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import time
import argparse
import re

#returns a list of key, value pairs for the input line
#the values are the words filtered for only a-z
#and the key is the two first characters in the value word
#but only 1 caracter in case of a 1 character word
def mapper (line):
    l = list(set(re.sub(r"[^a-z\s]+", "", line.lower()).split()))
    return [(w[0:min(2, len(w))], w) for w in l]

#these 3 functions are used to combine the values corresponding to each key into a single list
def to_list(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a

#counts how many prefix characters the words a and b have in common
def chars_common(a, b):
    res = 0
    for (a_i, b_i) in zip(a,b):
        if a_i == b_i:
            res +=  1
        else: break

    return res

#calculates the average common prefix for the words in lst
#debug is for debug printing
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

    #apply the key,value mapper using flatmap to produce a single list
    data = dataFile.flatMap(mapper)

    #combine all the words of each key, and sort the unique words
    combined = data.combineByKey(to_list, append, extend).mapValues(lambda x: sorted(set(x)))

    #sort the result based on the keys
    result = sorted(combined.collect())

    #produce the final sorted word list
    sorted_word = []
    for (key, values) in result:
        sorted_word.extend(values)

    #calculate the average common word prefix
    print('\naverage common prefix: ', ave_common_prefix_len(sorted_word))

    end = time.time()
    print(f'Calculations finished in: {end-start}')
