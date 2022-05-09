import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
import time
import argparse
import re

#returns a list of key, value pairs for the input line the
#values are the words filtered for only a-z and the key for
#each value is the first two characters in the word
#but only 1 character in case of a 1 character word
def sort_mapper (line):
    l = re.sub(r"[^a-z\s]+", "", line.lower()).split()
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

    last_word = lst[0]
    common_sum = 0

    for word in lst[1:]:
        if debug:
            print(last_word, word, chars_common(last_word, word))
        common_sum += chars_common(last_word, word)
        last_word = word

    if debug:
        print('\nlist length: ', lst_len, '\ncommon prefix sum:', common_sum)

    return common_sum/lst_len

def tuple_add(a, b):
    return (a[0] + b[0], a[1] + b[1])

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

    #apply the key,value mapper using flatmap
    data = dataFile.flatMap(sort_mapper)

    #combine all the words of each key, and sort the unique words
    combined = data.combineByKey(to_list, append, extend).mapValues(lambda x: sorted(set(x)))

    #sort the result based on the keys
    result = sorted(combined.collect())

    #produce the final sorted word list
    sorted_words = []
    for (key, values) in result:
        sorted_words.extend(values)

    #calculate the average common word prefix
    ave_common_prefix = ave_common_prefix_len(sorted_words)
    print('\naverage common prefix: ', ave_common_prefix)

    #calculate the number of unique words
    num_unique = len(sorted_words)
    print('number of unique words: ', num_unique)

    #Calculate the total word lenth adn number of words
    (total_word_len, num_words) = data.mapValues(lambda x: (len(x), 1)).reduceByKey(tuple_add).map(lambda x: x[1]).reduce(tuple_add)
    print('average word length: ', total_word_len/num_words)

    end = time.time()
    print(f'\nCalculations finished in: {end-start}')
