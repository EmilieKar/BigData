from tracemalloc import start
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict, Counter
import numpy as np
import re
import math
import statistics
from operator import add

#mean and standard deviation of the values,
#their minimal and maximal value,
#as well as the counts necessary for producing a histogram; i.e. for how many records does the value fall into a specific bin.
class ValueStats(MRJob):
    
    def mapper_s1(self, input, line):
        value = float(line.split()[2])
        yield ('key', value)

    def combiner_s1(self, key, values):
        #counter = Counter(values)
        start_val = next(values)
        sum, max_val, min_val = start_val
        nmb_terms = 1

        for val in values: 
            sum += val
            if val > max_val: 
                max_val = val 
            if val < min_val:
                min_val = val 
            nmb_terms += 1

        yield ('max_val', max_val)
        yield ('min_val', min_val)
        yield ('sum', sum)
        yield ('nmb_terms', nmb_terms)

    def reducer_s1(self, key, values):
        stats_list = list(zip(*values))
        if key == 'max_val':
            global max_val
            max_val = next(values)
            for val in values: 
                if val > max_val: 
                    max_val = val 
        elif key == 'min_val': 
            global min_val
            min_val = next(values)
            for val in values: 
                if val < min_val: 
                    min_val = val
        elif key == 'sum':
            global sum
            sum = sum(values)
        elif key == 'nmb_terms':
            global nmb_terms
            nmb_terms = sum(nmb_terms)
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_s1,
                combiner=self.combiner_s1,
                reducer=self.reducer_s1)
        ]

class ValueStats2(MRJob):

    def __init__(self, args):
        self.max = args['max_value']
        
    def bin_mapper(self, _, values):
        [value, mean, st_dev, max_val, min_val] = values
        bins = np.histogram(value, range=(min_val, max_val), bins=10)[0].tolist()

        yield ('bin_map', (bins, mean, st_dev, max_val, min_val))

    def bin_combiner(self, _, values):
        stats_list = list(zip(*values))
        mean = stats_list[1][0]
        st_dev = stats_list[2][0]
        max_val = stats_list[3][0]
        min_val = stats_list[4][0]
        bins = np.sum(np.array(stats_list[0], dtype=int), axis=0).tolist()

        yield ('bin_comb', (bins, mean, st_dev, max_val, min_val))

    def final_reducer(self, _, values):
        stats_list = list(zip(*values))

        yield ('bin_comb', np.sum(np.array(stats_list[0]), axis=0).tolist())
        yield ('mean',  stats_list[1][0])
        yield ('stdev', stats_list[2][0])
        yield ('max', stats_list[3][0])
        yield ('min', stats_list[4][0])


    def steps(self):
        return [
            MRStep(
                mapper=self.bin_mapper,
                combiner=self.bin_combiner,
                reducer=self.final_reducer)
        ]

if __name__ == '__main__':
    ValueStats.run()
