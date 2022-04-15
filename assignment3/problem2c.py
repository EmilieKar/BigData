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
    """
    def configure_args(self):
        super(ValueStats, self).configure_args()
        self.add_passthru_arg(
            '--bins', type=int, default=10, help='Bins for the bucketization')
    """

    """
    def __init__(self, args):
        print(args)
        buckets = 10

        if '-b' in args:
            ind = args.index('-b')
            buckets = int(args[ind + 1])
            args = args[:ind] + args[ind + 2:]

        self.buckets = buckets
        print(args)

        super().__init__(None)
    """

    """
    def mapper_init(self):
        self.values = defaultdict(int)

    def mapper(self, _, line):
        value = float(line.split()[2])
        self.values[value] += 1

    def mapper_final(self):
        values_sum = sum(v*n for (v,n) in self.values.items())
        values_terms = sum(self.values.values())
        print(values_sum, values_terms)
        yield ('sum_terms', (values_sum, values_terms))
        yield ('max', max(self.values.keys()))
        yield ('min', min(self.values.keys()))
    """
    def mapper_s1(self, _, line):
        value = float(line.split()[2])
        yield ('s1_map_value', value)

    def combiner_s1(self, key, values):
        values = list(values)
        #counter = Counter(values)
        part_sum = sum(values)
        part_sum_sq = sum(map(lambda x: x**2, values))
        sum_terms = len(values)
        max_val = max(values)
        min_val = min(values)
        median_val = statistics.median(values)
        yield ('s1_comb_values', (values, part_sum, part_sum_sq, sum_terms, max_val, min_val, median_val))

    def reducer_s1(self, _, values):
        stats_list = list(zip(*values))
        
        num_values = sum(stats_list[3])
        mean = sum(stats_list[1]) / num_values
        mean_sq = sum(stats_list[2]) / num_values

        st_dev = math.sqrt(mean_sq - mean**2)

        max_val = max(stats_list[4])
        min_val = min(stats_list[5])

        median_val = statistics.median(stats_list[6])

        #yield ('s1_result', (mean, st_dev, max_val, min_val))

        for value_list in stats_list[0]:
            for value in value_list:
                yield ('s1_result', (value, mean, st_dev, max_val, min_val))


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
                mapper=self.mapper_s1,
                combiner=self.combiner_s1,
                reducer=self.reducer_s1),
            MRStep(
                mapper=self.bin_mapper,
                combiner=self.bin_combiner,
                reducer=self.final_reducer)
        ]

if __name__ == '__main__':
    ValueStats.run()
