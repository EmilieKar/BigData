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
    def mapper_s1(self, _, line):
        value = float(line.split()[2])
        yield ('s1_map_value', value)

    def combiner_s1(self, key, values):
        values = list(values)
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
                yield ('s1_result', (value, mean, st_dev, max_val, min_val, median_val))


    def bin_mapper(self, _, values):
        [value, mean, st_dev, max_val, min_val, median] = values
        bins = np.histogram(value, range=(min_val, max_val), bins=10)[0].tolist()

        yield ('bin_map', (bins, mean, st_dev, max_val, min_val, median))

    def bin_combiner(self, _, values):
        stats_list = list(zip(*values))
        mean = stats_list[1][0]
        st_dev = stats_list[2][0]
        max_val = stats_list[3][0]
        min_val = stats_list[4][0]
        median = stats_list[5][0]
        bins = np.sum(np.array(stats_list[0], dtype=int), axis=0).tolist()

        yield ('bin_comb', (bins, mean, st_dev, max_val, min_val, median))

    def final_reducer(self, _, values):
        stats_list = list(zip(*values))

        yield ('bin_comb', np.sum(np.array(stats_list[0]), axis=0).tolist())
        yield ('mean',  stats_list[1][0])
        yield ('stdev', stats_list[2][0])
        yield ('max', stats_list[3][0])
        yield ('min', stats_list[4][0])
        yield ('median', stats_list[5][0])


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
