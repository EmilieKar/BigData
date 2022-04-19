from mrjob.job import MRJob
import numpy as np
import math
import statistics

#mean and standard deviation of the values,
#their minimal and maximal value,
#as well as the counts necessary for producing a histogram; i.e. for how many records does the value fall into a specific bin.
class ValueStats_s1(MRJob):
    def mapper(self, _, line):
        value = float(line.split()[2])
        yield (None, value)

    def combiner(self, _, values):
        values = list(values)

        part_sum = sum(values)
        part_sum_sq = sum(map(lambda x: x**2, values))
        sum_terms = len(values)
        max_val = max(values)
        min_val = min(values)
        yield (None, (part_sum, part_sum_sq, sum_terms, max_val, min_val))

    def reducer(self, _, values):
        stats_list = list(zip(*values))
        
        num_values = sum(stats_list[2])
        mean = sum(stats_list[0]) / num_values
        mean_sq = sum(stats_list[1]) / num_values

        st_dev = math.sqrt(mean_sq - mean**2)

        max_val = max(stats_list[3])
        min_val = min(stats_list[4])
        yield ('max', max_val)
        yield ('min', min_val)
        yield ('mean', mean)
        yield ('std', st_dev)

if __name__ == '__main__':
    ValueStats_s1.run()
