from mrjob.job import MRJob
import numpy as np
import math
import statistics

#This is the same as the stage1 in problem 2a but with the addition of median
#the median is aproximated as median of medians
#Calculates the min, max, mean and standard deviation of the values
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
        median_val = statistics.median(values)
        yield (None, (part_sum, part_sum_sq, sum_terms, max_val, min_val, median_val))

    def reducer(self, _, values):
        stats_list = list(zip(*values))
        
        num_values = sum(stats_list[2])
        mean = sum(stats_list[0]) / num_values
        mean_sq = sum(stats_list[1]) / num_values

        st_dev = math.sqrt(mean_sq - mean**2)

        max_val = max(stats_list[3])
        min_val = min(stats_list[4])

        median_val = statistics.median(stats_list[5])

        yield ('max', max_val)
        yield ('min', min_val)
        yield ('mean', mean)
        yield ('std', st_dev)
        yield ('median', median_val)

if __name__ == '__main__':
    ValueStats_s1.run()
