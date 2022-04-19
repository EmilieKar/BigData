from mrjob.job import MRJob
import numpy as np
import math
import statistics

#Return the bin array with a 1 in the correct bin and the rest zeros
def binner(value, min_limit, max_limit, bins):
    bins_list = [0]*bins

    limits = np.linspace(min_limit, max_limit, bins+1)
    for (n, (l_lim, u_lim)) in enumerate(zip(limits[:-1], limits[1:])):
        if l_lim <= value and value <= u_lim:
            bins_list[n] = 1
            return bins_list

    return bins_list

#Calculates the histogram for the input data given the,
#min, max, mean and standard deviation as parameters
class ValueStats_s2(MRJob):
    def configure_args(self):
        super(ValueStats_s2, self).configure_args()
        self.add_passthru_arg(
            '--min', type=float, help='Min value in the data')
        self.add_passthru_arg(
            '--max', type=float,  help='Max value in the data')
        self.add_passthru_arg(
            '--mean', type=float, help='Mean value in the data')
        self.add_passthru_arg(
            '--std', type=float, help='Standard deviation in the data')
        self.add_passthru_arg(
            '--bins', type=int, default=10, help='Bins for the bucketization')
    
    def mapper(self, _, line):
        value = float(line.split()[2])
        bins = binner(value, self.options.min, self.options.max, self.options.bins)

        yield (None, bins)

    def combiner(self, _, values):
        bins = np.sum(list(values), axis=0).tolist()

        yield (None, bins)

    def reducer(self, _, values):
        yield ('bins', np.sum(list(values), axis=0).tolist())
        yield ('mean',  self.options.mean)
        yield ('stdev', self.options.std)
        yield ('max', self.options.max)
        yield ('min', self.options.min)

if __name__ == '__main__':
    ValueStats_s2.run()
