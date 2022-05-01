#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import statistics

#Using map reduce to do one iteration of kmeans
#This solution is hardcoded for datapoints with 2 dimensions
class Kmeans_iteration_mrjob(MRJob):
    #returns index of closest centroid and the distance to that centroid
    def nearestCentroid(self, point):
        dist_list = []

        #rounding the answer down since the --center parameter can be of odd length
        for i in range(math.floor(len(self.options.centers)/2)):
            c_i = 2*i
            center = self.options.centers[c_i: c_i + 2]
            dist = math.sqrt(sum((c-p)**2 for (c,p) in zip(center, point)))
            dist_list.append(dist)

        return dist_list.index(min(dist_list)), min(dist_list)


    def configure_args(self):
        super(Kmeans_iteration_mrjob, self).configure_args()
        #This parameter is passed as a list where we assume the datapoints have 2 dimensions
        #That means the --center argument is passed as --center x1 y1 x2 y2 ...
        self.add_passthru_arg(
            '--centers', type=float, nargs="+", help='List of centers to use')

    #yields the index of the closes centroid
    #also the variation to be able to compare with the original kmeans solution
    def mapper(self, _, line):
        datapoint = list(map(float, line.split()))
        cluster, dist = self.nearestCentroid(datapoint)

        yield (cluster, datapoint)
        yield ('variation', dist**2)
    
    def combiner(self, key, values):
        if key == 'variation':
            yield('variation', sum(values))
        
        #Calculate the center of the datapoints
        else:
            # [sum(x), sum(y), n]
            res = [0,0,0]

            for v in values:
                res[0] += v[0]
                res[1] += v[1]
                res[2] += 1

            yield (key, res)

    #yeild the variation and the new centers
    def reducer(self, key, values):
        if key == 'variation':
            yield('variation', sum(values))
        
        #Calculate the center of the datapoints
        else:
            n = 0
            v_sum = [0,0]

            for v in values:
                v_sum[0] += v[0]
                v_sum[1] += v[1]
                n += v[2]

            center = [v/n for v in v_sum]
            yield (key, center)
    
    def final_reducer(self, key, values):
        if key == 'variation':
            yield('variation', values)
        
        else: 
            yield ('Centroids', [v for v in values])
    
    def steps(self):
        return[
            MRStep(
                mapper = self.mapper,
                combiners = self.combiner,
                reducer = self.reducer),
            MRStep(
                reducer = self.final_reducer)
        ]

if __name__ == '__main__':
    Kmeans_iteration_mrjob.run()
