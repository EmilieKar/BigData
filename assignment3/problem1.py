#!/usr/bin/env python


import multiprocessing
import logging
import argparse
import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs
import time

def generateData(n, c):
    logging.info(f"Generating {n} samples in {c} classes")
    X, y = make_blobs(n_samples=n, centers = c, cluster_std=1.7, shuffle=False,
                      random_state = 2122)
    return X


def nearestCentroid(datum, centroids):
    # norm(a-b) is Euclidean distance, matrix - vector computes difference
    # for all rows of matrix
    dist = np.linalg.norm(centroids - datum, axis=1)
    return np.argmin(dist), np.min(dist)

#takes data and centroids as input and
#return c, cluster_sizes, variation centroids_data_sum
#this is the function for the multiprocessing map workers
def calculateClusters(data, k, centroids):
    N = len(data)
    centroids_data_sum = np.zeros(centroids.shape)

    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    # Assign data points to nearest centroid
    variation = np.zeros(k)
    cluster_sizes = np.zeros(k, dtype=int)

    for i in range(N):
        cluster, dist = nearestCentroid(data[i],centroids)
        c[i] = cluster
        cluster_sizes[cluster] += 1
        variation[cluster] += dist**2
        centroids_data_sum[c[i]] += data[i]

    return c, cluster_sizes, variation, centroids_data_sum


def kmeans(args, data):
    N = len(data)
    k = args.k_clusters
    nr_iter = args.iterations
    workers = args.workers

    # Choose k random data points as centroids
    centroids = data[np.random.choice(np.array(range(N)),size=k,replace=False)]
    logging.debug("Initial centroids\n", centroids)

    N = len(data)

    # Assign data points to nearest centroid
    variation = np.zeros(k)

    logging.info("Iteration\tVariation\tDelta Variation")
    total_variation = 0.0

    #creating a pool of workers
    pool = multiprocessing.Pool(workers)
    for j in range(nr_iter):
        logging.debug("=== Iteration %d ===" % (j+1))

        #Constructing a parameter list and calling map on the pool
        #blocking while waiting for results
        parameterList = list(zip(np.array_split(data, workers), [k]*workers, [centroids]*workers))
        mapResult = pool.starmap(calculateClusters, parameterList)
        
        #decoding the results
        [c_results, cluster_sizes_reults, variation_results, centroids_data_sum_result] = list(zip(*mapResult))

        #transforming the results
        c = np.concatenate(c_results)
        cluster_sizes = np.sum(cluster_sizes_reults, axis=0)
        variation = np.sum(variation_results, axis=0)
        centroids_data_sum = np.sum(centroids_data_sum_result, axis=0)

        delta_variation = -total_variation
        total_variation = sum(variation) 
        delta_variation += total_variation
        logging.info("%3d\t\t%f\t%f" % (j, total_variation, delta_variation))

        # Recompute centroids
        centroids = np.zeros((k,2)) # This fixes the dimension to 2

        centroids = centroids_data_sum / cluster_sizes.reshape(-1,1)
        
        logging.debug(cluster_sizes)
        logging.debug(c)
        logging.debug(centroids)

    pool.close()
    pool.join()
    
    return total_variation, c


def computeClustering(args):
    if args.verbose:
        logging.basicConfig(format='# %(message)s',level=logging.INFO)
    if args.debug: 
        logging.basicConfig(format='# %(message)s',level=logging.DEBUG)

    X = generateData(args.samples, args.classes)

    start_time = time.time()
    #
    # Modify kmeans code to use args.worker parallel threads
    total_variation, assignment = kmeans(args, X)
    #
    #
    end_time = time.time()
    logging.info("Clustering complete in %3.2f [s]" % (end_time - start_time))
    print(f"Total variation {total_variation}")
    print(f"Total time: {end_time - start_time}")

    if args.plot: # Assuming 2D data
        fig, axes = plt.subplots(nrows=1, ncols=1)
        axes.scatter(X[:, 0], X[:, 1], c=assignment, alpha=0.2)
        plt.title("k-means result")
        #plt.show()        
        fig.savefig(args.plot)
        plt.close(fig)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute a k-means clustering.',
        epilog = 'Example: kmeans.py -v -k 4 --samples 10000 --classes 4 --plot result.png'
    )
    parser.add_argument('--workers', '-w',
                        default='1',
                        type = int,
                        help='Number of parallel processes to use (NOT IMPLEMENTED)')
    parser.add_argument('--k_clusters', '-k',
                        default='3',
                        type = int,
                        help='Number of clusters')
    parser.add_argument('--iterations', '-i',
                        default='100',
                        type = int,
                        help='Number of iterations in k-means')
    parser.add_argument('--samples', '-s',
                        default='10000',
                        type = int,
                        help='Number of samples to generate as input')
    parser.add_argument('--classes', '-c',
                        default='3',
                        type = int,
                        help='Number of classes to generate samples from')   
    parser.add_argument('--plot', '-p',
                        type = str,
                        help='Filename to plot the final result')   
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Print verbose diagnostic output')
    parser.add_argument('--debug', '-d',
                        action='store_true',
                        help='Print debugging output')
    args = parser.parse_args()
    computeClustering(args)

