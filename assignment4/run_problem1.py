"""
    This file is used to run the kmeans mrjob program.

    It generates the data and saves to a file. Then runs the mrjob program on this file

    To check the correctness first run the kmeans.py program one iteration with the -d parameter
    Then run this script with the same parameters as the kmeans program but set the --centers 
    parameter to the same starting centers used by kmeans (these are printed with the -d parameter).
    Note that this works since the data generator is using a fixed seed to the only randomness in
    the algorithms is the initial centeroids.
"""

import logging
import argparse
import numpy as np
from sklearn.datasets import make_blobs
import time
from problem1 import Kmeans_iteration_mrjob

def generateData(n, c):
    logging.info(f"Generating {n} samples in {c} classes")
    X, y = make_blobs(n_samples=n, centers = c, cluster_std=1.7, shuffle=False,
                      random_state = 2122)
    return X

def computeClustering(args):
    if args.verbose:
        logging.basicConfig(format='# %(message)s',level=logging.INFO)
    if args.debug: 
        logging.basicConfig(format='# %(message)s',level=logging.DEBUG)

    
    X = generateData(args.samples, args.classes)

    # Writing data to a file
    with open(args.data_file, "w") as file:
        for line in X:
            file.write(" ".join(map(str, line)) + '\n')

    start_time = time.time()

    if not args.centers:
        centers = X[np.random.choice(np.array(range(args.samples)),size=args.k_clusters,replace=False)].flatten().tolist()
    else:
        centers = args.centers
    centers_arg = str(centers).replace('[', '').replace(']','').replace(',','')
    
    mrjob_args = [args.data_file, '-r', args.runner, '--num-cores', str(args.cores), '--centers'] +  centers_arg.split()
    print(f'argument list for mrjob {mrjob_args}')
    mr_job = Kmeans_iteration_mrjob(args=mrjob_args)
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            print (key, value)

    end_time = time.time()
    logging.info("Clustering complete in %3.2f [s]" % (end_time - start_time))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute a k-means clustering.',
        epilog = 'Example: kmeans.py -v -k 4 --samples 10000 --classes 4 --plot result.png'
    )
    parser.add_argument('--cores',
                        default='1',
                        type = int,
                        help='Number of cores to request for mrjob)')
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
    parser.add_argument('--runner', '-r',
                        default='inline',
                        type = str,
                        help='Type of mrjob runner, inline or local')
    parser.add_argument('--data_file',
                        type = str,
                        default="data.dat",
                        help='Filename of file where the generated data is saved for mrjob')
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Print verbose diagnostic output')
    parser.add_argument('--debug', '-d',
                        action='store_true',
                        help='Print debugging output')
    parser.add_argument('--centers',
                        type=float,
                        nargs="+",
                        help='List of centers to use')
    args = parser.parse_args()
    computeClustering(args)

