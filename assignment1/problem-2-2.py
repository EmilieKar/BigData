import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
from matplotlib import pyplot as plt
from time import perf_counter

def sample_pi(n):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed()
    print("Hello from a worker")
    s = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            s += 1
    return s


def compute_pi(steps, workers):
    random.seed(1)
    n = int(steps / workers)
    
    p = multiprocessing.Pool(workers)
    s = p.map(sample_pi, [n]*workers)

    n_total = n*workers
    s_total = sum(s)
    pi_est = (4.0*s_total)/n_total
    print(" Steps\tSuccess\tPi est.\tError")
    print("%6d\t%7d\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, pi-pi_est))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot speedup when increasing worker count for Monte Carlo simulation aproximating pi.')
    parser.add_argument('--steps', '-s',
                        default='1000000',
                        type = int,
                        help='Number of steps in the Monte Carlo simulation')
    args = parser.parse_args()
    workers_list = [1,2,4,8,16,32]
    execution_times = []

    for workers in workers_list:
        start = perf_counter()
        compute_pi(args.steps, workers)
        end = perf_counter()
        execution_times.append(end-start)
        print(f'time:{end-start} workers:{workers}')

    actual_speedup = [execution_times[0]/e for e in execution_times]
    theoretical_speedup = workers_list

    plt.scatter(workers_list, actual_speedup, alpha=0.7, label="Actual speedup")
    plt.scatter(workers_list, theoretical_speedup, alpha=0.7, label="Theoretical speedup")
    plt.xlabel('Number of workers')
    plt.ylabel('Speedup')
    plt.title('Actual and theoretical speedup when running ' + 
              str(args.steps) + '\nMonte Carlo simulation steps')
    plt.legend()
    plt.savefig('fig.png')
