import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
import time

def sample_pi(queue, ret_queue, seed):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed(seed)

    while True:
        msg = queue.get()
        if msg == 'STOP':
            #print(f'Worker recived message {msg}')
            break
        
        s = 0
        for i in range(10000):
            x = random.random()
            y = random.random()
            if x**2 + y**2 <= 1.0:
                s += 1

        ret_queue.put(s)
    
# Adds jobs to queue if accuracy < desired accuracy 
# Adds "DONE" to queue if desired accuracy is reached
def compute_pi(queue, ret_queue, step, acc, nmb_workers):
    s = 0
    n = 0
    while True:
        s+= ret_queue.get()
        n+= step
        pi_est = (4.0*s)/n

        if abs(pi_est - pi)  <= acc:
            for _ in range(nmb_workers -1):
                queue.put('STOP')
            
            return pi_est, n
        else:
            queue.put('DO')
    

def run(args, workers = None):
    queue = multiprocessing.Queue()
    ret_queue = multiprocessing.Queue()

    if workers == None:
        workers = args.workers

    for _ in range(workers - 1):
        queue.put('DO')

    # Start worker processes
    for i in range(workers - 1):
        multiprocessing.Process(target=sample_pi, args=(queue, ret_queue, i + args.seed)).start()

    pi_est, n = compute_pi(queue, ret_queue, args.steps, args.accuracy, workers)
    #print(f'program finished with estimation {pi_est} using {n} samples')
    return n

def time_run(args, k_list):
    ret_list = []
    for k in k_list:
        start_time = time.time()
        n = run(args, k)
        stop_time = time.time()
        ret_list.append((stop_time-start_time)/(n /10000))
    return ret_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compute Pi using Monte Carlo simulation.')
    parser.add_argument('--workers', '-w',
                        default='3',
                        type = int,
                        help='Number of parallel processes')
    parser.add_argument('--accuracy', '-a',
                        default='0.00001',
                        type = float,
                        help='Accuracy of estimation')
    parser.add_argument('--steps', '-s',
                        default='10000',
                        type = int,
                        help='Number of steps for each increment in the Monte Carlo simulation')
    parser.add_argument('--seed',
                        default='1',
                        type = int,
                        help='Random seed for generator')
    args = parser.parse_args()

    print(time_run(args, [2,3,4]))
