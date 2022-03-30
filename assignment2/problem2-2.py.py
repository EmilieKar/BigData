import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi

def sample_pi(queue, ret_queue, seed):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed(seed)

    while True:
        msg = queue.get()
        if msg == 'STOP':
            print(f'Worker recived message {msg}')
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
def compute_pi(queue, ret_queue, step, acc):
    s = 0
    n = 0
    while True:
        s+= ret_queue.get()
        n+= step
        pi_est = (4.0*s)/n

        if abs(pi_est - pi)  <= acc:
            for _ in range(args.workers -1):
                queue.put('STOP')
            
            return pi_est, n
        else:
            queue.put('DO')
    

def run(args):
    print('Code started.......')
    queue = multiprocessing.Queue()
    ret_queue = multiprocessing.Queue()

    print('Filling queue......')
    for _ in range(args.workers - 1):
        queue.put('DO')

    print('Start workers......')
    # Start worker processes
    for i in range(args.workers - 1):
        multiprocessing.Process(target=sample_pi, args=(queue, ret_queue, i + args.seed)).start()

    print('Start checker......')
    pi_est, n = compute_pi(queue, ret_queue, args.steps, args.accuracy)
    print(f'program finished with estimation {pi_est} using {n} samples')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compute Pi using Monte Carlo simulation.')
    parser.add_argument('--workers', '-w',
                        default='3',
                        type = int,
                        help='Number of parallel processes')
    parser.add_argument('--accuracy', '-a',
                        default='0.001',
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
    
    run(args)
