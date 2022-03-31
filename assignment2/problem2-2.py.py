import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
import time
from matplotlib import pyplot as plt

def sample_pi(n, queue, ret_queue, seed):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed(seed)
    ret_queue.cancel_join_thread() #let the process exit without waiting for ret_queue to become empty
    print(f'{multiprocessing.current_process().name} created')

    while True:
        msg = queue.get()
        if msg == 'STOP':
            print(f'{multiprocessing.current_process().name} quits')
            #print(f'Worker recived message {msg}')
            break
        
        s = 0
        for i in range(n):
            x = random.random()
            y = random.random()
            if x**2 + y**2 <= 1.0:
                s += 1

        ret_queue.put(s)
    
# Adds jobs to queue if accuracy < desired accuracy 
# Adds "STOP" to queue if desired accuracy is reached
def compute_pi(queue, ret_queue, step, acc, nmb_workers):
    s = 0
    n = 0
    while True:
        s+= ret_queue.get()
        n+= step
        pi_est = (4.0*s)/n

        if abs(pi_est - pi)  <= acc:
            for _ in range(nmb_workers):
                queue.put('STOP')
            
            return pi_est, n
        else:
            queue.put('DO')
    

def run(args, workers = None):
    queue = multiprocessing.Queue()
    ret_queue = multiprocessing.Queue()

    if workers == None:
        workers = args.workers

    for _ in range(workers):
        queue.put('DO')

    # Start worker processes
    worker_list = []
    for i in range(workers):
        curr_worker = multiprocessing.Process(target=sample_pi, args=(args.steps, queue, ret_queue, i + args.seed), daemon=True)
        curr_worker.start()
        worker_list.append(curr_worker)

    print(worker_list)
    pi_est, n = compute_pi(queue, ret_queue, args.steps, args.accuracy, workers)
        
    print(f'queue size {queue.qsize()}')
    [w.join() for w in worker_list]
    
    print(f'program finished with estimation {pi_est} using {n} samples')
    return n

def time_run(args, k_list):
    ret_list = []
    base_samp_per_sec = None
    samp_per_sec_list = []
    print(f'Timed run with accuracy {args.accuracy} step size {args.steps} for workers {k_list}')
    for k in k_list:
        print(f'\n\nNew run with {k} workers')
        start_time = time.time()
        n = run(args, k)
        stop_time = time.time()
        ret_list.append((stop_time-start_time)/(n /args.steps))
        samp_per_sec_list.append(n/(stop_time-start_time))
        
        #print for debugg
        if base_samp_per_sec is None:
            base_samp_per_sec = n/(stop_time-start_time)
        print(f'workers:{k}, samples per second {n/(stop_time-start_time)}, speedup:{n/(stop_time-start_time)/base_samp_per_sec}, number of worker computations needed:{n /args.steps}')



    actual_speedup = [s/base_samp_per_sec for s in samp_per_sec_list]
    theoretical_speedup = k_list

    plt.scatter(k_list, actual_speedup, alpha=0.7, label="Actual speedup")
    plt.scatter(k_list, theoretical_speedup, alpha=0.7, label="Theoretical speedup")
    plt.xlabel('Number of workers')
    plt.ylabel('Speedup')
    plt.title(f'Actual and theoretical speedup when running Monte Carlo \n simulation with accuracy {args.accuracy} and step size {args.steps}')
    plt.legend()
    plt.savefig('fig.png')
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

    print(time_run(args, [1,2,4,8,16,32]))
