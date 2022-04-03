import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
import time
from matplotlib import pyplot as plt

#this if the function that the workers will run. They sample and send the result to the ret_queue. If it recieves a STOP on the queue it stops.
def sample_pi(n, queue, ret_queue, seed):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed(seed)
    ret_queue.cancel_join_thread() #let the process exit naturally without waiting for ret_queue to become empty

    while True:
        msg = queue.get()
        if msg == 'STOP':
            break
        
        s = 0
        for i in range(n):
            x = random.random()
            y = random.random()
            if x**2 + y**2 <= 1.0:
                s += 1

        ret_queue.put(s)
    
# The program of the main thread
# Adds jobs to queue if accuracy < desired accuracy 
# Adds "STOP" to queue if desired accuracy is reached
# Return the pi estimation and the number of samples it is based on
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

# This functin start a run of the system, creating the queues and spawning the workers
# Returns the pi estimation, number of samples and the worker list
# Note that the wrker list is returned so that we are able to join all the processes outside of the timed part of the program
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

    pi_est, n = compute_pi(queue, ret_queue, args.steps, args.accuracy, workers)
        
    return pi_est, n, worker_list

#Times a run of our parallel implementation of the pi approximation
#Returns number of samples drawn and total time
def time_run(args):
    print(f'Timed run with accuracy {args.accuracy} step size {args.steps} and with {args.workers} workers')

    start_time = time.time()
    pi_est, n, worker_list = run(args)
    stop_time = time.time()

    work_duration = stop_time - start_time

    #Wait for all the workers to quit
    [w.join() for w in worker_list]
    #print(worker_list)

    print(f"""program finished:
            Duration: {work_duration}s
            Estimation {pi_est}
            Samples {n}
            Samples per second {n/work_duration}
            Worker cycles {n/args.steps}
            Average cycles per worker {n/args.steps/args.workers}""")
    return n, (stop_time - start_time)

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
                        default='100000',
                        type = int,
                        help='Number of steps for each increment in the Monte Carlo simulation')
    parser.add_argument('--seed',
                        default='1',
                        type = int,
                        help='Random seed for generator')
    parser.add_argument('--plot',
                        action='store_true',
                        default=False,
                        help='Plots a speedup graph')
    parser.add_argument('--min_time',
                        default='0',
                        type = int,
                        help='Reruns the program until the calculation takes this long')
    args = parser.parse_args()

    # Plots a speedup graph
    if args.plot:
        worker_list = [1, 2, 4, 6, 8, 12, 16, 24, 32]
        actual_speedup = []
        base_samp_per_sec = None
        
        for worker in worker_list:
            args.workers = worker
            n = 0
            duration = 0
            
            #Reruns the computation until it takes at least ags.speed seconds to complete
            while True:
                args.seed = random.randint(0, 10000000)
                print(f'seed {args.seed}')
                n, duration = time_run(args)
                if duration >= args.min_time:
                    print('\nDone')
                    break
                else:
                    print('\nRerun')
            
            if base_samp_per_sec is None:
                base_samp_per_sec = n/duration

            actual_speedup.append(n/duration/base_samp_per_sec)

        theoretical_speedup = worker_list

        #Construct and save the plot
        plt.scatter(worker_list, actual_speedup, alpha=0.7, label="Actual speedup")
        plt.scatter(worker_list, theoretical_speedup, alpha=0.7, label="Theoretical speedup")
        plt.xlabel('Number of workers')
        plt.ylabel('Speedup')
        plt.title(f'Actual and theoretical speedup when running Monte Carlo \n simulation with accuracy {args.accuracy} and step size {args.steps}')
        plt.legend()
        plt.savefig('fig.png')

        print(f"""workers {worker_list}\nactual speedup {actual_speedup}""")

    elif args.min_time != 0:
        n = 0
        duration = 0
        #Reruns the computation until it takes at least ags.speed seconds to complete
        while True:
            args.seed = random.randint(0, 10000000)
            print(f'seed {args.seed}')
            n, duration = time_run(args)
            if duration >= args.min_time:
                print('\nDone')
                break
            else:
                print('\nRerun')

    else:
        time_run(args)
