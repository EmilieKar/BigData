from problem2a import ValueStats
from problem2a import ValueStats2
import sys
import time

def firstJob(input_file):
    global out_1
    mr_job = ValueStats(args=[input_file])
    with mr_job.make_runner() as runner:
        runner.run()
        out_1 = runner.get_output_dir()

def secondJob(input_file):
    mr_job = ValueStats2(args=[out_1,input_file])
    with mr_job.make_runner() as runner:
        runner.run()

if __name__ == '__main__':
    print(sys.argv)
    start = time.time()
    firstJob(sys.argv[1:])
    secondJob(sys.argv[1:])
    end = time.time()
    print(f'Total time: {end-start}')

    """
    import pprofile
    profiler = pprofile.Profile()

    with profiler:
        with mr_job.make_runner() as runner:
            ValueStats.run()
    
    profiler.print_stats()
    """
