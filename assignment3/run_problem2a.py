from problem2a import ValueStats
import sys
import time

if __name__ == '__main__':
    print(sys.argv)
    mr_job = ValueStats(args=sys.argv[1:])
    
    with mr_job.make_runner() as runner:
        start = time.time()
        ValueStats.run()
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
