from alt_prob2_filip_1 import ValueStats_s1
from alt_prob2_filip_2 import ValueStats_s2
import sys
import time

def firstJob(args):
    print('Running first job')
    mr_job = ValueStats_s1(args=args)
    add_args = []
    with mr_job.make_runner() as runner:
        runner.run()
        print(f"runner{list(runner.cat_output())}")
        for key, value in mr_job.parse_output(runner.cat_output()):
            add_args.append(f'--{key}')
            add_args.append(str(value))
        
    return add_args

def secondJob(args, add_args):
    print('Running second job')
    mr_job = ValueStats_s2(args=args+add_args)
    with mr_job.make_runner() as runner:
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            print (key, value)

if __name__ == '__main__':
    print(sys.argv)
    args = sys.argv[1:]
    start = time.time()

    add_args = firstJob(args)
    print(add_args)
    secondJob(args, add_args)

    end = time.time()
    print(f'Total time: {end-start}')
