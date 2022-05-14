import time
import argparse



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sample keys according to frequencies')
    parser.add_argument('-n',
                        type=int,
                        default=10000,
                        help='Set the number of keys to generate')
    parser.add_argument('--file',  '-f',
                        type=str,
                        required=True,
                        help='The file run the program on')

    args = parser.parse_args()


    with open(args.file) as file:
        content = file.read()
    print('Timing started')
    start = time.time()
    
    #sum all frequencies

    # calculate cumulative p's [p1, p2, p3 .. pn]
    # e.g. [0.01, 0.02, 0.03 ... 99.9, 100]

    #samples = for n: uniform(0,1)
    # sort(samples) ascending

    # not parallel, one iteration of keys needed.
    # r = 0
    # iterate cumulative p's:
    #  if p < samples[r] < p+1: give key, r += 1

    end = time.time()
    print(f'\nCalculations finished in: {end-start}')
