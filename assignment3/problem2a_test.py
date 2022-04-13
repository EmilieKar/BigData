import pandas
import math
import statistics
import sys
import os
import numpy as np

if __name__ == '__main__':
    value_list = []
    path = sys.argv[1]

    if os.path.exists(path):
        with open(path) as file:
            for line in file.readlines():   
                value_list.append(float(line.split()[2]))

    max_val = max(value_list)
    min_val = min(value_list)
    print('max', max_val)
    print('min', min_val)
    print('mean', statistics.mean(value_list))
    print('standard deviation', statistics.stdev(value_list))
    print('bins', np.histogram(value_list, range=(min_val, max_val), bins=10)[0].tolist())
