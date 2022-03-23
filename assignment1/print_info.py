import subprocess

"""To run on:
    Bayes use: python print_info.py
    Shannon use: bash /opt/local/bin/run_job.sh --partition "cpu-shannon" --script print_info.py
    Markov use: bash /opt/local/bin/run_job.sh --partition "cpu-markov" --script print_info.py
"""

print("Information about CPU")
cmd1 = 'lscpu'
print(cmd1)
#os.system(cmd1)
print(subprocess.check_output(cmd1.split()).decode("utf-8") )

print("---------------------------------------------")
print("Information about the file system")
cmd2 = "df -h"
print(cmd2)
#os.system(cmd2)
print(subprocess.check_output(cmd2.split()).decode("utf-8") )

print("---------------------------------------------")
print("Shell memory usage")
cmd3 = "ps ux"
print(cmd3)
#os.system(cmd3)
print(subprocess.check_output(cmd3.split()).decode("utf-8") )
