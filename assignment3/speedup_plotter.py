import subprocess
import os
import sys
import time
import argparse
from matplotlib import pyplot as plt

def cancel_jobs(job_list):
    for job in job_list:
        cmd = 'scancel ' + str(job)
        ans = subprocess.check_output(cmd.split()).decode("utf-8")

#Example usage of this script:
#python speedup_plotter.py --command 'problem1.py -s 50000 -i 100' --cluster 'cpu-markov'  --w_argument='-w' --w_list '1 2 4 8 12 16 24 32' --time 'Total time:' --slurm_cores +1 --figure 'fig.png' --title 'K-means speedup from 50000 and samples 100 iterations'
#This produces a figure called fig.png that plots the speedups for the python file problem1.py ran with fixed parameters:
#-s 50000 and -i 100 and with different worker parameters -w of 1, 2, 4, 8, 12, 16, 24, 32
#The measured time is fount in the output file on the line starting with 'Total time:'

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Create a speedup graph for a python program')
        parser.add_argument('--command',
                            help='the command to run, excluding the worker parameter')
        parser.add_argument('--w_argument', '-wa',
                            default='-w',
                            help='Set the worker argument name')
        parser.add_argument('--w_list', '-wl',
                            default='1',
                            help='Set the worker list')
        parser.add_argument('--cluster', '-cl',
                            default='cpu',
                            help='Set the cluster to run on, bayes, markov or shannon, cpu and gpu')
        parser.add_argument('--slurm_cores',
                            default=1,
                            type=int,
                            help='Set the slurm cores relative to the number of workers specified in --w_list')
        parser.add_argument('--time',
                            required=True,
                            help='Set identifier for the running time of the command in the output, the time should follow this identification and have unit second.')
        parser.add_argument('--title', '-t',
                            help='Set the title of the plot')
        parser.add_argument('--figure', '-f',
                            default='fig.png',
                            help='Set the file name of the figure')
        parser.add_argument('--no_error_check',
                            action='store_true',
                            help='Disables the check of error file')

        parser.add_argument('--debug', '-d',
                            action='store_true',
                            help='Only prints the command that is supposed to be run, used for debugging')

        args = parser.parse_args()

        username = os.getlogin()

        worker_list = list(map(int, args.w_list.split()))
        user_cmd = args.command.split()
        slurm_cmd = 'bash /opt/local/bin/run_job.sh '
        slurm_cpus = '--cpus-per-task '
        slurm_cluster = ' --partition '
        slurm_script = ' --script '

        squeue_cmd = 'squeue -u ' + username 
        
        job_ids_not_done = []
        job_worker_dict = {}
        job_time_dict = {}

        #enqueing the jobs
        for workers in worker_list:

            #constructing the bash command
            cmd = slurm_cmd + slurm_cpus + str(workers + int(args.slurm_cores)) + slurm_cluster + args.cluster + slurm_script + user_cmd[0] + ' -- ' + " ".join(user_cmd[1:]) + " "

            #enable running scripts without worker parameter
            if args.w_argument:
                cmd += args.w_argument + " " + str(workers)

            print(cmd)


            #running the command, i.e scheduling with slurm
            if not args.debug:
                submission_str = subprocess.check_output(cmd.split()).decode("utf-8")
                print(submission_str)
                job_id = int(submission_str.split()[-1])
                job_ids_not_done.append(job_id)
                job_worker_dict[job_id] = workers


        #if we are debugging then return here
        if args.debug:
            sys.exit()

        #waiting for processes to be vissible in squeue
        time.sleep(1)

        while len(job_ids_not_done) != 0:
            print(job_ids_not_done)
            for job in job_ids_not_done:
                if not args.no_error_check:
                    error_file_name = "./slurm-" + str(job) + ".error"
                    #check if files exist, otherwise skip
                    if os.path.exists(error_file_name):
                        error_file = open(error_file_name)

                        error_lines = error_file.readlines()
                        #if there are error messages raise exception
                        if len(error_lines) != 0:
                            #cancel all jobs
                            cancel_jobs(job_ids_not_done)
                            raise Exception('job ' + str(job) + ' has error:\n' + ''.join(error_lines))

                #check squeue
                squeue_rows = subprocess.check_output(squeue_cmd.split()).decode("utf-8")
                

                #check if job is done
                if str(job) not in squeue_rows:
                    print(f'job {job} is done')
                    out_file_name = "./slurm-" + str(job) + ".out"

                    #check if output file exists
                    if os.path.exists(out_file_name):
                        print(f'file {out_file_name} exists')
                        with open(out_file_name) as out_file:
                            out_lines = out_file.readlines()
                            print(out_lines)

                            #search for time identifier
                            for line in reversed(out_lines):
                                if args.time in line:
                                    #extract the time
                                    job_time = float(line.replace(args.time, "").split()[0])
                                    job_ids_not_done.remove(job)
                                    job_time_dict[job] = job_time


                #wait util we loop again
                time.sleep(1)

        print(job_worker_dict)
        print(job_time_dict)

        #overwrite worker list and recreate it together with the corresponding time list
        worker_list = []
        time_list = []
        for job in job_worker_dict.keys():
            worker_list.append(job_worker_dict[job])
            time_list.append(job_time_dict[job])

        #the base time for 1 worker
        base_time = time_list[worker_list.index(1)]

        actual_speedup = [base_time/t for t in time_list]

        if args.title:
            #Construct and save the plot
            plt.scatter(worker_list, actual_speedup, alpha=0.7, label="Actual speedup")
            plt.scatter(worker_list, worker_list, alpha=0.7, label="Theoretical speedup")
            plt.xlabel('Number of workers')
            plt.ylabel('Speedup')
            plt.title(args.title)
            plt.legend()
            plt.savefig(args.figure)

    except KeyboardInterrupt:
        print("Quiting and canceling all the jobs")
        #cancel all jobs
        cancel_jobs(job_ids_not_done)

