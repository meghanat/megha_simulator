import os
import fnmatch


# def newest(path):
#     all_files = os.listdir(path)
#     # log_files = [file for file in all_files if re.match(r'*.log', file)]
#     log_files = fnmatch.filter(all_files, '*.log')
#     paths = [os.path.join(path, basename) for basename in log_files]
#     return max(paths, key=os.path.getctime)


# curr_path = os.getcwd()

# file = newest(curr_path)

file = '/home/paradigm/Documents/PESU/Capstone/Simulators/megha_simulator/logs/record-2021-07-05-04-51-16_lifo.log'

print(file)

with open(file, encoding='utf-8') as f:
    lines = f.readlines()
    s = set()

    for line in lines:
        words = line.split()

        # If the line has "QueuingDelay", then get the values
        if ('LaunchOnNodeEvent:' in words):
            job_id = int(words[words.index('JobID:') + 1])
            s.add(job_id)
    
    queuingDelay = [{} for _ in range(len(s)+1)]
    all_job_ct = dict()

    # Read line by line
    for line in lines:
        words = line.split()

        # If the line has "LaunchOnNodeEvent", then get the values
        if ('LaunchOnNodeEvent:' in words):
            job_id = int(words[words.index('JobID:') + 1])
            task_id = int(words[words.index('TaskID:') + 1])
            current_time = float(words[words.index('CurrentTime:') + 1])
            start_time = float(words[words.index('JobStartTime:') + 1])
            task_qd = current_time - start_time

            # print(job_id, task_id, task_qd)

            # Add the values into a dictionary
            # print(job_id, task_id)
            queuingDelay[job_id][task_id] = task_qd
        
        elif('UpdateStatusForGM:' in words):
            # 2021-07-05 04:34:03,578 : gm : gm.py : update_status : 95 : INFO : UpdateStatusForGM: JobID: 4, JobCompletionTime: 107.53800000000004
            job_id = int(words[words.index('JobID:') + 1])
            job_ct = float(words[words.index('JobCompletionTime:') + 1])
            all_job_ct[job_id] = job_ct

count = 1

# Writing to file
with open("output_jobs_fifo.txt", "w") as write_file:
    for job in queuingDelay[1:]:
        write_file.write(str(job)+"\n")


# Get all the values in a sorted order QD
l = []
for job in queuingDelay[1:]:
    for i in sorted(job.keys()):
        print(i, end=" ")
        l.append(job[i])

with open("output_qd_fifo.txt", "w") as f:
    f.write(str(l))


# Get all the values in a sorted order JCT
l = []
for i in sorted(all_job_ct.keys()):
    print(i, end=" ")
    l.append(job[i])

with open("output_jct_fifo.txt", "w") as f:
    f.write(str(l))