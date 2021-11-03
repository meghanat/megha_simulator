# Format of the input trace files
1. Every line in the input trace files, represents one job.
2. In every line:
   1. Each of the fields of the line are separated by space character(s).
   2. The first field is the job arrival time.
   3. The second field is the number of tasks in the job.
   4. The third field is the average duration of a task of the job.
   5. From the fourth field to the end of the line, the fields are the
      durations of the tasks of the job.