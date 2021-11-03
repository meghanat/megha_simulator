# Trace Dataset File Format Description:

The format of the input trace dataset files is as follows:
1. Each trace file ends with the `.tr` file extension.
2. Every line in the trace dataset file represents a job.
3. The format for a line in the trace dataset file is as follows:
   1. The fields in a line are separated by space character(s).
   2. The first field is the line is the job arrival time.
   3. The second field is the number of tasks in the job., say `x`.
   4. The third field is the average duration of a task of the job.
   5. The remaining `x` fields in the line indicate the durations of the individual tasks of the job.