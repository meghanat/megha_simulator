"""Record the cluster demand from the trace dataset in servers per second."""

import os
import sys
import pathlib
from math import ceil, floor


PATH_TO_TRACE_FOLDER = pathlib.Path("./traces/input/")

if len(sys.argv) != 4:
    print("ERROR: Insufficient number of arguments provided!")
    print("Arguments required are: <Trace_file_name> <Start_time> <End_time>")
    sys.exit(-1)

NAME_OF_TRACE_FILE = sys.argv[1].strip()
START_TIME = int(sys.argv[2].strip())
END_TIME = int(sys.argv[3].strip())

# Check if the trace file exists
FULL_TRACE_FILE_PATH = PATH_TO_TRACE_FOLDER / NAME_OF_TRACE_FILE

if not os.path.exists(FULL_TRACE_FILE_PATH):
    print(f"Trace File: {FULL_TRACE_FILE_PATH}, not found!")
    exit(1)

# Assert that the time values are valid
assert START_TIME <= END_TIME, ("ERROR: The starting time and the ending "
                                "time must be in ascending order!")


interval_task_count: int = 0
interval_job_count: int = 0

with open(FULL_TRACE_FILE_PATH) as file_handler:
    for job in file_handler:
        raw_arrival_time, raw_task_count, _, task_durations_string = \
            job.split(maxsplit=3)

        arrival_time = float(raw_arrival_time)
        task_count = int(raw_task_count)

        is_job_in_time_range: bool = False

        for raw_task_duration in task_durations_string.split():
            task_duration = float(raw_task_duration)

            # Slice - 1
            duration_to_nearest_int_time = ceil(arrival_time) - arrival_time

            # Update the task_duration to reflect only the remaining
            # unaccounted running time
            task_duration -= duration_to_nearest_int_time
            # If all the task_duration has been accounted for then
            # move to the next task
            if task_duration <= 0:
                continue

            # Slice - 2
            remaining_int_task_duration = floor(task_duration)

            # Update the task_duration to reflect only the remaining
            # unaccounted running time
            task_duration -= remaining_int_task_duration
            # If all the task_duration has been accounted for then
            # move to the next task
            if task_duration <= 0:
                continue

            # Slice - 3
            ind_start_time: int = (ceil(arrival_time) +
                                   remaining_int_task_duration)

            # This value is one after the last second of the task's execution
            ind_end_time: int = (ceil(arrival_time) +
                                 remaining_int_task_duration + 1)

            assert task_duration < 1, ("Task duration was NOT less than 1, in "
                                       "the 3rd slice of the task_duration! "
                                       f"{task_duration=}")

            # Tasks and jobs on the limits of the interval are also included
            if (arrival_time > END_TIME or (ind_end_time - 1) < START_TIME):
                ...
            else:
                interval_task_count += 1
                is_job_in_time_range = True

        if is_job_in_time_range is True:
            interval_job_count += 1


TITLE_STRING: str = (f"For the trace file {NAME_OF_TRACE_FILE} "
                     f"({START_TIME} - {END_TIME}):")
print(TITLE_STRING)
print("-" * len(TITLE_STRING))
print(f"\tTotal number of tasks: {interval_task_count}")
print(f"\tTotal number of jobs: {interval_job_count}")
