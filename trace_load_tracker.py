"""Record the cluster demand from the trace dataset."""

import os
import sys
import json
import pathlib
from typing import List
from math import ceil, floor


PATH_TO_TRACE_FOLDER = pathlib.Path("./traces/input/")
PATH_TO_OUTPUT_FOLDER = pathlib.Path("./traces/demand/")


NAME_OF_TRACE_FILE = sys.argv[1].strip()

FULL_TRACE_FILE_PATH = PATH_TO_TRACE_FOLDER / NAME_OF_TRACE_FILE

if not os.path.exists(FULL_TRACE_FILE_PATH):
    print(f"Trace File: {FULL_TRACE_FILE_PATH}, not found!")
    exit(1)


timeline: List[int] = [0]*1_000

with open(FULL_TRACE_FILE_PATH) as file_handler:
    for job in file_handler:
        arrival_time, task_count, _, task_durations = job.split(maxsplit=3)

        i_arrival_time = floor(float(arrival_time))
        i_task_count = int(task_count)

        for task_duration in task_durations.split():
            i_task_duration = ceil(float(task_duration))
            completion_time = i_arrival_time + i_task_duration

            for time_point in range(i_arrival_time, completion_time + 1):
                timeline[time_point - 1] += 1

name_of_file, extension = NAME_OF_TRACE_FILE.split('.')
NAME_OF_OUTPUT_FILE = f"{name_of_file}_demand_{extension}"

with open(PATH_TO_OUTPUT_FOLDER / NAME_OF_OUTPUT_FILE, "w") as file_handler:
    for time_point in timeline:
        file_handler.write(f"{time_point}\n")

print("The trace demand file has been generated!")
