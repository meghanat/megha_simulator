"""Record the cluster demand from the trace dataset in servers per second."""

import os
import sys
import pathlib
from typing import List, Union
from math import ceil, floor


PATH_TO_TRACE_FOLDER = pathlib.Path("./traces/input/")
PATH_TO_OUTPUT_FOLDER = pathlib.Path("./traces/demand/")


NAME_OF_TRACE_FILE = sys.argv[1].strip()

FULL_TRACE_FILE_PATH = PATH_TO_TRACE_FOLDER / NAME_OF_TRACE_FILE

if not os.path.exists(FULL_TRACE_FILE_PATH):
    print(f"Trace File: {FULL_TRACE_FILE_PATH}, not found!")
    exit(1)


def set_timeline_value(timeline: List[Union[int, float]],
                       ind_start: int,
                       ind_end: int,
                       val: Union[float, int]):
    """
    Mark the beginning and ending of the server utilisation by the task, \
    in the timeline.

    Args:
        timeline (List[Union[int, float]]): The timeline of the cluster usage \
        by the trace dataset, in servers per second.
        ind_start (int): The starting index of the server utilisation by \
        the task.
        ind_end (int): The ending index of the server utilisation by \
        the task.
        val (Union[float, int]): The fraction of the server utilised by the \
        task.
    """
    assert 0 <= ind_start <= ind_end < len(timeline), ("The list size and "
                                                       "indices provided "
                                                       "don't match with "
                                                       "each other:"
                                                       f"\n{ind_start=}\n"
                                                       f"{ind_end=}\n"
                                                       f"{len(timeline)=}")

    timeline[ind_start] += val
    timeline[ind_end] -= val


# The units of every element in the list is: servers per second
timeline: List[Union[int, float]] = [0] * 1_000_000_000

with open(FULL_TRACE_FILE_PATH) as file_handler:
    for job in file_handler:
        raw_arrival_time, raw_task_count, _, task_durations_string = \
            job.split(maxsplit=3)

        arrival_time = float(raw_arrival_time)
        task_count = int(raw_task_count)

        for raw_task_duration in task_durations_string.split():
            task_duration = float(raw_task_duration)

            # Slice - 1
            duration_to_nearest_int_time = ceil(arrival_time) - arrival_time

            # If duration_to_nearest_int_time is 0, i.e. arrival_time is
            # an integer
            # then this operation becomes a no-op
            set_timeline_value(timeline,
                               floor(arrival_time),
                               ceil(arrival_time),
                               duration_to_nearest_int_time
                               if task_duration > duration_to_nearest_int_time
                               else task_duration
                               )

            # Update the task_duration to reflect only the remaining
            # unaccounted running time
            task_duration -= duration_to_nearest_int_time
            # If all the task_duration has been accounted for then
            # move to the next task
            if task_duration <= 0:
                continue

            # Slice - 2
            remaining_int_task_duration = floor(task_duration)

            # If remaining_int_task_duration is 0
            # then this operation becomes a no-op
            set_timeline_value(timeline,
                               ceil(arrival_time),
                               ceil(arrival_time) +
                               remaining_int_task_duration,
                               1
                               )

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
            ind_end_time: int = (ceil(arrival_time) +
                                 remaining_int_task_duration + 1)
            set_timeline_value(timeline,
                               ind_start_time,
                               ind_end_time,
                               task_duration
                               )

            assert task_duration < 1, ("Task duration was NOT less than 1, in "
                                       "the 3rd slice of the task_duration! "
                                       f"{task_duration=}")

runner: Union[float, int] = 0
for i in range(len(timeline)):
    runner += timeline[i]
    timeline[i] = runner

name_of_file, extension = NAME_OF_TRACE_FILE.split('.')
NAME_OF_OUTPUT_FILE = f"{name_of_file}_demand_{extension}"

with open(PATH_TO_OUTPUT_FOLDER / NAME_OF_OUTPUT_FILE, "w") as file_handler:
    for time_point in timeline:
        file_handler.write(f"{time_point}\n")

print("The trace demand file has been generated!")
print()
print("NOTE: \tThe topmost/1st line of the output file will represent time"
      " 0 s.")
print("\tThis means that, the 7th line in the output file represents time 6 s"
      " and so on.")
