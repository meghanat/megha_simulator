"""
This file contains the implementation of the Job class.

The Job class is used to describe a job that is input to the scheduler.
A job consists of one or more tasks. The instance of the Job class keeps
track of various paramters related to task assignment and completion.
"""

from typing import Dict, List, Optional, TYPE_CHECKING

from task import Task
from simulator_utils.values import TaskDurationDistributions

# For job optimization
import statistics

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from global_master import GM


class Job(object):
    job_count = 1  # To assign IDs
    job_start_tstamps: Dict[float, float] = {}

    def __init__(self, task_distribution: TaskDurationDistributions, line: str,
                 simulation):
        """
        Retaining below logic as-is to compare with Sparrow.

        During initialization of the object, we dephase the incoming job in
        case it has the exact submission time as another already submitted job

        Args:
            task_distribution (TaskDurationDistributions): In case we need to
            explore other task distribution methods. This is retained from the
            Sparrow code as-is.
            line (str): Line from the input trace file
            simulation (Simulation): The object of the simulation class. This
            is not currently begin used in the class's internal implementation.
        """

        job_args: List[str] = line.strip().split()
        self.start_time: float = float(job_args[0])
        self.num_tasks: int = int(job_args[1])
        self.simulation = simulation
        self.tasks: Dict[str, Task] = {}
        self.task_counter = 0
        self.completed_tasks = []
        self.gm: Optional[GM] = None
        self.completion_time = -1

        # For job optimization
        self.job_task_durations = [float(i) for i in job_args[3:]]
        self.avg_task_duration = float(job_args[2])
        self.total_job_duration = sum(self.job_task_durations)
        self.job_standard_deviation = statistics.stdev(self.job_task_durations)
        self.job_percentiles = statistics.quantiles(self.job_task_durations, n=100)

        self.job_median = self.job_percentiles[49]
        self.job_nnpercent = self.job_percentiles[-1]

        print(self.job_standard_deviation)
        print(self.job_median)
        print(self.job_nnpercent)

        # IF the job's start_time has never been seen before
        if self.start_time not in self.job_start_tstamps:
            # Add it to the dict of start time stamps
            self.job_start_tstamps[self.start_time] = self.start_time
        else:  # If the job's start_time has been seen before
            # Shift the start time of the jobs with this duplicate start time by 0.01s forward to prevent
            # a clash
            self.job_start_tstamps[self.start_time] += 0.01
            # Assign this shifted time stamp to the job start time
            self.start_time = self.job_start_tstamps[self.start_time]

        self.job_id = str(Job.job_count)
        Job.job_count += 1

        self.end_time = self.start_time

        # in case we need to explore other distr- retaining Sparrow code as-is
        if task_distribution == TaskDurationDistributions.FROM_FILE:
            self.file_task_execution_time(job_args)

    # checks if job's tasks have all been scheduled.
    def fully_scheduled(self) -> bool:

        for task_id in self.tasks:
            if not self.tasks[task_id].scheduled:
                return False
        return True

    # Job class - parse file line

    def file_task_execution_time(self, job_args):
        # Adding each of the tasks to the dict
        for task_duration in (job_args[3:]):
            # Same as eagle_simulation.py, This is done to read the floating
            # point value from the string and then convert it to an int
            duration = int(float(task_duration))
            self.task_counter += 1
            self.tasks[str(self.task_counter)] = Task(
                str(self.task_counter), self, duration)
