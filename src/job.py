from typing import List, Optional, TYPE_CHECKING

from task import Task
from globals import job_start_tstamps
from values import TaskDurationDistributions

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from gm import GM


class Job(object):
    job_count = 1  # to assign ids

    def __init__(self, task_distribution, line, simulation):
        global job_start_tstamps

        job_args: List[str] = line.strip().split()
        self.start_time: float = float(job_args[0])
        self.num_tasks: int = int(job_args[1])
        self.simulation = simulation
        self.tasks = {}
        self.task_counter = 0
        self.completed_tasks = []
        self.gm: Optional[GM] = None
        self.completion_time = -1

        # retaining below logic as-is to compare with Sparrow.
        # dephase the incoming job in case it has the exact submission time as another already submitted job
        if self.start_time not in job_start_tstamps:  # IF the job's start_time has never been seen before
            # Add it to the dict of start time stamps
            job_start_tstamps[self.start_time] = self.start_time
        else:  # If the job's start_time has been seen before
            # Shift the start time of the jobs with this duplicate start time by 0.01s forward to prevent a clash
            job_start_tstamps[self.start_time] += 0.01
            # Assign this shifted time stamp to the job start time
            self.start_time = job_start_tstamps[self.start_time]

        self.job_id = str(Job.job_count)
        Job.job_count += 1

        self.end_time = self.start_time

        # in case we need to explore other distr- retaining Sparrow code as-is
        if task_distribution == TaskDurationDistributions.FROM_FILE:
            self.file_task_execution_time(job_args)

    # checks if job's tasks have all been scheduled.
    def fully_scheduled(self):

        for task_id in self.tasks:
            if not self.tasks[task_id].scheduled:
                return False
        return True

    # Job class - parse file line

    def file_task_execution_time(self, job_args):
        # Adding each of the tasks to the dict
        for task_duration in (job_args[3:]):
            # Same as eagle_simulation.py, This is done to read the floating point value from the string and then convert it to an int
            duration = int(float(task_duration))
            self.task_counter += 1
            self.tasks[str(self.task_counter)] = Task(
                str(self.task_counter), self, duration)
