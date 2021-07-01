"""The `Task` class is just like a struct or Plain Old Data format."""
from __future__ import annotations
from typing import Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from job import Job


class Task(object):
    """
    The Task class is just like a struct in languages such as C.

    This is otherwise known as the Plain Old Data format.

    Args:
        object (object): This is the parent object class
    """

    def __init__(self, task_id: str, job: Job, duration: int):
        """
        Initialise the instance of the Task class.

        Args:
            task_id (str): The task identifier.
            job (Job): The instance of the Job to which the Task belongs.
            duration (int): Duration of the task.
        """
        self.task_id = task_id
        self.start_time = job.start_time
        self.scheduled_time = None
        self.end_time: Optional[float] = None
        self.job = job
        self.duration = duration
        self.node_id = None
        self.partition_id = None  # May differ from GM_id if repartitioning
        self.GM_id = None
        self.lm = None
        self.scheduled = False
