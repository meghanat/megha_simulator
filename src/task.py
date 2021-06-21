# This is just like a struct or Plain Old Data format
class Task(object):
    """
    The Task class is just like a struct in languages such as C. This is other wise
    know as the Plain Old Data format.

    Args:
        object (object): This is the parent object class
    """

    def __init__(self, task_id, job, duration):
        self.task_id = task_id
        self.start_time = job.start_time
        self.scheduled_time = None
        self.end_time = None
        self.job = job
        self.duration = duration
        self.node_id = None
        self.partition_id = None  # May differ from GM_id if repartitioning
        self.GM_id = None
        self.lm = None
        self.scheduled = False
