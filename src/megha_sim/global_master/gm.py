from __future__ import annotations

import json
from typing import List, Dict, TYPE_CHECKING, TypedDict

import simulator_utils.globals
from events import MatchFoundEvent
from simulation_logger import SimulatorLogger


# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from job import Job


class NodeResources(TypedDict):
    CPU: int
    RAM: int
    Disk: int
    constraints: List[int]


class PartitionResources(TypedDict):
    partition_id: str
    nodes: Dict[str, NodeResources]


class LMResources(TypedDict):
    LM_id: str
    partitions: Dict[str, PartitionResources]


logger = SimulatorLogger(__name__).get_logger()


class GM(object):
    def __init__(self, simulation, GM_id: str, config):
        self.GM_id = GM_id
        self.simulation = simulation
        self.RR_counter: int = 0
        self.global_view: Dict[str, LMResources] = {}
        self.job_queue: List[Job] = []
        self.jobs_scheduled: List[Job] = []

        # Populate internal_partitions info
        for LM_id in config["LMs"]:
            self.global_view[LM_id] = config["LMs"][LM_id]

        print("GM", self.GM_id, "initialised")

    def update_status(self, current_time: float):
        """
        Updates global view of GM by getting partial updates from each LM.

        Args:
            current_time (float): The current time in the simulation.
        """
        for LM_id in self.simulation.lms:
            lm = self.simulation.lms[LM_id]
            p_partial_status, p_tasks_completed = lm.get_status(self)
            partial_status = json.loads(p_partial_status)
            tasks_completed = json.loads(p_tasks_completed)
            self.global_view[lm.LM_id] = partial_status
            # Through Job object delete task
            for record in tasks_completed:  # Iterate over the tasks completed and update each job's status
                job_id = record[0]
                task_id = record[1]

                job_unscheduled = False

                # if not all tasks in the job have been scheduled
                for index in range(0, len(self.job_queue)):
                    job = self.job_queue[index]
                    if job.job_id == job_id:
                        job_unscheduled = True
                        task = job.tasks[task_id]
                        job.completed_tasks.append(task)
                        break

                if(job_unscheduled):
                    continue

                # If all tasks in the job have been scheduled already
                for index in range(0, len(self.jobs_scheduled)):
                    job = self.jobs_scheduled[index]

                    if job.job_id == job_id:
                        task = job.tasks[task_id]
                        job.completed_tasks.append(task)
                        if len(
                                job.tasks) == len(
                                job.completed_tasks):  # no more tasks left
                            # NOTE:job completion time = end time of last task
                            # === max of the task duration for a job
                            job.completion_time = task.end_time
                            print(job.completion_time)
                            simulator_utils.globals.jobs_completed.append(job)
                            self.jobs_scheduled.remove(job)
                        break

        self.schedule_tasks(current_time)

    def unschedule_job(self, unverified_job: Job):
        """
        Job is inserted back into the job_queue of the GM.

        The job is inserted back into the job_queue of the GM from the \
        job_scheduled queue of the GM.

        Args:
            unverified_job (Job): The job that needs to be moved, as it was \
                assigned on a worker node not actually available at that time
        """
        for index in range(0, len(self.jobs_scheduled)):
            if unverified_job.job_id == self.jobs_scheduled[index].job_id:
                # remove job from list and add to front of job_queue
                self.job_queue.insert(0, self.jobs_scheduled.pop(index))
                break

    def __get_node(self, GM_id: str, LM_id: str, node_id: str) \
            -> NodeResources:
        return self.global_view[LM_id]["partitions"][GM_id]["nodes"][node_id]

    def repartition(self, current_time):
        """
        Search the external partitions for a free worker node.

        Args:
            current_time (float): The current time in the simulation.
        """
        # While the job_queue for the current GM is not empty
        while len(self.job_queue) > 0:
            job = self.job_queue[0]  # Get the Job from the head of the queue

            # print("Scheduling Tasks from Job: ",job.job_id)
            for task_id in job.tasks:  # Go over the tasks for the job
                task = job.tasks[task_id]
                """If the task is already scheduled then, there is
                nothing to do."""
                if(job.tasks[task_id].scheduled):
                    continue

                matchfound: bool = False
                # print("Scheduling Task:", task_id)

                # Search in the GM's external partitions:
                for GM_id in self.simulation.gms:
                    if GM_id == self.GM_id:
                        """Skip the partitions of the GM searching for a free
                        worker node in the external partitions."""
                        ...
                    else:
                        """We search each of the other GM's internal
                        partitions in each LM, for a free worker node."""
                        for _ in range(self.simulation.NUM_LMS):
                            # Which LM? searching the LMs in RR fashion
                            LM_id = str(self.RR_counter %
                                        self.simulation.NUM_LMS + 1)
                            self.RR_counter += 1

                            """Search in external partitions, hence iterating
                            over a dict."""
                            for node_id in (self.global_view[LM_id]
                                            ["partitions"]
                                            [GM_id]["nodes"]):
                                node = self.__get_node(GM_id, LM_id, node_id)
                                logger.info(f"Checking worker node , "
                                            f"{GM_id}_{LM_id}_{node_id} , "
                                            f"{job.job_id}_{task_id}")

                                # The worker node is unoccupied
                                if node["CPU"] == 1:
                                    node["CPU"] = 0
                                    job.tasks[task_id].scheduled = True
                                    if(job.fully_scheduled()):
                                        self.jobs_scheduled.append(
                                            self.job_queue.pop(0))
                                    print(
                                        current_time,
                                        "RepartitionEvent",
                                        self.GM_id,
                                        ",",
                                        GM_id,
                                        ",",
                                        job.job_id +
                                        "_" +
                                        task.task_id)
                                    # may need to add processing overhead here
                                    # if required
                                    self.simulation.event_queue.put(
                                        (current_time,
                                            MatchFoundEvent(
                                                job.tasks[task_id],
                                                self,
                                                self.simulation.lms[LM_id],
                                                node_id,
                                                current_time,
                                                external_partition=GM_id)))

                                    """We have found a free worker node and
                                    hence, we do not need to search any
                                    further."""
                                    matchfound = True
                                    break

                            if matchfound is True:
                                """If we found a free worker node then stop
                                searching any more LMs."""
                                break

                        if matchfound is True:
                            """If we found a free worker node then stop
                            searching any more GMs."""
                            break

                if matchfound is True:
                    """If this task was successfully placed then, move on to
                    the next task."""
                    ...
                else:
                    print(current_time, "No resources available in cluster")
                    return

    def schedule_tasks(self, current_time: float):
        """
        Search the internal partitions of the GM to find a free worker node.

        Args:
            current_time (float): The current time in the simulation.
        """
        while len(self.job_queue) > 0:
            # While the job_queue for the current GM is not empty
            job = self.job_queue[0]  # Get job from the head of queue
            for task_id in job.tasks:  # Go over the tasks for the job
                if(job.tasks[task_id].scheduled):
                    # If the task is already scheduled, then there is
                    # nothing to do
                    continue

                matchfound: bool = False

                # We search each of the GM's internal partitions in each LM
                for _ in range(self.simulation.NUM_LMS):
                    # Which LM? searching the LMs in RR fashion
                    LM_id = str(self.RR_counter % self.simulation.NUM_LMS + 1)
                    self.RR_counter += 1

                    # Searching in the internal partition iterating over a dict
                    for node_id in (self.global_view[LM_id]["partitions"]
                                    [self.GM_id]["nodes"]):
                        node = self.__get_node(self.GM_id, LM_id, node_id)
                        logger.info(f"Checking worker node , "
                                    f"{self.GM_id}_{LM_id}_{node_id} , "
                                    f"{job.job_id}_{task_id}")

                        if node["CPU"] == 1:  # If the Node is available
                            node["CPU"] = 0
                            job.tasks[task_id].scheduled = True
                            if job.fully_scheduled():
                                self.jobs_scheduled.append(self.job_queue
                                                           .pop(0))
                            # May need to add processing overhead here if
                            # required
                            self.simulation.event_queue.put(
                                (current_time,
                                 MatchFoundEvent(
                                     job.tasks[task_id],
                                     self,
                                     self.simulation.lms[LM_id],
                                     node_id,
                                     current_time)))
                            matchfound = True
                            break

                    if matchfound is True:
                        break

                if matchfound:
                    # If this task was successfully placed then, move
                    # on to the next task
                    continue
                else:
                    # repartition
                    self.repartition(current_time)
                    return

    def queue_job(self, job, current_time):
        print(current_time, ",", "JobArrivalEvent",
              ",", job.job_id, ",", self.GM_id)
        job.gm = self
        self.job_queue.append(job)
        if(len(self.job_queue) == 1):  # first job
            self.schedule_tasks(current_time)
