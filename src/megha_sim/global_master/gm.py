"""
File containing the implementation of the Global master.

The file contains the implementation of the Global master module of the \
modified scheduler architecture.
"""

from __future__ import annotations
import json
import random
from typing import List, Dict, TYPE_CHECKING


import simulator_utils.globals
from events import MatchFoundEvent
from simulation_logger import (SimulatorLogger, MATCHING_LOGIC_MSG,
                               CLUSTER_SATURATED_MSG,
                               MATCHING_LOGIC_REPARTITION_MSG)
from .gm_types import (PartitionKey, LMResources, ConfigFile,
                       OrganizedPartitionResources, NodeResources,
                       PartitionResources)

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from job import Job
    from local_master import LM

# Seed the random number generator
random.seed(47)


logger = SimulatorLogger(__name__).get_logger()


class GM:
    """
    Class defining the implementation of the Global Master.

    This class provides the implementation of the different \
    interfaces provided by the Global Master to the rest of \
    the scheduler architecture.
    """

    def __init__(self, simulation, GM_id: str, config: ConfigFile):
        self.GM_id = GM_id
        self.simulation = simulation
        self.RR_counter: int = 0
        self.global_view: Dict[str, LMResources] = {}
        self.job_queue: List[Job] = []
        self.jobs_scheduled: List[Job] = []

        # 3 Dictionaries
        self.internal_partitions: Dict[PartitionKey,
                                       OrganizedPartitionResources] = dict()
        self.external_partitions: Dict[PartitionKey,
                                       OrganizedPartitionResources] = dict()
        self.saturated_partitions: Dict[PartitionKey,
                                        OrganizedPartitionResources] = dict()

        # Populate internal_partitions info
        # for LM_id in config["LMs"]:
        #     self.global_view[LM_id] = config["LMs"][LM_id]

        # Populate the 3 sets with the cluster information
        for LM_id in config["LMs"]:
            for partition_id in config["LMs"][LM_id]["partitions"]:
                partition_nodes: Dict[str, NodeResources] = (config["LMs"]
                                                             [LM_id]
                                                             ["partitions"]
                                                             [partition_id]
                                                             ["nodes"])
                # create partition object
                partition_obj: OrganizedPartitionResources = \
                    OrganizedPartitionResources(lm_id=LM_id,
                                                partition_id=partition_id,
                                                free_nodes=partition_nodes,
                                                busy_nodes=dict())

                key = PartitionKey(gm_id=partition_id, lm_id=LM_id)
                if partition_id == self.GM_id:
                    self.internal_partitions[key] = partition_obj
                else:
                    self.external_partitions[key] = partition_obj

        print("GM", self.GM_id, "initialised")

    def __update_partition(self,
                           old_partition_data: OrganizedPartitionResources,
                           new_partition_data: PartitionResources) -> bool:
        # Initially assume that the partition is saturated
        is_saturated: bool = True

        for node_id in new_partition_data["nodes"]:
            is_free = (True
                       if new_partition_data["nodes"][node_id]["CPU"] == 1
                       else False)

            """A partition is actually saturated if each of its worker nodes
            are busy (i.e. `not is_free`)"""
            is_saturated = is_saturated and not is_free

            if node_id in old_partition_data["free_nodes"].keys() and \
               not is_free:
                # Move the worker node to the `busy_nodes` dictionary
                old_partition_data["busy_nodes"][node_id] =\
                    old_partition_data["free_nodes"][node_id]

                """Remove the worker node from the `free_nodes`
                dictionary"""
                del(old_partition_data["free_nodes"][node_id])
            elif node_id in old_partition_data["busy_nodes"].keys() and \
                    is_free:
                # Move the worker node to `free_nodes` dictionary
                old_partition_data["free_nodes"][node_id] =\
                    old_partition_data["busy_nodes"][node_id]

                """Remove the worker node from the `busy_nodes`
                dictionary"""
                del(old_partition_data["busy_nodes"][node_id])

        return is_saturated

    def __move_partition(self, key: PartitionKey,
                         from_partition:
                             Dict[PartitionKey, OrganizedPartitionResources],
                         to_partition:
                             Dict[PartitionKey, OrganizedPartitionResources]):
        # Preconditions
        assert from_partition.get(key) is not None
        assert to_partition.get(key) is None

        # Move the partition to the `to_partition` dictionary
        to_partition[key] = from_partition[key]

        """Remove the worker node from the `free_nodes`
        dictionary"""
        del(from_partition[key])

        # Postconditions
        assert from_partition.get(key) is None
        assert to_partition.get(key) is not None

    def update_status(self, current_time: float):
        """
        Update the global view of GM by getting partial updates from each LM.

        Args:
            current_time (float): The current time in the simulation.
        """
        for LM_id in self.simulation.lms:
            lm: LM = self.simulation.lms[LM_id]
            p_partial_status, p_tasks_completed = lm.get_status(self)
            partial_status: LMResources = json.loads(p_partial_status)
            tasks_completed = json.loads(p_tasks_completed)

            self.global_view[lm.LM_id] = partial_status  # Original

            # Iterate over all the LMs partitions
            for gm_id in partial_status["partitions"]:
                # Find the partition in the 3 sets
                key = PartitionKey(gm_id=gm_id, lm_id=LM_id)

                # Update the data in the LM
                # Check if the LM needs to be moved around
                # Check if the partition is an internal partition
                if gm_id == self.GM_id:
                    # Check if the partition is an unsaturated partition
                    if key in self.internal_partitions.keys():
                        # Update each of the workers in the partition
                        is_saturated = \
                            self.__update_partition(
                                self.internal_partitions[key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `saturated_partitions` dictionary"""
                        if is_saturated is True:
                            self.__move_partition(key,
                                                  self.internal_partitions,
                                                  self.saturated_partitions)
                    else:  # The partition is a saturated partition
                        # Update each of the workers in the partition
                        is_saturated = \
                            self.__update_partition(
                                self.saturated_partitions[key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `internal_partitions` dictionary"""
                        if is_saturated is False:
                            self.__move_partition(key,
                                                  self.saturated_partitions,
                                                  self.internal_partitions)
                else:  # The partition is an external partition
                    # Check if the partition is an unsaturated partition
                    if key in self.external_partitions.keys():
                        # Update each of the workers in the partition
                        is_saturated = \
                            self.__update_partition(
                                self.external_partitions[key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `saturated_partitions` dictionary"""
                        if is_saturated is True:
                            self.__move_partition(key,
                                                  self.external_partitions,
                                                  self.saturated_partitions)
                    else:  # The partition is a saturated partition
                        # Update each of the workers in the partition
                        is_saturated = \
                            self.__update_partition(
                                self.saturated_partitions[key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `external_partitions` dictionary"""
                        if is_saturated is False:
                            self.__move_partition(key,
                                                  self.saturated_partitions,
                                                  self.external_partitions)
            # -------------------------------------------

            # Iterate over the tasks completed and update each job's status
            for record in tasks_completed:
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
                            job.end_time = job.completion_time
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
                # Remove job from list and add to front of job_queue
                self.job_queue.insert(0, self.jobs_scheduled.pop(index))
                break

    def __get_node(self, GM_id: str, LM_id: str, node_id: str) \
            -> NodeResources:
        return self.global_view[LM_id]["partitions"][GM_id]["nodes"][node_id]

    def repartition(self, current_time: float):
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
                """Make sure that the 2 sources for `task_id` agree with each
                other"""
                assert task.task_id == task_id

                """If the task is already scheduled then, there is
                nothing to do."""
                if(job.tasks[task_id].scheduled):
                    continue

                if len(self.external_partitions) == 0:
                    """
                    There are no free worker nodes in the external partitions
                    and hence we cannot allocate the task to any worker node.
                    """
                    print(current_time, "No resources available in cluster")
                    logger.info(f"{current_time} , {CLUSTER_SATURATED_MSG} ,"
                                f" {self.GM_id}")
                    return

                # We randomly pick a non-saturated external partition
                key_external_partition = random.choice(
                    list(self.external_partitions.keys()))
                external_partition = (self.external_partitions
                                      [key_external_partition])

                # Get the LM id to verify with the LM later
                lm_id = external_partition["lm_id"]

                # We randomly pick a free worker node
                free_worker_id = random.choice(
                    list(external_partition["free_nodes"].keys()))

                free_worker_node = (external_partition["free_nodes"]
                                    [free_worker_id])
                free_worker_node["CPU"] = 0

                # Move the worker node to the `busy_nodes` dictionary
                external_partition["busy_nodes"][free_worker_id] =\
                    external_partition["free_nodes"][free_worker_id]

                """Remove the worker node from the `free_nodes`
                dictionary"""
                del(external_partition["free_nodes"][free_worker_id])

                job.tasks[task_id].scheduled = True
                if(job.fully_scheduled()):
                    self.jobs_scheduled.append(self.job_queue.pop(0))

                gm_id = external_partition["partition_id"]
                print(current_time, ", RepartitionEvent ,",
                      self.GM_id, ",",
                      gm_id,
                      ",",
                      job.job_id +
                      "_" +
                      task.task_id)
                logger.info(f"{MATCHING_LOGIC_REPARTITION_MSG} , "
                            f"{gm_id}_{lm_id}_{free_worker_id} , "
                            f"{job.job_id}_{task.task_id}")

                """If this external partition is now completely full then,
                move it to the `saturated_partitions` list"""
                if len(external_partition["free_nodes"]) == 0:
                    self.saturated_partitions[key_external_partition] = \
                        self.external_partitions[key_external_partition]

                    """Remove the external partition from the
                    `external_partitions` dictionary"""
                    del(self.external_partitions[key_external_partition])

                """May need to add processing overhead here if required"""
                self.simulation.event_queue.put(
                    (current_time,
                        MatchFoundEvent(
                            job.tasks[task_id],
                            self,
                            self.simulation.lms[lm_id],
                            free_worker_id,
                            current_time,
                            external_partition=gm_id)))

    def __repartition(self, current_time):
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
                                logger.info("Searching worker node.")

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
        # While the job_queue for the current GM is not empty
        while len(self.job_queue) > 0:
            job = self.job_queue[0]  # Get job from the head of queue
            for task_id in job.tasks:  # Go over the tasks for the job
                """Make sure that the 2 sources for `task_id` agree with each
                other"""
                assert task_id == job.tasks[task_id].task_id
                if(job.tasks[task_id].scheduled):
                    """If the task is already scheduled, then there is
                    nothing to do"""
                    continue

                if len(self.internal_partitions) == 0:
                    """
                    There are no free worker nodes in the internal partitions
                    and hence we perform the repartition operation.
                    """
                    self.repartition(current_time)
                    return

                # We randomly pick a non-saturated internal partition
                key_internal_partition = random.choice(
                    list(self.internal_partitions.keys()))
                internal_partition = (self.internal_partitions
                                      [key_internal_partition])

                # Get the LM id to verify with the LM later
                lm_id = internal_partition["lm_id"]

                # We randomly pick a free worker node
                free_worker_id = random.choice(
                    list(internal_partition["free_nodes"].keys()))

                free_worker_node = (internal_partition["free_nodes"]
                                    [free_worker_id])
                free_worker_node["CPU"] = 0

                # Move the worker node to the `busy_nodes` dictionary
                internal_partition["busy_nodes"][free_worker_id] =\
                    internal_partition["free_nodes"][free_worker_id]

                """Remove the worker node from the `free_nodes`
                dictionary"""
                del(internal_partition["free_nodes"][free_worker_id])

                job.tasks[task_id].scheduled = True
                if job.fully_scheduled():
                    self.jobs_scheduled.append(self.job_queue.pop(0))

                """If this internal partition is now completely full then,
                move it to the `saturated_partitions` dictionary"""
                if len(internal_partition["free_nodes"]) == 0:
                    self.saturated_partitions[key_internal_partition] = \
                        self.internal_partitions[key_internal_partition]

                    """Remove the internal partition from the
                    `internal_partitions` dictionary"""
                    del(self.internal_partitions[key_internal_partition])

                logger.info(f"{MATCHING_LOGIC_MSG} , "
                            f"{self.GM_id}_{lm_id}_{free_worker_id} , "
                            f"{job.job_id}_{task_id}")

                """May need to add processing overhead here if
                required"""
                self.simulation.event_queue.put(
                    (current_time, MatchFoundEvent(
                        job.tasks[task_id],
                        self,
                        self.simulation.lms[lm_id],
                        free_worker_id,
                        current_time)))

    def __schedule_tasks(self, current_time: float):
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
                        logger.info("Searching worker node.")

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
