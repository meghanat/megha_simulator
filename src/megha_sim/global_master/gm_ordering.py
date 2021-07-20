"""
File containing the implementation of the Global master.

The file contains the implementation of the Global master module of the \
modified scheduler architecture.
"""

from __future__ import annotations
import json
import random
from typing import List, Dict, TYPE_CHECKING, Tuple
from sortedcontainers import SortedDict
from operator import neg

import simulator_utils.globals
from events import MatchFoundEvent
from simulation_logger import SimulatorLogger, MATCHING_LOGIC_MSG
from .gm_types import (PartitionKey, LMResources, ConfigFile,
                       OrganizedPartitionResources, NodeResources,
                       PartitionResources, FreeSlotsCount,
                       OrderedPartition)

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
        self.job_queue: List[Job] = []
        self.jobs_scheduled: List[Job] = []

        # Map the partition to the number of free slots in it
        self.global_view: Dict[PartitionKey, FreeSlotsCount] = {}

        # The current `POLICY` ensures that the first choice is
        # from amongst the partitions with the most number of free worker
        # slots.
        POLICY = neg
        # 3 Dictionaries
        self.internal_partitions: \
            Dict[FreeSlotsCount,
                 Dict[PartitionKey,
                      OrganizedPartitionResources]] = SortedDict(POLICY)

        # TODO: [DONE] Experiment with ordering the external partition in
        # reverse so as to not clash with the GM who owns that partition.
        # NOTE: By 'reverse' I mean: start from the partition with the
        # least number of free worker slots and then move towards the ones
        # with the most number of free worker slots.
        self.external_partitions: \
            Dict[FreeSlotsCount,
                 Dict[PartitionKey,
                      OrganizedPartitionResources]] = SortedDict()
        # SortedDict(POLICY)

        # All saturated partitions have no free worker slots, hence no extra
        # information needs to be saved nor any ordering needs to be
        # maintained.
        self.saturated_partitions: \
            Dict[PartitionKey,
                 OrganizedPartitionResources] = dict()

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

                free_slots_count = len(partition_obj["free_nodes"])
                key = PartitionKey(gm_id=partition_id, lm_id=LM_id)

                self.global_view[key] = free_slots_count

                if partition_id == self.GM_id:
                    # If this `free_slots_count` value has not been recorded
                    # before
                    if self.internal_partitions.get(free_slots_count) is None:
                        self.internal_partitions[free_slots_count] = dict()
                    self.internal_partitions[free_slots_count][key] = \
                        partition_obj
                else:
                    # If this `free_slots_count` value has not been recorded
                    # before
                    if self.external_partitions.get(free_slots_count) is None:
                        self.external_partitions[free_slots_count] = dict()
                    self.external_partitions[free_slots_count][key] = \
                        partition_obj

        print("GM", self.GM_id, "initialised")

    def __update_partition(self,
                           old_partition_data: OrganizedPartitionResources,
                           new_partition_data: PartitionResources) \
            -> Tuple[bool, int]:
        # Initially assume that the partition is saturated
        is_saturated: bool = True

        for node_id in new_partition_data["nodes"]:
            is_free = (True
                       if new_partition_data["nodes"][node_id]["CPU"] == 1
                       else False)

            """A partition is actually saturated if each of its worker nodes
            are busy (i.e. `is_free` is False)"""
            is_saturated = is_saturated and is_free is False

            # If the worker node was earlier free but now it is busy
            if node_id in old_partition_data["free_nodes"].keys() and \
               is_free is False:
                # Move the worker node to the `busy_nodes` dictionary
                old_partition_data["busy_nodes"][node_id] =\
                    old_partition_data["free_nodes"][node_id]

                """Remove the worker node from the `free_nodes`
                dictionary"""
                del(old_partition_data["free_nodes"][node_id])

            # If the worker node was earlier busy but now it is free
            elif node_id in old_partition_data["busy_nodes"].keys() and \
                    is_free is True:
                # Move the worker node to `free_nodes` dictionary
                old_partition_data["free_nodes"][node_id] =\
                    old_partition_data["busy_nodes"][node_id]

                """Remove the worker node from the `busy_nodes`
                dictionary"""
                del(old_partition_data["busy_nodes"][node_id])

        return is_saturated, len(old_partition_data["free_nodes"])

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

    def __relocate_partition(self,
                             partition: Dict[FreeSlotsCount,
                                             Dict[PartitionKey,
                                                  OrganizedPartitionResources
                                                  ]
                                             ],
                             old_free_slots_count: FreeSlotsCount,
                             new_free_slots_count: FreeSlotsCount,
                             partition_key: PartitionKey):
        """Update the position of the node in the dictionary."""
        if partition.get(new_free_slots_count) is None:
            partition[new_free_slots_count] = dict()

        partition[new_free_slots_count][partition_key] = \
            partition[old_free_slots_count][partition_key]

        """Remove the partition from its previous
        position in the `partitions` dictionary"""
        del(partition[old_free_slots_count][partition_key])

        """Check if there are any remaining partitions with
        `old_free_slots_count` number of free slots"""
        if len(partition[old_free_slots_count].keys()) == 0:
            del(partition[old_free_slots_count])

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

            # NOTE: Iterate over all the LMs partitions
            for gm_id in partial_status["partitions"]:
                # NOTE: Find the partition in the 3 sets
                key = PartitionKey(gm_id=gm_id, lm_id=LM_id)

                # NOTE: Update the data in the LM
                # NOTE: Check if the LM needs to be moved around
                # Check if the partition is an internal partition
                if gm_id == self.GM_id:
                    # Check if the partition is an unsaturated partition
                    if self.global_view[key] != 0:
                        free_slot_key = self.global_view[key]
                        # Update each of the workers in the partition
                        is_saturated, free_slots_count = \
                            self.__update_partition(
                                self.internal_partitions[free_slot_key][key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `saturated_partitions` dictionary"""
                        if is_saturated is True:
                            self.__move_partition(key,
                                                  (self.internal_partitions
                                                   [free_slot_key]),
                                                  self.saturated_partitions)
                            if len(self.internal_partitions[free_slot_key])\
                               == 0:
                                del(self.internal_partitions[free_slot_key])
                        else:
                            """The partition is not saturated but may need
                            to be relocated, if its free slots count has
                            changed.
                            """
                            if free_slots_count != self.global_view[key]:
                                self.__relocate_partition(
                                    self.internal_partitions,
                                    self.global_view[key],
                                    free_slots_count,
                                    key
                                )
                        # Update the map
                        self.global_view[key] = free_slots_count
                    else:  # The partition is a saturated partition
                        # Update each of the workers in the partition
                        is_saturated, free_slots_count = \
                            self.__update_partition(
                                self.saturated_partitions[key],
                                partial_status["partitions"][gm_id]
                            )
                        # Update the map
                        self.global_view[key] = free_slots_count
                        """Check if the partition needs to be moved to the
                        `internal_partitions` dictionary"""
                        if is_saturated is False:
                            if self.internal_partitions.get(free_slots_count)\
                               is None:
                                self.internal_partitions[free_slots_count]\
                                    = dict()
                            self.__move_partition(key,
                                                  self.saturated_partitions,
                                                  (self.internal_partitions
                                                   [free_slots_count]))
                else:  # The partition is an external partition
                    # Check if the partition is an unsaturated partition
                    if self.global_view[key] != 0:
                        free_slot_key = self.global_view[key]

                        # Update each of the workers in the partition
                        is_saturated, free_slots_count = \
                            self.__update_partition(
                                self.external_partitions[free_slot_key][key],
                                partial_status["partitions"][gm_id]
                            )

                        """Check if the partition needs to be moved to the
                        `saturated_partitions` dictionary"""
                        if is_saturated is True:
                            self.__move_partition(key,
                                                  (self.external_partitions
                                                   [free_slot_key]),
                                                  self.saturated_partitions)
                            if len(self.external_partitions[free_slot_key])\
                               == 0:
                                del(self.external_partitions[free_slot_key])
                        else:
                            """The partition is not saturated but may need
                            to be relocated, if its free slots count has
                            changed.
                            """
                            if free_slots_count != self.global_view[key]:
                                self.__relocate_partition(
                                    self.external_partitions,
                                    self.global_view[key],
                                    free_slots_count,
                                    key
                                )
                        # Update the map
                        self.global_view[key] = free_slots_count
                    else:  # The partition is a saturated partition
                        # Update each of the workers in the partition
                        is_saturated, free_slots_count = \
                            self.__update_partition(
                                self.saturated_partitions[key],
                                partial_status["partitions"][gm_id]
                            )

                        # Update the map
                        self.global_view[key] = free_slots_count
                        """Check if the partition needs to be moved to the
                        `external_partitions` dictionary"""
                        if is_saturated is False:
                            if self.external_partitions.get(free_slots_count)\
                               is None:
                                self.external_partitions[free_slots_count]\
                                    = dict()
                            self.__move_partition(key,
                                                  self.saturated_partitions,
                                                  (self.external_partitions
                                                   [free_slots_count]))
            # -------------------------------------------

            # TODO: Check comment "Through Job object delete task"

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
                            assert task.end_time is not None, \
                                "Task end time not recorded!"
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
                    return

                # NOTE: This is not a type error
                gm_id, lm_id, free_worker_id = self.__get_worker_node(
                    self.external_partitions)

                job.tasks[task_id].scheduled = True
                if(job.fully_scheduled()):
                    self.jobs_scheduled.append(self.job_queue.pop(0))

                print(current_time, ", RepartitionEvent ,",
                      self.GM_id, ",",
                      gm_id,
                      ",",
                      job.job_id +
                      "_" +
                      task.task_id)
                logger.info(f"{MATCHING_LOGIC_MSG} , "
                            f"{gm_id}_{lm_id}_{free_worker_id} , "
                            f"{job.job_id}_{task.task_id}")

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

    def __get_worker_node(self, partition: OrderedPartition) \
            -> Tuple[str, str, str]:
        """
        Select the worker node with the highest chances of actually begin free.

        Select the worker node from the partition with most number of \
        free worker slots. This is aimed to help overcome (to a certain \
        degree) the inaccuracies in the stale view of the cluster that the \
        GM has.
        """
        # We pick the dictionary of partitions with the most number of
        # free worker slots.
        free_slot_key, partition_dict = partition.peekitem(index=0)

        # We randomly pick a non-saturated internal partition
        key_internal_partition = random.choice(list(partition_dict.keys()))
        internal_partition = partition_dict[key_internal_partition]

        # Get the GM id to verify with the LM later
        gm_id = internal_partition["partition_id"]

        # Get the LM id to verify with the LM later
        lm_id = internal_partition["lm_id"]

        # We randomly pick a free worker node
        free_worker_id = random.choice(
            list(internal_partition["free_nodes"].keys()))

        free_worker_node = (internal_partition["free_nodes"]
                            [free_worker_id])
        free_worker_node["CPU"] = 0

        assert internal_partition["busy_nodes"].get(free_worker_id) is None,\
            "Worker ID is in 'busy_nodes' before insertion, itself!"

        # Move the worker node to the `busy_nodes` dictionary
        internal_partition["busy_nodes"][free_worker_id] =\
            internal_partition["free_nodes"][free_worker_id]

        # Remove the worker node from the `free_nodes` dictionary
        del(internal_partition["free_nodes"][free_worker_id])

        # Update the mapping of partition to the number of free slots in it
        self.global_view[key_internal_partition] -= 1
        free_slots_count = self.global_view[key_internal_partition]

        assert free_slots_count >= 0, "'free_slots_count' became negative!"

        # If this internal partition is now completely full then,
        # move it to the `saturated_partitions` dictionary
        if free_slots_count == 0:
            self.saturated_partitions[key_internal_partition] = \
                partition[free_slot_key][key_internal_partition]

            # Remove the internal partition from the
            # `internal_partitions` dictionary
            del(partition[free_slot_key][key_internal_partition])
        else:
            # Update the position of the node in the dictionary.
            if partition.get(free_slots_count) is None:
                partition[free_slots_count] = dict()

            partition[free_slots_count][key_internal_partition] = \
                partition[free_slot_key][key_internal_partition]

            # Remove the internal partition from its previous
            # position in the `internal_partitions` dictionary
            del(partition[free_slot_key][key_internal_partition])

        # Check if there are any remaining partitions with `free_slot_key`
        # number of free slots
        if len(partition[free_slot_key]) == 0:
            del(partition[free_slot_key])

        return gm_id, lm_id, free_worker_id

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

                # NOTE: This is not a type error
                _, lm_id, free_worker_id = self\
                    .__get_worker_node(self.internal_partitions)

                job.tasks[task_id].scheduled = True
                if job.fully_scheduled():
                    self.jobs_scheduled.append(self.job_queue.pop(0))

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

    def queue_job(self, job, current_time):
        print(current_time, ",", "JobArrivalEvent",
              ",", job.job_id, ",", self.GM_id)
        job.gm = self
        self.job_queue.append(job)
        if(len(self.job_queue) == 1):  # first job
            self.schedule_tasks(current_time)
