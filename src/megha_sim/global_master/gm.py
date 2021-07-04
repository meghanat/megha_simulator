import json
from typing import List, Dict, TYPE_CHECKING, TypedDict

from events import MatchFoundEvent
import simulator_utils.globals


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


class GM(object):
    def __init__(self, simulation, GM_id: str, config):
        self.GM_id = GM_id
        self.simulation = simulation
        self.RR_counter: int = 0
        self.global_view: Dict[str, LMResources] = {}
        self.job_queue: List[Job] = []
        self.jobs_scheduled: List[Job] = []

        # populate internal_partitions info
        for LM_id in config["LMs"]:
            self.global_view[LM_id] = config["LMs"][LM_id]

        print("GM", self.GM_id, "initialised")

    # updates global view of GM by getting partial updates from each LM
    def update_status(self, current_time):
        # global jobs_completed

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

                job_unscheduled=False

                # if not all tasks in the job have been scheduled
                for index in range(0, len(self.job_queue)):
                    job = self.job_queue[index]
                    if job.job_id == job_id:
                        job_unscheduled=True
                        task = job.tasks[task_id]
                        job.completed_tasks.append(task)
                        break

                if(job_unscheduled):
                    continue

                #if all tasks in the job have been scheduled already
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

    def unschedule_job(self, unverified_job):
        # """
        # The job is inserted back into the job_queue of the GM from the
        # job_scheduled queue of the GM

        # :param unverified_job: The job that needs to be moved, as it was assigned on a worker node
        #  not actually available at that time
        # :type unverified_job: Job
        # """
        for index in range(0, len(self.jobs_scheduled)):
            if unverified_job.job_id == self.jobs_scheduled[index].job_id:
                # remove job from list and add to front of job_queue
                self.job_queue.insert(0, self.jobs_scheduled.pop(index))
                break

    # searches the external partitions
    def repartition(self, current_time):
        # search in external partitions:
        for GM_id in self.simulation.gms:
            if GM_id == self.GM_id:  # Skip the partition of the GM searching for a worker node in a external partition
                continue
            else:
                # While the job_queue for the current GM is not empty
                while len(self.job_queue) > 0:
                    job = self.job_queue[0]  # get job from head of queue
                    # print("Scheduling Tasks from Job: ",job.job_id)

                    for task_id in job.tasks:  # Go over the tasks for the job
                        task = job.tasks[task_id]
                        # If the task is already scheduled then, there is
                        # nothing to do
                        if(job.tasks[task_id].scheduled):
                            continue
                        matchfound = False
                        # print("Scheduling Task:",task_id)
                        # which LM? searching the LMs in RR fashion
                        LM_id = str(self.RR_counter %
                                    self.simulation.NUM_LMS + 1)
                        self.RR_counter += 1
                        # search in external partitions
                        # iterating over a dict
                        for node_id in (self.global_view[LM_id]["partitions"]
                                        [GM_id]["nodes"]):
                            node = self.global_view[LM_id]["partitions"][GM_id]["nodes"][node_id]
                            if node["CPU"] == 1:  # node unoccupied
                                # print("Match found in internal partitions")
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
                                # may need to add processing overhead here if
                                # required
                                self.simulation.event_queue.put(
                                    (current_time,
                                     MatchFoundEvent(
                                         job.tasks[task_id],
                                         self,
                                         self.simulation.lms[LM_id],
                                         node_id,
                                         current_time,
                                         external_partition=GM_id)))
                                matchfound = True
                                break
                        if matchfound:  # If this task was successfully placed then, move on to the next task
                            continue
                        else:
                            print(current_time,
                                  "No resources available in cluster")
                            return
                # print("Exit scheduling loop for now")

    # search internal partitions
    def schedule_tasks(self, current_time):

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
                        node = (self.global_view[LM_id]["partitions"]
                                [self.GM_id]["nodes"][node_id])
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
