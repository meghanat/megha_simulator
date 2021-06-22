import json

from simulator_utils.values import NETWORK_DELAY, InconsistencyType
from events import LaunchOnNodeEvent, InconsistencyEvent, LMUpdateEvent

class LM(object):

    def __init__(self, simulation, LM_id, partiton_size, LM_config):
        self.LM_id = LM_id
        self.partiton_size = partiton_size
        self.LM_config = LM_config
        print("LM ", LM_id, "initialised")
        self.simulation = simulation
        # we hold the key-value pairs of the list of tasks completed (value) for each GM (key)
        self.tasks_completed = {}
        for GM_id in self.simulation.gms:
            self.tasks_completed[GM_id] = []

    def get_status(self, gm):
        # """
        # One we have sent the response, the LM clears the list of tasks the LM has completed for the particular GM.

        # :param gm: The handle to the GM object
        # :type gm: GM
        # :return: List of the LM config and the tasks completed by the LM from that GM
        # :rtype: List[str, str]
        # """
        # deep copy to ensure GM's copy and LM's copy are separate
        response = [json.dumps(self.LM_config), json.dumps(
            self.tasks_completed[gm.GM_id])]
        self.tasks_completed[gm.GM_id] = []
        return response

    # LM checks if GM's request is valid
    def verify_request(self, task, gm, node_id, current_time, external_partition=None):

        # check if repartitioning
        if(external_partition is not None):
            if(self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"] == 1):
                self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"] = 0
                task.node_id = node_id
                task.partition_id = external_partition
                task.lm = self
                task.GM_id = gm.GM_id

        # network delay as the request has to be sent from the LM to the selected worker node
                self.simulation.event_queue.put(
                    (current_time+NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
                return True
            else:  # if inconsistent
                self.simulation.event_queue.put((current_time, InconsistencyEvent(
                    task, gm, InconsistencyType.EXTERNAL_INCONSISTENCY, self.simulation)))
        # internal partition
        else:
            if(self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"] == 1):
                # allot node to task
                self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"] = 0
                task.node_id = node_id
                task.partition_id = gm.GM_id
                task.GM_id = gm.GM_id
                task.lm = self
                self.simulation.event_queue.put(
                    (current_time+NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
            else:  # if inconsistent
                self.simulation.event_queue.put((current_time, InconsistencyEvent(
                    task, gm, InconsistencyType.INTERNAL_INCONSISTENCY, self.simulation)))

    def task_completed(self, task):
        # reclaim resources
        self.LM_config["partitions"][task.partition_id]["nodes"][task.node_id]["CPU"] = 1

    # Append the details of the task that was just completed to the list of tasks completed for the corresponding GM that sent it
        # note GM_id used here, not partition, in case of repartitioning
        self.tasks_completed[task.GM_id].append(
            (task.job.job_id, task.task_id))
        self.simulation.event_queue.put((task.end_time+NETWORK_DELAY, LMUpdateEvent(
            self.simulation, periodic=False, gm=self.simulation.gms[task.GM_id])))
