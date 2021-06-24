import pickle
import queue
import json


from local_master import LM
from global_master import GM
from job import Job
from simulator_utils.values import TaskDurationDistributions
from events import JobArrival, LMUpdateEvent


class Simulation(object):
    def __init__(self, workload, config, NUM_GMS, NUM_LMS, PARTITION_SIZE, cpu, memory, storage):

        # Each localmaster has one partition per global master so the total number of partitions in the cluster are:
        # NUM_GMS * NUM_LMS
        # Given the number of worker nodes per partition is PARTITION_SIZE
        # so the total_nodes are NUM_GMS*NUM_LMS*PARTITION_SIZE
        self.total_nodes = NUM_GMS*NUM_LMS*PARTITION_SIZE
        self.NUM_GMS = NUM_GMS
        self.NUM_LMS = NUM_LMS
        self.config = json.load(open(config))
        self.WORKLOAD_FILE = workload

        self.jobs = {}
        self.event_queue = queue.PriorityQueue()

        # initialise GMs
        self.gms = {}
        counter = 1
        while len(self.gms) < self.NUM_GMS:
            self.gms[str(counter)] = GM(self, str(counter), pickle.loads(
                pickle.dumps(self.config)))  # create deep copy
            counter += 1

        # initialise LMs
        self.lms = {}
        counter = 1

        while len(self.lms) < self.NUM_LMS:
            self.lms[str(counter)] = LM(self, str(counter), PARTITION_SIZE, pickle.loads(
                pickle.dumps(self.config["LMs"][str(counter)])))  # create deep copy
            counter += 1

        self.shared_cluster_status = {}

        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False
        print("Simulation instantiated")

 # Simulation class
    def run(self):
        last_time = 0

        self.jobs_file = open(self.WORKLOAD_FILE, 'r')

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        line = self.jobs_file.readline()  # first job
        new_job = Job(self.task_distribution, line, self)
        self.event_queue.put((float(line.split()[0]), JobArrival(
            self, self.task_distribution, new_job, self.jobs_file)))
        # starting the periodic LM updates
        self.event_queue.put((float(line.split()[0]), LMUpdateEvent(self)))
        self.jobs_scheduled = 1

        # start processing events
        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            if(new_events is None):
                continue
            for new_event in new_events:
                if(new_event is None):
                    continue
                self.event_queue.put(new_event)

        print("Simulation ending, no more events")
        self.jobs_file.close()
