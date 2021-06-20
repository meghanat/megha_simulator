from typing import List, Optional, Tuple, Union, Final, Literal
from enum import Enum, unique
import sys
import time
import logging
import math
import random
import queue
import collections
import json
import copy
import pickle

#####################################################################################################################
@unique
class InconsistencyType(Enum):
	INTERNAL_INCONSISTENCY = Literal[0]
	EXTERNAL_INCONSISTENCY = Literal[1]

#####################################################################################################################
LM_HEARTBEAT_INTERVAL=30

class TaskDurationDistributions:
    CONSTANT : Final[int]
    MEAN : Final[int]
    FROM_FILE : Final[int]
    CONSTANT, MEAN, FROM_FILE  = range(3)
class EstimationErrorDistribution:
    CONSTANT : Final[int]
    RANDOM : Final[int]
    MEAN : Final[int]
    CONSTANT, RANDOM, MEAN = range(3)

#####################################################################################################################
#####################################################################################################################

# Event class is an Abstract class
class Event(object):
	"""
	This is the abstract Event object class.

	Args:
		object (Object): Parent object class.
	"""
	def __init__(self):
		raise NotImplementedError("Event is an abstract class and cannot be instantiated directly")

	def __lt__(self, other) -> bool:
		return True

	def run(self, current_time):
		""" Returns any events that should be added to the queue. """
		raise NotImplementedError("The run() method must be implemented by each class subclassing Event")

#####################################################################################################################
#####################################################################################################################

#created when a task completes
class TaskEndEvent(Event):
	"""
	This event is created when a task has completed. The `end_time` is set as the `current_time` of running the event.

	Args:
		Event (Event): Parent Event class.
	"""
	def __init__(self, task):
		"""
		Initialise the TaskEndEvent class with the task object.

		Args:
			task (Task): The task object representing the task which has completed.
		"""
		self.task : Task = task
	def __lt__(self, other : Event) -> bool:
		"""
		Compare the TaskEndEvent Object with another object of Event class.

		Args:
			other (Event): The object to compare with.

		Returns:
			bool: The TaskEndEvent object is always lesser than the object it is compared with.
		"""
		return True

	def run(self, current_time):
		print(current_time,",","TaskEndEvent",",",self.task.job.job_id+"_"+self.task.task_id+"___",self.task.duration)
		self.task.end_time=current_time
		if self.task.lm is not None:
			self.task.lm.task_completed(self.task)

#####################################################################################################################
#####################################################################################################################
#created after LM verifies GM request
class LaunchOnNodeEvent(Event):
	"""
	This event is created when a task is sent to a particular worker node in a particular partition, selected by the global master and verified by the local master.

	Args:
		Event (Event): Parent Event class.
	"""

	def __init__(self,task,simulation):
		self.task=task
		self.simulation=simulation

	def run(self, current_time):
		print(current_time,",","LaunchOnNodeEvent",",",self.task.job.job_id+"_"+self.task.task_id,",",self.task.partition_id+"_"+self.task.node_id)
		self.simulation.event_queue.put((current_time+self.task.duration+NETWORK_DELAY,TaskEndEvent(self.task)))#launching requires network transfer


#####################################################################################################################
#####################################################################################################################

#if GM has outdated info, LM creates this event
class InconsistencyEvent(Event):
	def __init__(self, task, gm, type, simulation):
		self.task=task
		self.gm : GM = gm
		self.type : Final = type
		self.simulation=simulation

	def run(self, current_time):
		if(self.type==InconsistencyType.INTERNAL_INCONSISTENCY):#internal inconsistency -> failed to place task on external partition
			print(current_time,",","InternalInconsistencyEvent")
		else:# external inconsistency  -> failed to place task on internal partition
			print(current_time,",","ExternalInconsistencyEvent")
		self.task.scheduled=False

		#if job already moved to jobs_scheduled queue, need to remove and add to front of queue
		self.gm.unschedule_job(self.task.job)
		self.simulation.event_queue.put((current_time,LMUpdateEvent(self.simulation,periodic=False,gm=self.gm)))

		#***********************************************************************#
		#  NEED TO CHECK THIS AND SEE IF IT CAN BE MODIFIED SUCH				#
		# THAT THE UNSCHEDULE OPERATION IS CALLED JUST BEFORE CONFIG IS UPDATED #
		#  THIS IS TO ENSURE THE DELAY DUE TO INCONSISTENCY IS REFLECTED		#
		#***********************************************************************#


#####################################################################################################################
#####################################################################################################################
#created when GM finds a match in the external or internal partition
class MatchFoundEvent(Event):
	def __init__(self,task, gm,lm,node_id,current_time,external_partition=None):
		self.task=task
		self.gm =gm
		self.lm=lm
		self.node_id=node_id
		self.current_time=current_time
		self.external_partition=external_partition

	def run(self, current_time):
		#add network delay to LM, similar to sparrow: 
		print(current_time,",","MatchFoundEvent",",",self.task.job.job_id+"_"+self.task.task_id,",",self.gm.GM_id+"_"+str(self.node_id),"_",self.lm.LM_id)
		self.lm.verify_request(self.task,self.gm,self.node_id,current_time+NETWORK_DELAY,external_partition=self.external_partition)

#####################################################################################################################
#####################################################################################################################
#created periodically or when LM needs to piggyback update on response
class  LMUpdateEvent(Event):
	
	def __init__(self,simulation,periodic=True,gm=None):
		self.simulation=simulation
		self.periodic : bool = periodic
		self.gm : Optional[GM] = gm

		if self.periodic is True:
			assert self.gm is None, "LMUpdateEvent.__init__: Periodic is set to true so self.gm must be None!"
		elif self.periodic is False:
			assert self.gm is not None, "LMUpdateEvent.__init__: Periodic is set to false so self.gm must not be None!"

	def run(self,current_time):
		print(current_time,",","LMUpdateEvent",",",self.periodic)
		
		#update only that GM which is inconsistent or if the GM's task has completed
		if not self.periodic:
			self.gm.update_status(current_time+NETWORK_DELAY)

		if self.periodic and not self.simulation.event_queue.empty():
			for GM_id in self.simulation.gms:
				self.simulation.gms[GM_id].update_status(current_time+NETWORK_DELAY)
			self.simulation.event_queue.put((current_time + LM_HEARTBEAT_INTERVAL+NETWORK_DELAY,self))#add the next heartbeat, network delay added because intuitively we do not include it in the LM_HEARTBEAT INTERVAL
		
#####################################################################################################################
#####################################################################################################################
#created for each job
class JobArrival(Event):

	gm_counter : int = 0

	def __init__(self, simulation, task_distribution, job, jobs_file):
		self.simulation = simulation
		self.task_distribution = task_distribution
		self.job = job
		self.jobs_file = jobs_file  # Jobs file (input trace file) handler

	def __lt__(self, other) -> bool:
		return True


	def run(self, current_time):
		new_events : List[Tuple[float, Event]] = []
		#needs to be assigned to a GM - RR
		JobArrival.gm_counter=(JobArrival.gm_counter)%self.simulation.NUM_GMS+1
        # assigned_GM --> Handle to the global master object
		assigned_GM : GM =self.simulation.gms[str(JobArrival.gm_counter)]
		#GM needs to add job to its queue
		assigned_GM.queue_job(self.job, current_time)


		# Creating a new Job Arrival event for the next job in the trace
		line = self.jobs_file.readline()
		if (len(line)==0):
			self.simulation.scheduled_last_job = True
		else:
			self.job = Job(self.task_distribution,line,self.simulation)
			new_events.append((self.job.start_time, self))
			self.simulation.jobs_scheduled += 1
		return new_events


#####################################################################################################################
#####################################################################################################################

# This is just like a struct or Plain Old Data format
class Task(object):

	def __init__(self,task_id,job,duration):
		self.task_id=task_id
		self.start_time=job.start_time
		self.scheduled_time=None
		self.end_time=None
		self.job=job
		self.duration=duration
		self.node_id=None
		self.partition_id=None #may differ from GM_id if repartitioning
		self.GM_id=None
		self.lm=None
		self.scheduled=False

#####################################################################################################################
#####################################################################################################################

class Job(object):
	job_count = 1 # to assign ids

	def __init__(self, task_distribution, line,simulation):
		global job_start_tstamps
		
		job_args : List[str]= line.strip().split()
		self.start_time : float = float(job_args[0])
		self.num_tasks : int= int(job_args[1])
		self.simulation=simulation
		self.tasks={}
		self.task_counter=0
		self.completed_tasks=[]
		self.gm : Optional[GM] = None
		
		#retaining below logic as-is to compare with Sparrow.
		#dephase the incoming job in case it has the exact submission time as another already submitted job
		if self.start_time not in job_start_tstamps:  # IF the job's start_time has never been seen before
			job_start_tstamps[self.start_time] = self.start_time  # Add it to the dict of start time stamps
		else:  # If the job's start_time has been seen before
			job_start_tstamps[self.start_time] += 0.01  # Shift the start time of the jobs with this duplicate start time by 0.01s forward to prevent a clash
			self.start_time = job_start_tstamps[self.start_time]  # Assign this shifted time stamp to the job start time
		
		self.job_id = str(Job.job_count)
		Job.job_count += 1
		
		self.end_time = self.start_time
		
		#in case we need to explore other distr- retaining Sparrow code as-is
		if	task_distribution == TaskDurationDistributions.FROM_FILE: 
			self.file_task_execution_time(job_args)
		
	#checks if job's tasks have all been scheduled.	
	def fully_scheduled(self):
		
		for task_id in self.tasks:
			if not self.tasks[task_id].scheduled:
				return False
		return True


	#Job class - parse file line
	def file_task_execution_time(self, job_args):
		for task_duration in (job_args[3:]):  # Adding each of the tasks to the dict
			duration=int(float(task_duration))	 # Same as eagle_simulation.py, This is done to read the floating point value from the string and then convert it to an int
			self.task_counter+=1
			self.tasks[str(self.task_counter)]=Task(str(self.task_counter),self,duration)

#####################################################################################################################
#####################################################################################################################

class LM(object):

	def __init__(self,simulation,LM_id,partiton_size,LM_config):
		self.LM_id=LM_id
		self.partiton_size=partiton_size
		self.LM_config=LM_config
		print("LM ",LM_id,"initialised")
		self.simulation=simulation
		self.tasks_completed={}  # we hold the key-value pairs of the list of tasks completed (value) for each GM (key)
		for GM_id in self.simulation.gms:
			self.tasks_completed[GM_id]=[]

	def get_status(self,gm):
        # """
        # One we have sent the response, the LM clears the list of tasks the LM has completed for the particular GM.

        # :param gm: The handle to the GM object
        # :type gm: GM
        # :return: List of the LM config and the tasks completed by the LM from that GM
        # :rtype: List[str, str]
        # """
		#deep copy to ensure GM's copy and LM's copy are separate
		response=[ json.dumps(self.LM_config),json.dumps(self.tasks_completed[gm.GM_id])]
		self.tasks_completed[gm.GM_id]=[]
		return response

	#LM checks if GM's request is valid
	def verify_request(self,task,gm,node_id,current_time,external_partition=None):

		#check if repartitioning
		if(external_partition is not None):
			if(self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"]==1):
				self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"]=0
				task.node_id=node_id
				task.partition_id=external_partition
				task.lm=self
				task.GM_id=gm.GM_id

                # network delay as the request has to be sent from the LM to the selected worker node
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,LaunchOnNodeEvent(task,self.simulation)))
				return True
			else:# if inconsistent	
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,InconsistencyEvent(task,gm,InconsistencyType.EXTERNAL_INCONSISTENCY,self.simulation)))
		#internal partition
		else:
			if(self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]==1):
				#allot node to task
				self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]=0
				task.node_id=node_id
				task.partition_id=gm.GM_id
				task.GM_id=gm.GM_id
				task.lm=self
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,LaunchOnNodeEvent(task,self.simulation)))
			else:# if inconsistent	
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,InconsistencyEvent(task,gm,InconsistencyType.INTERNAL_INCONSISTENCY,self.simulation)))


	def task_completed(self,task):
		#reclaim resources
		self.LM_config["partitions"][task.partition_id]["nodes"][task.node_id]["CPU"]=1

        # Append the details of the task that was just completed to the list of tasks completed for the corresponding GM that sent it
		self.tasks_completed[task.GM_id].append((task.job.job_id,task.task_id)) #note GM_id used here, not partition, in case of repartitioning
		self.simulation.event_queue.put((task.end_time+NETWORK_DELAY,LMUpdateEvent(self.simulation,periodic=False, gm=self.simulation.gms[task.GM_id])))

#####################################################################################################################
#####################################################################################################################

class GM(object):
	def __init__(self,simulation,GM_id,config):
		self.GM_id=GM_id
		self.simulation=simulation
		self.RR_counter : int = 0
		self.global_view={}
		self.job_queue : List[Job] = []
		self.jobs_scheduled : List[Job] = []

		#populate internal_partitions info
		for LM_id in config["LMs"]:
			self.global_view[LM_id]=config["LMs"][LM_id]

		print("GM", self.GM_id, "initialised")

	#updates global view of GM by getting partial updates from each LM
	def update_status(self,current_time):
		global jobs_completed

		for LM_id in self.simulation.lms:
			lm=self.simulation.lms[LM_id]
			p_partial_status,p_tasks_completed=lm.get_status(self)
			partial_status=json.loads(p_partial_status)
			tasks_completed=json.loads(p_tasks_completed)
			self.global_view[lm.LM_id]=partial_status
			#Through Job object delete task
			for record in tasks_completed:  # Iterate over the tasks completed and update each job's status
				job_id=record[0]
				task_id=record[1]
				for index in range(0,len(self.jobs_scheduled)):
					job=self.jobs_scheduled[index]
							
					if job.job_id==job_id:
						task=job.tasks[task_id]
						job.completed_tasks.append(task)
						if len(job.tasks) == len(job.completed_tasks): #no more tasks left
							job.completion_time=task.end_time  # NOTE:job completion time = end time of last task === max of the task duration for a job
							print(job.completion_time)
							jobs_completed.append(job)
							self.jobs_scheduled.remove(job)
						break

		self.schedule_tasks(current_time)

	def unschedule_job(self,unverified_job):
        # """
        # The job is inserted back into the job_queue of the GM from the job_scheduled queue of the GM

        # :param unverified_job: The job that needs to be moved, as it was assigned on a worker node
        #  not actually available at that time
        # :type unverified_job: Job
        # """
		for index in range(0,len(self.jobs_scheduled)):
			if unverified_job.job_id==self.jobs_scheduled[index].job_id:
				#remove job from list and add to front of job_queue
				self.job_queue.insert(0,self.jobs_scheduled.pop(index))
				break


    # searches the external partitions
	def repartition(self,current_time):
		#search in external partitions:
		for GM_id in self.simulation.gms:
			if GM_id == self.GM_id:  # Skip the partition of the GM searching for a worker node in a external partition 
				continue
			else:
				while len(self.job_queue)>0:  # While the job_queue for the current GM is not empty
					job=self.job_queue[0]# get job from head of queue
					# print("Scheduling Tasks from Job: ",job.job_id)
					
					for task_id in job.tasks:  # Go over the tasks for the job
						task=job.tasks[task_id]
						if(job.tasks[task_id].scheduled):  # If the task is already scheduled then, there is nothing to do
							continue
						matchfound=False
						# print("Scheduling Task:",task_id)
						#which LM? searching the LMs in RR fashion
						LM_id=str(self.RR_counter%self.simulation.NUM_LMS+1)
						self.RR_counter+=1
						#search in external partitions
						for node_id in self.global_view[LM_id]["partitions"][GM_id]["nodes"]:  # iterating over a dict
							node=self.global_view[LM_id]["partitions"][GM_id]["nodes"][node_id]
							if node["CPU"]==1:# node unoccupied
								# print("Match found in internal partitions")
								node["CPU"]=0
								job.tasks[task_id].scheduled=True
								if(job.fully_scheduled()):
									self.jobs_scheduled.append(self.job_queue.pop(0))
								print(current_time,"RepartitionEvent",self.GM_id,",",GM_id,",",job.job_id+"_"+task.task_id)
								self.simulation.event_queue.put((current_time,MatchFoundEvent(job.tasks[task_id],self,self.simulation.lms[LM_id],node_id,current_time,external_partition=GM_id)))#may need to add processing overhead here if required
								matchfound=True
								break
						if(matchfound):  # If this task was successfully placed then, move on to the next task
							continue 
						else:
							print(current_time,"No resources available in cluster")
							return
				# print("Exit scheduling loop for now")

	#search internal partitions
	def schedule_tasks(self,current_time):
		
		while len(self.job_queue)>0:  # While the job_queue for the current GM is not empty
			job=self.job_queue[0]# get job from head of queue
			for task_id in job.tasks:  # Go over the tasks for the job
				if(job.tasks[task_id].scheduled):  # If the task is already scheduled then, there is nothing to do
					continue
				matchfound=False
				#which LM? searching the LMs in RR fashion
				LM_id=str(self.RR_counter%self.simulation.NUM_LMS+1)
				self.RR_counter+=1
				#search in internal partitions
				for node_id in self.global_view[LM_id]["partitions"][self.GM_id]["nodes"]:   # iterating over a dict
					node=self.global_view[LM_id]["partitions"][self.GM_id]["nodes"][node_id]
					if node["CPU"]==1:# node available
						node["CPU"]=0
						job.tasks[task_id].scheduled=True
						if(job.fully_scheduled()):
							self.jobs_scheduled.append(self.job_queue.pop(0))
						self.simulation.event_queue.put((current_time,MatchFoundEvent(job.tasks[task_id],self,self.simulation.lms[LM_id],node_id,current_time)))#may need to add processing overhead here if required
						matchfound=True
						break
				if(matchfound):  # If this task was successfully placed then, move on to the next task
					continue 
				else:
					#repartition
					self.repartition(current_time)
					return
		

	def queue_job(self, job,current_time):
		print(current_time,",","JobArrivalEvent",",",job.job_id,",",self.GM_id)
		job.gm = self
		self.job_queue.append(job)
		if(len(self.job_queue)==1):#first job
			self.schedule_tasks(current_time)

#####################################################################################################################
#####################################################################################################################

class Simulation(object):
	def __init__(self, workload, config, NUM_GMS, NUM_LMS,PARTITION_SIZE,cpu,memory,storage):

        # Each localmaster has one partition per global master so the total number of partitions in the cluster are:
        # NUM_GMS * NUM_LMS
        # Given the number of worker nodes per partition is PARTITION_SIZE
        # so the total_nodes are NUM_GMS*NUM_LMS*PARTITION_SIZE
		self.total_nodes=NUM_GMS*NUM_LMS*PARTITION_SIZE;
		self.NUM_GMS=NUM_GMS
		self.NUM_LMS=NUM_LMS
		self.config=json.load(open(config))
		self.WORKLOAD_FILE=workload
		
		self.jobs = {}
		self.event_queue = queue.PriorityQueue()
		
		#initialise GMs
		self.gms={}
		counter=1
		while len(self.gms)<self.NUM_GMS:
			self.gms[str(counter)]=GM(self,str(counter),pickle.loads(pickle.dumps(self.config)))#create deep copy
			counter+=1

		#initialise LMs
		self.lms={}
		counter=1

		while len(self.lms) < self.NUM_LMS:
			self.lms[str(counter)]=LM(self,str(counter),PARTITION_SIZE,pickle.loads(pickle.dumps(self.config["LMs"][str(counter)])))# create deep copy
			counter+=1

		self.shared_cluster_status = {}
 
		self.jobs_scheduled = 0
		self.jobs_completed = 0
		self.scheduled_last_job = False
		print("Simulation instantiated")

	 #Simulation class
	def run(self):
		last_time = 0

		self.jobs_file = open(self.WORKLOAD_FILE, 'r')

		self.task_distribution = TaskDurationDistributions.FROM_FILE

		line = self.jobs_file.readline()#first job
		new_job = Job(self.task_distribution,line,self)
		self.event_queue.put((float(line.split()[0]), JobArrival(self, self.task_distribution, new_job, self.jobs_file)))
		self.event_queue.put((float(line.split()[0]), LMUpdateEvent(self)))#starting the periodic LM updates
		self.jobs_scheduled = 1

		#start processing events
		while (not self.event_queue.empty()):
			current_time, event = self.event_queue.get()
			assert current_time >= last_time
			last_time = current_time
			new_events = event.run(current_time)
			if(new_events is  None):
				continue
			for new_event in new_events:
				if(new_event is  None):
					continue
				self.event_queue.put(new_event)

		print( "Simulation ending, no more events")
		self.jobs_file.close()

#################MAIN########################

job_start_tstamps = {}
jobs_completed=[]


if __name__ == "__main__":
    WORKLOAD_FILE= sys.argv[1]
    CONFIG_FILE=sys.argv[2]
    NUM_GMS	=int(sys.argv[3])
    NUM_LMS=int(sys.argv[4])
    PARTITION_SIZE=int(sys.argv[5])
    SERVER_CPU=float(sys.argv[6])# currently set to 1 because of comparison with Sparrow
    SERVER_RAM=float(sys.argv[7])# ditto
    SERVER_STORAGE=float(sys.argv[8])# ditto

    NETWORK_DELAY = 0.0005 #same as sparrow


    t1 = time.time() # not simulation's virtual time. This is just to understand how long the program takes
    s = Simulation(WORKLOAD_FILE,CONFIG_FILE,NUM_GMS,NUM_LMS,PARTITION_SIZE,SERVER_CPU,SERVER_RAM,SERVER_STORAGE)
    print("Simulation running")
    s.run()
    print ("Simulation ended in ", (time.time() - t1), " s ")

    print(jobs_completed)
