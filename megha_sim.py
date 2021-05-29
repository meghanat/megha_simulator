import sys
import time
import logging
import math
import random
import queue
import copy
import collections
import json

LM_HEARTBEAT_INTERVAL=30

class TaskDurationDistributions:
	CONSTANT, MEAN, FROM_FILE  = range(3)
class EstimationErrorDistribution:
	CONSTANT, RANDOM, MEAN  = range(3)

#####################################################################################################################
#####################################################################################################################
class Event(object):
	def __init__(self):
		raise NotImplementedError("Event is an abstract class and cannot be instantiated directly")

	def __lt__(self, other):
		return True

	def run(self, current_time):
		""" Returns any events that should be added to the queue. """
		raise NotImplementedError("The run() method must be implemented by each class subclassing Event")

#####################################################################################################################
#####################################################################################################################


class TaskEndEvent(Event):
	def __init__(self,task):
		self.task=task
	def __lt__(self, other):
		return True

	def run(self, current_time):
		self.task.end_time=current_time
		self.task.lm.task_completed(task)

#####################################################################################################################
#####################################################################################################################
	
class LaunchOnnodeEvent(Event):

	def __init__(self,task,node_id,lm,simulation,launchTime):
		self.task=task
		self.lm=lm
		self.node_id=self.node_id
		self.launchTime=launchTime
		self.task.node_id=node_id

	def  run(self):
		self.simulation.event_queue.put((self.launchTime+self.task.duration+NETWORK_DELAY,TaskEndEvent(self.task)))


#####################################################################################################################
#####################################################################################################################


class InconsistencyEvent(Event):
	def __init__(self, simulation,job, GM_id,LM_id,partition_id,node_id):
		self.job=job
		self.GM_id=GM_id
		self.LM_id=LM_id
		self.partition_id=self.partition_id
		self.node_id=self.node_id
		self.simulation=simulation

	def  run():
		new_events=[]

		return new_events

#####################################################################################################################
#####################################################################################################################
class MatchFoundEvent(Event):
	def __init__(self,task, gm,lm,node_id,current_time):
		self.task=task
		self.gm =gm
		self.lm=lm
		self.node_id=node_id
		self.current_time=current_time

	def  run():
		#add network delay to LM, similar to sparrow: 
		self.lm.verify_request(task,gm,node_id,current_time+NETWORK_DELAY)

#####################################################################################################################
#####################################################################################################################

class  MatchNotFoundEvent(Event):
	"""docstring for  """
	def __init__(self, job, gm):
		self.job=job
		self.gm=gm
		

	def run():
		new_events=[]
		return new_events
		
#####################################################################################################################
#####################################################################################################################

class  LMUpdateEvent(Event):
	
	def __init__(self,simulation,periodic=True):
		self.simulation=simulation
		self.periodic=periodic
	def run(self,current_time):
		for gm in self.simulation.gms:
			gm.update_status()
			if(self.periodic and not self.simulation.event_queue.empty()):
				self.simulation.event_queue.put((current_time + LM_HEARTBEAT_INTERVAL,self))
		
#####################################################################################################################
#####################################################################################################################

class JobArrival(Event):

	gm_counter=1

	def __init__(self, simulation, task_distribution, job, jobs_file):
		self.simulation = simulation
		self.task_distribution = task_distribution
		self.job = job
		self.jobs_file = jobs_file

	def __lt__(self, other):
		return True


	def run(self, current_time):
		new_events = []
		#needs to be assigned to a GM - RR
		assigned_GM=self.simulation.gms[str(JobArrival.gm_counter)]
		print("GM assigned:",assigned_GM.GM_id)
		Job.per_job_task_info[self.job.id] = {}
		for tasknr in range(0,self.job.num_tasks):
			Job.per_job_task_info[self.job.id][tasknr] =- 1 
		#GM needs to add job to its queue
		new_events.append(assigned_GM.queue_job(self.job,current_time))

		#increment counter for RR
		gm_counter=(JobArrival.gm_counter+1)%self.simulation.NUM_GMS+1

		# Creating a new Job Arrival event for the next job in the trace
		line = self.jobs_file.readline()
		if (line == ''):
			self.simulation.scheduled_last_job = True
		self.job = Job(self.task_distribution,line,self.simulation)
		new_events.append((self.job.start_time, self))
		self.simulation.jobs_scheduled += 1
		return new_events


#####################################################################################################################
#####################################################################################################################

class Task(object):

	def __init__(self,task_id,job,duration):
		self.task_id=task_id
		self.start_time=job.start_time
		self.scheduled_time=None
		self.end_time=None
		self.job=job
		self.duration=duration
		self.node_id=None
		self.partition_id=None
		self.LM_id=None

class Job(object):
	job_count = 1
	per_job_task_info = {}

	def __init__(self, task_distribution, line,simulation):
		global job_start_tstamps
		
		job_args= (line.split('\n'))[0].split()
		self.start_time = float(job_args[0])
		self.num_tasks= int(job_args[1])
		self.simulation=simulation
		self.tasks={}
		self.task_counter=0
		
		#dephase the incoming job in case it has the exact submission time as another already submitted job
		if self.start_time not in job_start_tstamps:
			job_start_tstamps[self.start_time] = self.start_time
		else:
			job_start_tstamps[self.start_time] += 0.01
			self.start_time = job_start_tstamps[self.start_time]
		
		self.id = Job.job_count
		Job.job_count += 1
		self.completed_tasks_count = 0
		self.end_time = self.start_time
		

		
		if	task_distribution == TaskDurationDistributions.FROM_FILE: 
			self.file_task_execution_time(job_args)
		elif task_distribution == TaskDurationDistributions.CONSTANT:
			while len(self.unscheduled_tasks) < self.num_tasks:
				self.unscheduled_tasks.appendleft(mean_task_duration)

		print("Job created ",self.id, " task: ",len(self.unscheduled_tasks))
		# self.estimate_distribution = estimate_distribution
		

	#Job class	 """ Returns true if the job has completed, and false otherwise. """
	def update_task_completion_details(self, completion_time):
		self.completed_tasks_count += 1
		self.end_time = max(completion_time, self.end_time)
		assert self.completed_tasks_count <= self.num_tasks
		return self.num_tasks == self.completed_tasks_count


	#Job class
	def file_task_execution_time(self, job_args):
		for task_duration in (job_args[3:]):
			duration=int(float(task_duration))	
			self.task_counter+=1
			self.tasks[str(task_counter)]=Task(task_id,job,duration)
		

	#Job class
	def update_remaining_time(self):
		self.remaining_exec_time -= self.estimated_task_duration
		#assert(self.remaining_exec_time >=0)
		if (len(self.unscheduled_tasks) == 0): #Improvement
			self.remaining_exec_time = -1


class LM(object):

	def __init__(self,job,simulation,LM_id,partiton_size,LM_config):
		self.LM_id=LM_id
		self.partiton_size=partiton_size
		self.LM_config=LM_config
		print("LM ",LM_id,"initialised")
		self.simulation=simulation
		self.tasks_completed={}
		for gm in self.simulation.gms:
			self.task_completed[gm.GM_id]=[]

	def verify_request(self,task,gm,node_id,current_time):
		#check node
		if(self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]==1):
			#allot node to task
			self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]=0
			print("LM",self.LM_id,"scheduling task on node",node_id," at ",current_time)
			task.node_id=node_id
			task.partition_id=gm.GM_id
			task.lm=self


	def task_completed(self,task,node_id):
		self.LM_config["partitions"][task.partition_id][node_id]["CPU"]=1
		self.tasks_completed[task.gm.GM_id].append(task)
		self.simulation.event_queue.put((task.end_time+(2*NETWORK_DELAY),LMUpdateEvent(periodic=False)))



class GM(object):
	def __init__(self,simulation,GM_id,config):
		self.GM_id=GM_id
		self.glob=config 
		self.simulation=simulation
		self.RR_counter=0
		self.global_view={}
		self.internal_partitions={} #keyed by LM_id
		self.job_queue=[]

		#populate internal_partitions info
		for LM_id in self.config["LMs"]:
			self.internal_partitions[LM_id]=self.config["LMs"][LM_id]["partitions"][self.GM_id]["nodes"]

		print("GM",self.GM_id," initialised")

	def update_status(self):
		for lm in self.simulation.lm:
			partial_status,tasks_completed=lm.get_status(self)
			self.global_view[lm.LM_id]=partial_status
			for task in tasks_completed:
				#Through Job object delete task
				#Check timestamps everywhere
				#Should all events be returned? Can be modified later
				#Repartition- if no internal resources available
				#Need to clear tasks_completed in LM after update
				#Need to make GM call schedule_tasks again
				#Work it out by hand
				#See if all network delays are accounted for.
				#External and Internal inconsistencies handling.


	#search internal partitions
	def schedule_tasks(self,current_time):
		while len(self.job_queue)>0:
			job=self.job_queue[0]# get job from head of queue
			for task_id in job.tasks:
				#which LM?
				LM_id=str(self.RR_counter%self.simulation.NUM_LMS+1)
				self.RR_counter+=1
				for node in self.internal_partitions[LM_id]:
					if node["CPU"]==1:# node unoccupied
						print("Match found in internal partitions")
						node["CPU"]=0
						self.simulation.event_queue.put(current_time,MatchFoundEvent(job.tasks[task_id],self,self.simulation.lms[LM_id],node["node_id"],current_time))#may need to add processing overhead here if required

	def queue_job(self,job,current_time):

		self.job_queue.append((job,current_time))
		print("Job queued")
		if(len(self.job_queue)==1):#first job
			self.schedule_tasks(current_time)

class Simulation(object):
	def __init__(self, workload, config, NUM_GMS, NUM_LMS,PARTITION_SIZE,cpu,memory,storage):

		self.total_nodes=NUM_GMS*NUM_LMS*PARTITION_SIZE;
		self.NUM_GMS=NUM_GMS
		self.NUM_LMS=NUM_LMS
		self.config=json.load(open(config))
		self.WORKLOAD_FILE=workload
		
		self.jobs = {}
		self.event_queue = queue.PriorityQueue()
		self.gms={}
		counter=1
		while len(self.gms)<self.NUM_GMS:
			self.gms[str(counter)]=GM(self,str(counter),self.config)
			counter+=1
		self.lms={}
		counter=1

		while len(self.lms) < self.NUM_LMS:
			self.lms[str(counter)]=LM(self,str(counter),PARTITION_SIZE,config["LMs"][str(counter)])
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

		line = self.jobs_file.readline()
		print("Job:",line)
		new_job = Job(self.task_distribution,line,self)
		self.event_queue.put((float(line.split()[0]), JobArrival(self, self.task_distribution, new_job, self.jobs_file)))
		self.event_queue.put((float(line.split()[0]), LMUpdateEvent(self)))
		self.jobs_scheduled = 1
		print((float(line.split()[0])))

		while (not self.event_queue.empty()):
			current_time, event = self.event_queue.get()
			assert current_time >= last_time
			last_time = current_time
			new_events = event.run(current_time)
			for new_event in new_events:
				self.event_queue.put(new_event)

		print( "Simulation ending, no more events")
		self.jobs_file.close()

#################MAIN########################

job_start_tstamps = {}

WORKLOAD_FILE= sys.argv[1]
CONFIG_FILE=sys.argv[2]
NUM_GMS	=int(sys.argv[3])
NUM_LMS=int(sys.argv[4])
PARTITION_SIZE=int(sys.argv[5])
SERVER_CPU=float(sys.argv[6])
SERVER_RAM=float(sys.argv[7])
SERVER_STORAGE=float(sys.argv[8])

NETWORK_DELAY = 0.0005 #same as sparrow


t1 = time.time()
s = Simulation(WORKLOAD_FILE,CONFIG_FILE,NUM_GMS,NUM_LMS,PARTITION_SIZE,SERVER_CPU,SERVER_RAM,SERVER_STORAGE)
print("Simulation running")
s.run()
print ("Simulation ended in ", (time.time() - t1), " s ")

finished_file.close()
load_file.close()
