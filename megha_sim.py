import sys
import time
import logging
import math
import random
import queue
import copy
import collections
import json
import copy
import pickle
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
		print(current_time,",","TaskEndEvent",",",self.task.job.id+"_"+self.task.task_id)
		self.task.end_time=current_time
		self.task.lm.task_completed(self.task)

#####################################################################################################################
#####################################################################################################################
	
class LaunchOnnodeEvent(Event):

	def __init__(self,task,simulation):
		self.task=task
		self.simulation=simulation

	def run(self, current_time):
		print(current_time,",","LaunchOnnodeEvent",",",self.task.job.id+"_"+self.task.task_id,",",self.task.partition_id+"_"+self.task.node_id)
		self.simulation.event_queue.put((current_time+self.task.duration+NETWORK_DELAY,TaskEndEvent(self.task)))


#####################################################################################################################
#####################################################################################################################


class InconsistencyEvent(Event):
	def __init__(self,task,gm,type,simulation):
		self.task=task
		self.gm=gm
		self.type=type
		self.simulation=simulation

	def run(self, current_time):
		if(self.type==0):#internal inconsistency
			print(current_time,",","InternalInconsistencyEvent")
		else:
			print(current_time,",","ExternalInconsistencyEvent")
		self.task.scheduled=False

		#if job already moved to jobs_scheduled queue, need to remove and add to front of queue
		self.gm.unschedule_job(self.task.job)
		self.simulation.event_queue.put((current_time+NETWORK_DELAY,LMUpdateEvent(self.simulation,periodic=False)))

		#***********************************************************************#
		#  NEED TO CHECK THIS AND SEE IF IT CAN BE MODIFIED SUCH				#
		# THAT THE UNSCHEDULE OPERATION IS CALLED JUST BEFORE CONFIG IS UPDATED #
		#  THIS IS TO ENSURE THE DELAY DUE TO INCONSISTENCY IS REFLECTED		#
		#***********************************************************************#


#####################################################################################################################
#####################################################################################################################
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
		print(current_time,",","MatchFoundEvent",",",self.task.job.id+"_"+self.task.task_id,",",self.gm.GM_id+"_"+str(self.node_id))
		self.lm.verify_request(self.task,self.gm,self.node_id,current_time+NETWORK_DELAY,external_partition=self.external_partition)

#####################################################################################################################
#####################################################################################################################

class  LMUpdateEvent(Event):
	
	def __init__(self,simulation,periodic=True):
		self.simulation=simulation
		self.periodic=periodic
	def run(self,current_time):
		print(current_time,",","LMUpdateEvent",",",self.periodic)
		for GM_id in self.simulation.gms:
			self.simulation.gms[GM_id].update_status(current_time)

		if(self.periodic and not self.simulation.event_queue.empty()):
			self.simulation.event_queue.put((current_time + LM_HEARTBEAT_INTERVAL,self))
		
#####################################################################################################################
#####################################################################################################################

class JobArrival(Event):

	gm_counter=0

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
		JobArrival.gm_counter=(JobArrival.gm_counter)%self.simulation.NUM_GMS+1
		assigned_GM=self.simulation.gms[str(JobArrival.gm_counter)]
		Job.per_job_task_info[self.job.id] = {}
		for tasknr in range(0,self.job.num_tasks):
			Job.per_job_task_info[self.job.id][tasknr] =- 1 
		#GM needs to add job to its queue
		assigned_GM.queue_job(self.job,current_time)


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
		self.completed_tasks=[]
		self.gm=None
		
		
		#dephase the incoming job in case it has the exact submission time as another already submitted job
		if self.start_time not in job_start_tstamps:
			job_start_tstamps[self.start_time] = self.start_time
		else:
			job_start_tstamps[self.start_time] += 0.01
			self.start_time = job_start_tstamps[self.start_time]
		
		self.id = str(Job.job_count)
		Job.job_count += 1
		self.completed_tasks_count = 0
		self.end_time = self.start_time
		

		
		if	task_distribution == TaskDurationDistributions.FROM_FILE: 
			self.file_task_execution_time(job_args)
		# elif task_distribution == TaskDurationDistributions.CONSTANT:
		# 	while len(self.unscheduled_tasks) < self.num_tasks:
		# 		self.unscheduled_tasks.appendleft(mean_task_duration)
		
	def fully_scheduled(self):
		
		for task_id in self.tasks:
			if not self.tasks[task_id].scheduled:
				return False
		return True


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
			self.tasks[str(self.task_counter)]=Task(str(self.task_counter),self,duration)
		

	#Job class
	def update_remaining_time(self):
		self.remaining_exec_time -= self.estimated_task_duration
		#assert(self.remaining_exec_time >=0)
		if (len(self.unscheduled_tasks) == 0): #Improvement
			self.remaining_exec_time = -1


class LM(object):

	def __init__(self,simulation,LM_id,partiton_size,LM_config):
		self.LM_id=LM_id
		self.partiton_size=partiton_size
		self.LM_config=LM_config
		print("LM ",LM_id,"initialised")
		self.simulation=simulation
		self.tasks_completed={}
		for GM_id in self.simulation.gms:
			self.tasks_completed[GM_id]=[]

	def get_status(self,gm):

		response=[ json.dumps(self.LM_config),json.dumps(self.tasks_completed[gm.GM_id])]
		self.tasks_completed[gm.GM_id]=[]
		return response

	def verify_request(self,task,gm,node_id,current_time,external_partition=None):

		#check if repartitioning
		print("external_partition:",external_partition)
		if(external_partition is not None):
			if(self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"]==1):
				self.LM_config["partitions"][external_partition]["nodes"][node_id]["CPU"]=0
				task.node_id=node_id
				task.partition_id=external_partition
				task.lm=self
				task.GM_id=gm.GM_id
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,LaunchOnnodeEvent(task,self.simulation)))
				return True
			else:
				
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,InconsistencyEvent(task,gm,0,self.simulation)))
		
		else:
			if(self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]==1):
				#allot node to task
				self.LM_config["partitions"][gm.GM_id]["nodes"][node_id]["CPU"]=0
				task.node_id=node_id
				task.partition_id=gm.GM_id
				task.GM_id=gm.GM_id
				task.lm=self
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,LaunchOnnodeEvent(task,self.simulation)))
			else:
				self.simulation.event_queue.put((current_time+NETWORK_DELAY,InconsistencyEvent(task,gm,1,self.simulation)))


	def task_completed(self,task):

		self.LM_config["partitions"][task.partition_id]["nodes"][task.node_id]["CPU"]=1
		self.tasks_completed[task.GM_id].append((task.job.id,task.task_id)) #note GM_id used here, not partition, in case of repartitioning

		self.simulation.event_queue.put((task.end_time+(2*NETWORK_DELAY),LMUpdateEvent(self.simulation,periodic=False)))



class GM(object):
	def __init__(self,simulation,GM_id,config):
		self.GM_id=GM_id
		self.simulation=simulation
		self.RR_counter=0
		self.global_view={}
		self.job_queue=[]
		self.jobs_scheduled=[]

		#populate internal_partitions info
		for LM_id in config["LMs"]:
			self.global_view[LM_id]=config["LMs"][LM_id]

		print("GM",self.GM_id," initialised")

	def update_status(self,current_time):
		global jobs_completed

		for LM_id in self.simulation.lms:
			lm=self.simulation.lms[LM_id]
			p_partial_status,p_tasks_completed=lm.get_status(self)
			partial_status=json.loads(p_partial_status)
			tasks_completed=json.loads(p_tasks_completed)
			self.global_view[lm.LM_id]=partial_status
			#Through Job object delete task
			for record in tasks_completed:
				job_id=record[0]
				task_id=record[1]
				for index in range(0,len(self.jobs_scheduled)):
					job=self.jobs_scheduled[index]
							
					if job.id==job_id:
						task=job.tasks[task_id]
						job.completed_tasks.append(task)
						if len(job.tasks) == len(job.completed_tasks): #no more tasks left
							job.completion_time=task.end_time
							# print("Before app:",len(jobs_completed))
							jobs_completed.append(job)
							# print("After app:",len(jobs_completed))
							# print("Queue before:",len(self.jobs_scheduled))
							self.jobs_scheduled.remove(job)
							# print("Queue after:",len(self.jobs_scheduled))
						break
						# print("Tasks before deletion: ",len(job.tasks))
						del job.tasks[task.task_id]
						# print("Tasks after deletion: ",len(job.tasks))
				
						

		self.schedule_tasks(current_time)
				
				#Check timestamps everywhere
				#Should all events be returned? Can be modified later
				#Repartition- if no internal resources available
				#Need to clear tasks_completed in LM after update - done
				#Need to make GM call schedule_tasks again - done
				#Work it out by hand
				#See if all network delays are accounted for.
				#External and Internal inconsistencies handling.

	def unschedule_job(self,unverified_job):

		for index in range(0,len(self.jobs_scheduled)):
			if unverified_job.id==self.jobs_scheduled[index].id:
				#remove job from list and add to front of job_queue
				self.job_queue.insert(0,self.jobs_scheduled.pop(index))



	def repartition(self,current_time):
		#search in external partitions:
		for GM_id in self.simulation.gms:
			if GM_id == self.GM_id:
				continue
			else:
				while len(self.job_queue)>0:
					job=self.job_queue[0]# get job from head of queue
					# print("Scheduling Tasks from Job: ",job.id)
					
					for task_id in job.tasks:
						task=job.tasks[task_id]
						if(job.tasks[task_id].scheduled):
							continue
						matchfound=False
						# print("Scheduling Task:",task_id)
						#which LM?
						LM_id=str(self.RR_counter%self.simulation.NUM_LMS+1)
						self.RR_counter+=1
						#search in internal partitions
						for node_id in self.global_view[LM_id]["partitions"][GM_id]["nodes"]:
							node=self.global_view[LM_id]["partitions"][GM_id]["nodes"][node_id]
							if node["CPU"]==1:# node unoccupied
								# print("Match found in internal partitions")
								node["CPU"]=0
								job.tasks[task_id].scheduled=True
								if(job.fully_scheduled()):
									self.jobs_scheduled.append(self.job_queue.pop(0))
								print(current_time,"RepartitionEvent",self.GM_id,",",GM_id,",",job.id+"_"+task.task_id)
								self.simulation.event_queue.put((current_time,MatchFoundEvent(job.tasks[task_id],self,self.simulation.lms[LM_id],node_id,current_time,external_partition=GM_id)))#may need to add processing overhead here if required
								matchfound=True
								break
						if(matchfound):
							continue 
						else:
							print(current_time,"No resources available in cluster")
							return
				# print("Exit scheduling loop for now")

	#search internal partitions
	def schedule_tasks(self,current_time):
		
		while len(self.job_queue)>0:
			job=self.job_queue[0]# get job from head of queue
			# print("Scheduling Tasks from Job: ",job.id)
			
			for task_id in job.tasks:
				if(job.tasks[task_id].scheduled):
					continue
				matchfound=False
				# print("Scheduling Task:",task_id)
				#which LM?
				LM_id=str(self.RR_counter%self.simulation.NUM_LMS+1)
				self.RR_counter+=1
				#search in internal partitions
				for node_id in self.global_view[LM_id]["partitions"][self.GM_id]["nodes"]:
					node=self.global_view[LM_id]["partitions"][self.GM_id]["nodes"][node_id]
					if node["CPU"]==1:# node unoccupied
						# print("Match found in internal partitions")
						node["CPU"]=0
						job.tasks[task_id].scheduled=True
						if(job.fully_scheduled()):
							self.jobs_scheduled.append(self.job_queue.pop(0))
						self.simulation.event_queue.put((current_time,MatchFoundEvent(job.tasks[task_id],self,self.simulation.lms[LM_id],node_id,current_time)))#may need to add processing overhead here if required
						matchfound=True
						break
				if(matchfound):
					continue 
				else:
					#repartition
					self.repartition(current_time)
					# print("GM ",self.GM_id," out of resources")
					return
		# print("Exit scheduling loop for now")

	def queue_job(self,job,current_time):
		print(current_time,",","JobArrivalEvent",",",job.id,",",self.GM_id)
		job.gm=self
		self.job_queue.append(job)
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
			self.gms[str(counter)]=GM(self,str(counter),pickle.loads(pickle.dumps(self.config)))
			counter+=1
		self.lms={}
		counter=1

		while len(self.lms) < self.NUM_LMS:
			self.lms[str(counter)]=LM(self,str(counter),PARTITION_SIZE,pickle.loads(pickle.dumps(self.config["LMs"][str(counter)])))
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
		new_job = Job(self.task_distribution,line,self)
		self.event_queue.put((float(line.split()[0]), JobArrival(self, self.task_distribution, new_job, self.jobs_file)))
		self.event_queue.put((float(line.split()[0]), LMUpdateEvent(self)))
		self.jobs_scheduled = 1

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

print(jobs_completed)