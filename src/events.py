from typing import List, Optional, Tuple, Final, TYPE_CHECKING

from job import Job
from task import Task
from values import LM_HEARTBEAT_INTERVAL, NETWORK_DELAY, InconsistencyType

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from gm import GM

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
