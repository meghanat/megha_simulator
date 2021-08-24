"""
This file contains the implementation of various `Events` in the simulator.

Raises:
    NotImplementedError: This exception is raised when attempting to create \
    an instance of the `Event` class.
    NotImplementedError: This exception is raised when attempting to call \
    `run` on an instance of the `Event` class.
"""
from __future__ import annotations
from io import TextIOWrapper
from typing import List, Optional, Tuple, TYPE_CHECKING

from job import Job
from task import Task
from simulation_logger import SimulatorLogger
from simulator_utils.values import (LM_HEARTBEAT_INTERVAL, NETWORK_DELAY,
                                    InconsistencyType,
                                    TaskDurationDistributions)

# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
    from local_master import LM
    from global_master import GM
    from simulation import Simulation

# Get the logger object for this module
logger = SimulatorLogger(__name__).get_logger()


class Event(object):
    """
    This is the abstract `Event` object class.

    Args:
        object (Object): Parent object class.
    """

    def __init__(self):
        """
        One cannot initialise the object of the abstract class `Event`.

        This raises a `NotImplementedError` on attempting to create \
        an object of the class.

        Raises:
            NotImplementedError: This exception is raised when attempting to \
            create an instance of the `Event` class.
        """
        raise NotImplementedError(
            "Event is an abstract class and cannot be instantiated directly")

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `Event` object with another object of `Event` class.

        Args:
            other (Event): The object to compare with.

        Returns:
            bool: The Event object is always lesser than the object it is \
            compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to handle the `Event`.

        Args:
            current_time (float): The current time in the simulation.

        Raises:
            NotImplementedError: This exception is raised when attempting to \
            call `run` on an instance of the `Event` class.
        """
        """Return any events that should be added to the queue."""
        raise NotImplementedError(
            "The run() method must be implemented by each class subclassing "
            "Event")


##########################################################################
##########################################################################


class TaskEndEvent(Event):
    """
    This event is created when a task has completed.

    The `end_time` is set as the `current_time` of running the event.

    Args:
            Event (Event): Parent Event class.
    """

    def __init__(self, task: Task):
        """
        Initialise the instance of the `TaskEndEvent` class.

        Args:
                task (Task): The task object representing the task which has \
                completed.
        """
        self.task: Task = task

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `TaskEndEvent` object with another object of Event class.

        Args:
                other (Event): The object to compare with.

        Returns:
                bool: The `TaskEndEvent` object is always lesser than the \
                object it is compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to perform on the event of task completion.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the TaskEndEvent
        logger.info(f"{current_time} , TaskEndEvent , {self.task.job.job_id}"
                    f"_{self.task.task_id}_{self.task.duration}")
        self.task.end_time = current_time
        if self.task.lm is not None:
            self.task.lm.task_completed(self.task)

###############################################################################
###############################################################################


class LaunchOnNodeEvent(Event):
    """
    Event created after the Local Master verifies the Global Master's request.

    This event is created when a task is sent to a particular worker node in a
    particular partition, selected by the global master and verified by the
    local master.

    Args:
            Event (Event): Parent Event class.
    """

    def __init__(self, task: Task, simulation: Simulation):
        """
        Initialise the instance of the `LaunchOnNodeEvent` class.

        Args:
            task (Task): The Task object to be launched on the selected node.
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.task = task
        self.simulation = simulation

    def run(self, current_time: float):
        """
        Run the actions to handle a `LaunchOnNodeEvent`.

        Run the actions to perform on the event of launching a task on the \
        node.

        Args:
            current_time (float): The current time in the simulation.
        """
        assert self.task.partition_id is not None
        assert self.task.node_id is not None

        # Log the LaunchOnNodeEvent
        logger.info(
            f"{current_time} , "
            "LaunchOnNodeEvent , "
            f"{self.task.job.job_id}_"
            f"{self.task.task_id} , "
            f"{self.task.partition_id}_"
            f"{self.task.node_id}")

        # launching requires network transfer
        self.simulation.event_queue.put(
            (current_time + self.task.duration, TaskEndEvent(self.task)))


##########################################################################
##########################################################################


class InconsistencyEvent(Event):
    """
    Event created when the Global Master tries placing a task on a busy node.

    This happens when the Global Master has outdated information about the
    cluster. This event is created by the Local Master.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(self, task: Task, gm: GM, type: InconsistencyType,
                 simulation: Simulation):
        """
        Initialise the instance of the InconsistencyEvent class.

        Args:
            task (Task): The Task object that caused the inconsistency.
            gm (GM): The Global Master responsible for the inconsistency.
            type (InconsistencyType): The type of inconsistency caused.
            simulation (Simulation): The simulation object to insert events \
            into.
        """
        self.task: Task = task
        self.gm: GM = gm
        self.type: InconsistencyType = type
        self.simulation: Simulation = simulation

    def run(self, current_time: float):
        """
        Run the actions to handle the `InconsistencyEvent`.

        Run the actions to perform on the event of the Global
        Master requesting to place a task on a busy worker node.

        Args:
            current_time (float): The current time in the simulation.
        """
        if(self.type == InconsistencyType.INTERNAL_INCONSISTENCY):
            """Internal inconsistency -> failed to place task on an internal
               partition."""
            logger.info(f"{current_time} , InternalInconsistencyEvent")
        else:
            """External inconsistency -> failed to place task on an external
               partition."""
            logger.info(f"{current_time} , ExternalInconsistencyEvent")
        self.task.scheduled = False

        """
        If the job is already moved to jobs_scheduled queue, then we need to
        remove it and add it to the front of the queue.
        """
        self.gm.unschedule_job(self.task.job)
        self.simulation.event_queue.put(
            (current_time, LMUpdateEvent(
                self.simulation, periodic=False, gm=self.gm)))


##########################################################################
##########################################################################
# created when GM finds a match in the external or internal partition
class MatchFoundEvent(Event):
    """
    Event created when the Global Master finds a free worker node.

    Args:
        Event (Event): Parent Event class.
    """

    def __init__(
            self,
            task: Task,
            gm: GM,
            lm: LM,
            node_id: str,
            current_time: float,
            external_partition: Optional[str] = None):
        """
        Initialise the instance of the `MatchFoundEvent` class.

        Args:
            task (Task): The task object to launch on the selected node.
            gm (GM): The Global Master that wants to allocate the task to \
            a worker node.
            lm (LM): The Local Master, the selected worker node belongs to.
            node_id (str): The identifier of the selected worker node.
            current_time (float): The current time in the simulation.
            external_partition (Optional[str], optional): Identifier of \
            the Global Master to which the external partition (if selected) \
            belongs to. Defaults to None.
        """
        self.task = task
        self.gm = gm
        self.lm = lm
        self.node_id = node_id
        self.current_time = current_time
        self.external_partition = external_partition

    def run(self, current_time: float):
        """
        Run the actions to handle the `MatchFoundEvent`.

        Request the Local Master to which the worker node belongs, to
        verify if the worker node is indeed free to take up a task.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the MatchFoundEvent
        logger.info(f"{current_time} , "
                    "MatchFoundEvent , "
                    f"{self.task.job.job_id}_{self.task.task_id} , "
                    f"{self.gm.GM_id}_{self.lm.LM_id}_{self.node_id}")

        # Add network delay to LM, similar to sparrow:
        self.lm.verify_request(
            self.task,
            self.gm,
            self.node_id,
            current_time +
            NETWORK_DELAY,
            external_partition=self.external_partition)

##########################################################################
##########################################################################
# created periodically or when LM needs to piggyback update on response


class LMUpdateEvent(Event):
    """
    Event created when Local Masters send updates to Global Masters.

    Event created when the Local Masters update the Global Master
    about the status of the cluster.

    Args:
        Event (Event): Parent `Event` class.
    """

    def __init__(self, simulation: Simulation,
                 periodic: bool = True, gm: Optional[GM] = None):
        """
        Initialise the instance of the `LMUpdateEvent` class.

        Args:
            simulation (Simulation): The simulation object to insert events \
            into.
            periodic (bool, optional): Whether the updates are periodic or not\
            . Defaults to True.
            gm (Optional[GM], optional): The Global Master that needs to be \
            updated about the state of the cluster. Defaults to None.
        """
        self.simulation: Simulation = simulation
        self.periodic: bool = periodic
        self.gm: Optional[GM] = gm

        if self.periodic is True:
            assert self.gm is None, ("LMUpdateEvent.__init__: Periodic is set"
                                     " to true so self.gm must be None!")
        elif self.periodic is False:
            assert self.gm is not None, ("LMUpdateEvent.__init__: Periodic is"
                                         " set to false so self.gm must not be"
                                         " None!")

    def run(self, current_time: float):
        """
        Run the actions to handle the `LMUpdateEvent`.

        If the update is **none periodic** then just call the \
        `GM.update_status` on the particular Global Master.
        If the update is **periodic** then call `GM.update_status` on each
        and every Global Master and place another periodic `LMUpdateEvent`
        to occur after `LM_HEARTBEAT_INTERVAL + NETWORK_DELAY` interval of \
        time.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the LMUpdateEvent
        logger.info(f"{current_time} , LMUpdateEvent , {self.periodic}")

        # update only that GM which is inconsistent or if the GM's task has
        # completed
        if not self.periodic:
            assert self.gm is not None, ("LMUpdateEvent.run: Periodic is"
                                         " set to false so self.gm must not be"
                                         " None!")
            self.gm.update_status(current_time + NETWORK_DELAY)

        if self.periodic and not self.simulation.event_queue.empty():
            for GM_id in self.simulation.gms:
                self.simulation.gms[GM_id].update_status(
                    current_time + NETWORK_DELAY)
            """
            Add the next heartbeat, network delay added because intuitively
            we do not include it in the LM_HEARTBEAT INTERVAL.
            """
            self.simulation.event_queue.put(
                (current_time + LM_HEARTBEAT_INTERVAL + NETWORK_DELAY, self))

##########################################################################
##########################################################################
# created for each job


class JobArrival(Event):
    """
    Event created on the arrival of a `Job` into the user queue.

    Args:
        Event (Event): Parent `Event` class.
    """

    gm_counter: int = 0

    def __init__(self, simulation: Simulation,
                 task_distribution: TaskDurationDistributions,
                 job: Job,
                 jobs_file: TextIOWrapper):
        """
        Initialise the instance of the `JobArrival` class.

        Args:
            simulation (Simulation): The simulation object to insert events \
            into.
            task_distribution (TaskDurationDistributions): Select the \
            distribution of the duration/run-time of the tasks of the Job
            job (Job): The Job object that has arrived into the user queue.
            jobs_file (TextIOWrapper): File handle to the input trace file.
        """
        self.simulation = simulation
        self.task_distribution = task_distribution
        self.job = job
        self.jobs_file = jobs_file  # Jobs file (input trace file) handler

    def __lt__(self, other: Event) -> bool:
        """
        Compare the `JobArrival` object with another object of Event class.

        Args:
            other (Event): The object to compare with.

        Returns:
            bool: The `JobArrival` object is always lesser than the object \
                  it is compared with.
        """
        return True

    def run(self, current_time: float):
        """
        Run the actions to handle the `JobArrival` event.

        Args:
            current_time (float): The current time in the simulation.
        """
        # Log the JobArrival
        logger.info(f"{current_time} , JobArrival , {self.task_distribution}")

        new_events: List[Tuple[float, Event]] = []
        # needs to be assigned to a GM - RR
        JobArrival.gm_counter = (
            JobArrival.gm_counter % self.simulation.NUM_GMS + 1)
        # assigned_GM --> Handle to the global master object
        assigned_GM: GM = self.simulation.gms[str(JobArrival.gm_counter)]
        # GM needs to add job to its queue
        assigned_GM.queue_job(self.job, current_time)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if len(line) == 0:
            self.simulation.scheduled_last_job = True
        else:
            self.job = Job(self.task_distribution, line, self.simulation)
            new_events.append((self.job.start_time, self))
            self.simulation.jobs_scheduled += 1
        return new_events
