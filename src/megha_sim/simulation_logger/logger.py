"""
Create the logging object to use in the different components of the simulator.

The logging object is used to log different events and analyse them at \
runtime, as the simulator executes.
This provides a detailed and uniform manner for analysis, without generating \
large log files.
"""
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from typing_extensions import TypedDict
from simulation_logger.msg_list import (MATCHING_LOGIC_MSG,
                                        MATCHING_LOGIC_REPARTITION_MSG,
                                        CLUSTER_SATURATED_MSG)
from simulator_utils.values import NETWORK_DELAY


class TColors():
    """Class for declaring common ANSI escape sequences."""

    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_CYAN = '\033[96m'
    OK_GREEN = '\033[92m'
    SUCCESS = '\033[92;1m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class DataPoints(TypedDict):
    """
    Store the statistics of the data points measured.

    Args:
        TypedDict (TypedDict): The `TypedDict` base class.
    """

    internal_matching_logic_op: int
    external_matching_logic_op: int
    task_end_event: int
    launch_on_node_event: int
    internal_inconsistency_event: int
    external_inconsistency_event: int
    match_found_event: int
    periodic_lm_update_event: int
    aperiodic_lm_update_event: int
    job_arrival_event: int
    cluster_saturated_event: int


class TaskInfo(TypedDict):
    """
    Store the information about the jobs.

    Args:
        TypedDict (TypedDict): The `TypedDict` base class.
    """
    job_arrival_time: float  # Job Arrival Time
    task_launch_time: float  # Task Launch Time (launch on node event)
    task_duration_trace: int  # Task Duration From trace
    task_duration_gm: float  # Task Duration when GM realizes that task is completed
    task_queuing_delay: float
    task_end_time_node: float  # Task end time on node


class MatchingOps(TypedDict):
    """
    Format and types of each of the parameters measured per task.

    Args:
        NamedTuple (NamedTuple): Typed version of collections.namedtuple().
    """

    job_id: str
    task_id: str
    workers_searched: int


class Logger:
    """Class to analyses the generate logs during runtime."""

    LINE_SEPARATOR = "\n\n" + "-" * 80 + "\n\n"
    INTEGRITY_MESSAGE = ("The simulator statistics generated, are from a "
                         "successful run of the simulator over the trace "
                         "dataset.\n")

    def __init__(self, output_file_path: Path) -> None:
        """
        Initialise the object.

        Args:
            output_file_path (Path): The path to the output file.
        """
        self.output_file_path = output_file_path

        self.has_integrity: bool = False
        self.metadata_text: List[str] = list()
        self.data_points: DataPoints = \
            DataPoints(internal_matching_logic_op=0,
                       external_matching_logic_op=0,
                       task_end_event=0,
                       launch_on_node_event=0,
                       internal_inconsistency_event=0,
                       external_inconsistency_event=0,
                       match_found_event=0,
                       periodic_lm_update_event=0,
                       aperiodic_lm_update_event=0,
                       job_arrival_event=0,
                       cluster_saturated_event=0)
        self.matching_logic_op_task_measurements: Dict[str, MatchingOps] = \
            dict()

        # For job optimisation
        self.queuing_delay: Dict[str, Dict[str, TaskInfo]] = dict()
        self.all_job_ct = dict()

        self.internal_inconsistency_count_per_task: Dict[str, int] = dict()
        self.external_inconsistency_count_per_task: Dict[str, int] = dict()

        # Create the output file to test the path and create the empty file.
        # Any path related errors will now be generated at the beginning of
        # simulation rather than at the end of the simulation.
        with open(self.output_file_path, "w") as _:
            ...

    def metadata(self, msg: str) -> None:
        """
        Store the data to write out into the top of the generated statistics \
        file.

        Args:
            msg (str): The metadata message to write.
        """
        self.metadata_text.append(msg)

    def info(self, msg: str) -> None:
        """
        Take the log message and run the analysis on it.

        Args:
            msg (str): The log message to run analysis on.
        """
        if msg.startswith(MATCHING_LOGIC_MSG) is True or \
           msg.startswith(MATCHING_LOGIC_REPARTITION_MSG) \
           is True:
            if msg.startswith(MATCHING_LOGIC_MSG) is True:
                self.data_points["internal_matching_logic_op"] += 1
            else:
                self.data_points["external_matching_logic_op"] += 1
            matching_logic_details: List[str] = (msg
                                                 .split(" , ")[1:])
            self.__parse_matching_logic_stmt(matching_logic_details)
        else:  # The logging message if for an event in the simulator
            # Get the event name from the log message
            event_name = msg.split(" , ")[1]

            # String with the format x_y
            # Where,
            # x is the job ID
            # y is the task ID
            job_id_task_id: str

            if event_name == "TaskEndEvent":
                self.data_points["task_end_event"] += 1
                split_msg = msg.split(" , ")
                task_end_time_node = float(split_msg[0])
                job_id = split_msg[2]
                task_id = split_msg[3]
                self.queuing_delay[job_id][task_id]["task_end_time_node"] = \
                    task_end_time_node
                self.queuing_delay[job_id][task_id]["task_duration_gm"] = \
                    (task_end_time_node + 2 * NETWORK_DELAY) - \
                    self.queuing_delay[job_id][task_id]["job_arrival_time"]

            elif event_name == "LaunchOnNodeEvent":
                self.data_points["launch_on_node_event"] += 1
                vals = msg.split(" , ")
                current_time = float(vals[0])  # This is the task lauch time
                job_id = vals[2]
                task_id = vals[3]
                # Task duration as per the trace dataset
                duration = int(vals[-2])
                # Job start time is the job arrival time
                start_time = float(vals[-1])
                task_qd = current_time - start_time

                assert task_qd >= 0, "Task_qd is negative"

                if job_id not in self.queuing_delay:
                    self.queuing_delay[job_id] = dict()
                self.queuing_delay[job_id][task_id] = TaskInfo(
                    job_arrival_time=start_time,
                    task_launch_time=current_time,
                    task_duration_trace=duration,
                    task_duration_gm=0,
                    task_queuing_delay=task_qd,  # task_qd
                    task_end_time_node=0)

            elif event_name == "InternalInconsistencyEvent":
                self.data_points["internal_inconsistency_event"] += 1
                job_id_task_id = msg.split(" , ")[2]
                self.internal_inconsistency_count_per_task[job_id_task_id] = \
                    self.internal_inconsistency_count_per_task.get(
                        job_id_task_id, 0) + 1
            elif event_name == "ExternalInconsistencyEvent":
                self.data_points["external_inconsistency_event"] += 1
                job_id_task_id = msg.split(" , ")[2]
                self.external_inconsistency_count_per_task[job_id_task_id] = \
                    self.external_inconsistency_count_per_task.get(
                        job_id_task_id, 0) + 1
            elif event_name == "MatchFoundEvent":
                self.data_points["match_found_event"] += 1
            elif event_name == "LMUpdateEvent":
                is_periodic = msg.split(" , ")[2]
                assert is_periodic == "True" or is_periodic == "False"
                if is_periodic == "True":
                    self.data_points["periodic_lm_update_event"] += 1
                else:
                    self.data_points["aperiodic_lm_update_event"] += 1
            elif event_name == "JobArrival":
                self.data_points["job_arrival_event"] += 1
            elif event_name == CLUSTER_SATURATED_MSG:
                self.data_points["cluster_saturated_event"] += 1
            elif event_name == "UpdateStatusForGM":
                vals = msg.split(" , ")
                job_id = int(vals[2])
                job_ct = float(vals[3])
                self.all_job_ct[job_id] = job_ct

    def integrity(self) -> None:
        """Add a message at the end of the statistics file to mark \
        successful completion of the log analysis."""
        self.has_integrity = True

    def __parse_matching_logic_stmt(self, matching_logic_details: List[str]):
        """
        Parse and record details of the Matching Logic logs.

        Args:
            matching_logic_details (List[str]): List of the details to record.
        """
        job_id, task_id = matching_logic_details[1].split("_")
        key: str = f"{job_id}_{task_id}"

        assert len(matching_logic_details[1].split("_")) == 2

        if key in self.matching_logic_op_task_measurements:
            self.matching_logic_op_task_measurements[key]["workers_searched"] \
                += 1
        else:
            self.matching_logic_op_task_measurements[key] = \
                MatchingOps(job_id=job_id,
                            task_id=task_id,
                            workers_searched=1)

    def flush(self):
        """
        Write out the generated statistics into an output file.

        The output file is given by `self.output_file_path`.
        """

        # Check if all the tasks have been accounted for
        if (len(self.matching_logic_op_task_measurements) !=
                self.data_points["task_end_event"]):
            self.has_integrity = False

        # ---

        sanity_matching_logic_ops_count: int = 0
        for key in self.matching_logic_op_task_measurements:
            sanity_matching_logic_ops_count += \
                (self.matching_logic_op_task_measurements[key]
                 ["workers_searched"])

        # Check if all the 'matching logic' events have been accounted for
        measured_matching_logic_ops_count = (self.data_points
                                             ["external_matching_logic_op"] +
                                             self.data_points
                                             ["internal_matching_logic_op"])
        if (measured_matching_logic_ops_count !=
                sanity_matching_logic_ops_count):
            self.has_integrity = False

        # ---

        # Write all the metadata, statistics and other data into the output
        # file
        with open(self.output_file_path, "w") as file_handler:
            for line in self.metadata_text:
                file_handler.write(f"{line}\n")

            file_handler.write(Logger.LINE_SEPARATOR)

            for key in self.data_points:
                file_handler.write(f"{TColors.BOLD}{key}{TColors.END} :"
                                   f" {self.data_points[key]}\n")

            file_handler.write(Logger.LINE_SEPARATOR)

            file_handler.write("Derived attributes:\n")

            # Write out derived measurements.
            # 1. Total inconsistency events
            total_inconsistency_event = (self.data_points
                                         ['internal_inconsistency_event'] +
                                         self.data_points
                                         ['external_inconsistency_event'])
            file_handler\
                .write(f"{TColors.BOLD}Total Inconsistency count :"
                       f"{TColors.END} {total_inconsistency_event}\n")

            # 2. Total matching logic operations
            total_matching_logic_op = (self.data_points
                                       ["external_matching_logic_op"] +
                                       self.data_points
                                       ["internal_matching_logic_op"])
            file_handler\
                .write(f"{TColors.BOLD}Total matching logic operations :"
                       f"{TColors.END} {total_matching_logic_op}\n")

            # 3. Success percentage of matching logic operation
            total_task_count = len(self.matching_logic_op_task_measurements
                                   .keys())

            # Function to calculate ratio of free worker searched to
            # total number of workers searched for a task
            fraction_of_workers_searched_per_task = \
                (lambda task_id: 1 /
                 (self.matching_logic_op_task_measurements
                  [task_id]
                  ["workers_searched"]
                  )
                 )
            # Find the average success percentage of finding a worker node
            # Higher the value = Lesser workers searched to find a free worker
            # node
            success_percent = \
                sum(
                    map(fraction_of_workers_searched_per_task,
                        self.matching_logic_op_task_measurements
                        )
                ) / total_task_count

            file_handler.write(f"{TColors.BOLD}Percentage ratio of free worker"
                               f" found to number of workers searched"
                               f"{TColors.END} = success_percent="
                               f"{success_percent:%}\n")

            # 4. Average number of worker searched per task
            workers_searched_per_task = \
                (lambda task_id:
                 (self.matching_logic_op_task_measurements
                  [task_id]
                  ["workers_searched"]
                  )
                 )
            avg_workers_searched_per_task = \
                sum(
                    map(workers_searched_per_task,
                        self.matching_logic_op_task_measurements
                        )
                ) / total_task_count
            file_handler.write(f"{TColors.BOLD}Average number of workers "
                               f"searched per task ={TColors.END}"
                               f" {avg_workers_searched_per_task}\n")

            file_handler.write(Logger.LINE_SEPARATOR)

            file_handler.write(
                f"{TColors.BOLD}Log sanity checks:{TColors.END}\n\n")

            if self.has_integrity is True:
                file_handler.write(Logger.INTEGRITY_MESSAGE)
                file_handler.write(f"{TColors.SUCCESS}SUCCESS:{TColors.END} "
                                   "All Matching Logic "
                                   "Operations have all been successfully "
                                   "been accounted for!\n")
                file_handler.write(f"{TColors.SUCCESS}SUCCESS:{TColors.END} "
                                   "The measurements have covered all tasks!"
                                   "\n")
            else:
                file_handler.write(f"{TColors.FAIL}FAILURE:{TColors.END}\n")
                file_handler.write("Matching Logic Operations accounted for "
                                   f"({measured_matching_logic_ops_count}) != "
                                   "Sum of all Matching Logic Operations "
                                   "across all tasks "
                                   f"({sanity_matching_logic_ops_count})\n")
                file_handler\
                    .write("Measurements "
                           f"({len(self.matching_logic_op_task_measurements)})"
                           " != tasks_completed_count "
                           f"({self.data_points['task_end_event']})\n")

        # External inconsistency count per task has the extension "exi"
        external_inconsistency_file_count = str(self.
                                                output_file_path.
                                                resolve()).split('.')[0] + \
            "_exi.txt"
        with open(external_inconsistency_file_count, "w") as file_handler:
            for key in self.external_inconsistency_count_per_task:
                file_handler.\
                    write(f"{key} : "
                          f"{self.external_inconsistency_count_per_task[key]}"
                          "\n")

        # Internal inconsistency count per task has the extension "ini"
        internal_inconsistency_file_count = str(self.
                                                output_file_path.
                                                resolve()).split('.')[0] + \
            "_ini.txt"
        with open(internal_inconsistency_file_count, "w") as file_handler:
            for key in self.internal_inconsistency_count_per_task:
                file_handler.\
                    write(f"{key} : "
                          f"{self.internal_inconsistency_count_per_task[key]}"
                          "\n")

        # Write the worker nodes searched per task into a file
        workers_searched_file_count = str(self.
                                          output_file_path.
                                          resolve()).split('.')[0] + \
            "_workers_searched.txt"
        with open(workers_searched_file_count, "w") as fHandler:
            for key in self.matching_logic_op_task_measurements:
                count = (self.matching_logic_op_task_measurements[key]
                         ["workers_searched"])
                fHandler.write(f"{key} : {count}\n")

        JOBS_INFO_FILE_NAME = str(self.output_file_path
                                  .resolve()).split('.')[0] + \
            "_jobs_info.csv"
        with open(JOBS_INFO_FILE_NAME, "w") as f:
            HEADER_LINE = ("Job ID,"
                           "Task ID,"
                           "Job Arrival Time,"
                           "Task Launch Time,"  # launch on node event
                           "Task Duration (Trace),"  # From trace
                           # When GM realizes that task is completed
                           "Task Duration (GM),"
                           "Task Queuing Delay,"
                           "Task End Time On Node\n")
            f.write(HEADER_LINE)
            for job_id in sorted(self.queuing_delay.keys()):
                for task_id in sorted(self.queuing_delay[job_id].keys()):
                    task_info = self.queuing_delay[job_id][task_id]
                    TASK_LINE = (f"{job_id},"
                                 f"{task_id},"
                                 f"{task_info['job_arrival_time']},"
                                 f"{task_info['task_launch_time']},"
                                 f"{task_info['task_duration_trace']},"
                                 f"{task_info['task_duration_gm']},"
                                 f"{task_info['task_queuing_delay']},"
                                 f"{task_info['task_end_time_node']}\n")
                    f.write(TASK_LINE)

        JOB_COMPLETION_TIME_FILE_NAME = str(self.output_file_path
                                            .resolve()).split('.')[0] + \
            "_job_completion_time.txt"
        with open(JOB_COMPLETION_TIME_FILE_NAME, "w") as f:
            sort__job_ct = []
            # print(self.all_job_ct)

            for job_id in sorted(self.all_job_ct.keys()):
                sort__job_ct.append(self.all_job_ct[job_id])

            f.write(str(sort__job_ct))


class SimulatorLogger:
    """This class is to define and create instances of the logging class."""

    LOG_FILE_PATH = Path("logs")
    LOG_FILE_NAME = None

    is_setup: bool = False
    logger_obj: Optional[Logger] = None

    def __init__(self, _: str):
        """
        Initialise the object.

        Args:
            _ (str): This parameter will be removed in a future release.
        """
        if SimulatorLogger.is_setup is False:
            # print(f'{datetime.now().strftime("record-%Y-%m-%d-%H-%M-%S.log")}'
            # ' Hello')
            SimulatorLogger.LOG_FILE_NAME = (SimulatorLogger.LOG_FILE_PATH /
                                             datetime.now()
                                             .strftime("record-%Y-%m-%d-"
                                                       "%H-%M-%S.log"))
            SimulatorLogger.logger_obj = Logger(
                output_file_path=SimulatorLogger.LOG_FILE_NAME)

            # Makes sure that the root logger is setup only once
            SimulatorLogger.is_setup = True

    def get_logger(self) -> Logger:
        """
        Return an instance of the `Logger` class to the caller.

        Returns:
            Logger: Object used for logging runtime information.
        """
        assert SimulatorLogger.logger_obj is not None
        return SimulatorLogger.logger_obj
