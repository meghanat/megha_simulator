"""
Count the number of operations taken by the 'Matching Logic'.

Script to count the number of search operations taken to successfully or \
otherwise find a free worker node in the cluster, during the simulation.

Raises:
    FileNotFoundError: This exception is raised when the file, passed in \
    through the command line, does not exist.
"""
import os
import sys
from pathlib import Path
from typing import List, NamedTuple, Dict, TypedDict


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


class LogLineType(NamedTuple):
    """
    Format and types of each of the attributes logged, every line.

    Args:
        NamedTuple (NamedTuple): Typed version of collections.namedtuple().
    """

    timestamp: str
    module_name: str
    filename: str
    function_name: str
    line_no: str
    level_name: str
    message: str


class MatchingOps(TypedDict):
    """
    Format and types of each of the parameters measured per task.

    Args:
        NamedTuple (NamedTuple): Typed version of collections.namedtuple().
    """
    job_id: str
    task_id: str
    workers_searched: int


measurements: Dict[str, MatchingOps] = dict()


def parse_matching_logic_stmt(matching_logic_details: List[str]):
    """
    Parse and record details of the Matching Logic logs.

    Args:
        matching_logic_details (List[str]): List of the details to record.
    """
    # gm_id, lm_id, node_id = matching_logic_details[0].split("_")
    job_id, task_id = matching_logic_details[1].split("_")
    key: str = f"{job_id}_{task_id}"

    assert len(matching_logic_details[1].split("_")) == 2

    if key in measurements:
        measurements[key]["workers_searched"] += 1
    else:
        measurements[key] = MatchingOps(job_id=job_id,
                                        task_id=task_id,
                                        workers_searched=1)


FULL_LOG_PATH = Path("./logs") / sys.argv[1]
LOG_LINE_COLS: List[str] = ["timestamp", "module_name",
                            "filename", "function_name", "line_no",
                            "level_name", "message"]

if os.path.isfile(FULL_LOG_PATH) is False:
    raise FileNotFoundError(f"The file: {FULL_LOG_PATH} does not exist!")

matching_logic_ops_count: int = 0
tasks_completed_count: int = 0

with open(FULL_LOG_PATH) as file_handler:
    for line in file_handler:
        logged_line = LogLineType(*(line.strip().split(" : ")))
        if logged_line.message.startswith("Checking worker node") is True:
            matching_logic_ops_count += 1
            matching_logic_details: List[str] = (logged_line
                                                 .message
                                                 .split(" , ")[1:])
            parse_matching_logic_stmt(matching_logic_details)
        elif logged_line.message.split(" , ")[1] == "TaskEndEvent":
            tasks_completed_count += 1

print(f"Matching logic operations taken: {TColors.BOLD}"
      f"{matching_logic_ops_count}{TColors.END}")
print(f"Number of tasks completed: {TColors.BOLD}"
      f"{tasks_completed_count}{TColors.END}")

success_percent = sum(map(lambda task_id: 1 / (measurements[task_id]
                                               ["workers_searched"]),
                          measurements)) / len(measurements.keys())

print(f"{success_percent=:%}")

print()
print("-"*80)
print(f"{TColors.BOLD}Log sanity checks:{TColors.END}")
print()


class InvalidResultException(Exception):
    """
    Exception raised when any sanity checks on the log results fails.

    Args:
        Exception (Exception): Base Exception class.
    """

    def __init__(self, message: str) -> None:
        """
        Initialise the exception class with the Exception message.

        The message is only meant to send to the parent class `Exception` \
        which contains the `__str__` method to print the Exception message.

        Args:
            message (str): Exception message to provide to the user.
        """
        self.message = message
        super().__init__(message)


if len(measurements) != tasks_completed_count:
    raise InvalidResultException(f"measurements ({len(measurements)}) != "
                                 "tasks_completed_count "
                                 f"({tasks_completed_count})")
else:
    print(f"{TColors.SUCCESS}SUCCESS:{TColors.END} The measurements have"
          " covered all tasks!")


sanity_matching_logic_ops_count: int = 0
for key in measurements:
    sanity_matching_logic_ops_count += measurements[key]["workers_searched"]

if matching_logic_ops_count != sanity_matching_logic_ops_count:
    raise InvalidResultException("Matching Logic Operations accounted for "
                                 f"({matching_logic_ops_count}) != "
                                 "Sum of all Matching Logic Operations "
                                 "across all tasks "
                                 f"({sanity_matching_logic_ops_count})")
else:
    print(f"{TColors.SUCCESS}SUCCESS:{TColors.END} All Matching Logic "
          "Operations have all been successfully been accounted for!")
