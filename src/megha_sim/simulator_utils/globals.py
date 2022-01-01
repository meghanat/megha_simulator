"""
This file contains the global variables used in the simulator.

The global variables are used to store the list of jobs completed
as well as the start times of each of the jobs given as input to
the simulator from the trace file.
"""

from __future__ import annotations
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from job import Job

jobs_completed: List[Job] = []
DEBUG_MODE: bool = not False
