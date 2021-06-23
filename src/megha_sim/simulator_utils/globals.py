"""
This file contains the global variables used in the simulator.

The global variables are used to store the list of jobs completed
as well as the start times of each of the jobs given as input to
the simulator from the trace file.
"""

from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from job import Job

job_start_tstamps: Dict[float, float] = {}
jobs_completed: List[Job] = []
