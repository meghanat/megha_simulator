"""
Program to run the simulator based on the parameters provided by the user.

This program uses the `megha_sim` module to run the simulator as per the
Megha architecture and display/log the actions and results of the simulation.
"""

import os
import sys
import time
from typing_extensions import Final
import cProfile
import pstats
import io
from pstats import SortKey


from megha_sim import Simulation, simulator_globals
from megha_sim import SimulatorLogger

if __name__ == "__main__":
    WORKLOAD_FILE: Final[str] = sys.argv[1]
    CONFIG_FILE: Final[str] = sys.argv[2]
    NUM_GMS: Final[int] = int(sys.argv[3])
    NUM_LMS: Final[int] = int(sys.argv[4])
    PARTITION_SIZE: Final[int] = int(sys.argv[5])
    # currently set to 1 because of comparison with Sparrow
    SERVER_CPU: Final[float] = float(sys.argv[6])
    SERVER_RAM: Final[float] = float(sys.argv[7])  # ditto
    SERVER_STORAGE: Final[float] = float(sys.argv[8])  # ditto

    WORKLOAD_FILE_NAME: Final[str] = (os.path.basename(WORKLOAD_FILE)
                                      .split(".")[0])

    logger = SimulatorLogger(__name__).get_logger()

    logger.metadata(f"Analysing logs for trace file: {WORKLOAD_FILE_NAME}")
    logger.metadata(f"Number of GMs: {NUM_GMS}")
    logger.metadata(f"Number of LMs: {NUM_LMS}")
    logger.metadata(f"Number of Partition Size: {PARTITION_SIZE}")

    logger.metadata("Simulator Info , Received CMD line arguments.")

    NETWORK_DELAY = 0.0005  # same as sparrow

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_GMS, NUM_LMS,
                   PARTITION_SIZE, SERVER_CPU, SERVER_RAM, SERVER_STORAGE)
    # print("Simulator Info , Simulation running")
    logger.metadata("Simulator Info , Simulation running")
    pr = cProfile.Profile()
    pr.enable()
    s.run()
    pr.disable()
    text = io.StringIO()
    sortby = SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=text).sort_stats(sortby)
    ps.print_stats()
    print(text.getvalue())
    # s.run()
    time_elapsed = time.time() - t1
    # print("Simulation ended in ", time_elapsed, " s ")
    logger.metadata(f"Simulation ended in {time_elapsed} s ")

    print(simulator_globals.jobs_completed)

    # print(f"Number of Jobs completed: {len(simulator_globals.jobs_completed)}")
    logger.metadata(
        "Simulator Info , Number of Jobs completed: "
        f"{len(simulator_globals.jobs_completed)}")

    logger.integrity()
    logger.flush()
