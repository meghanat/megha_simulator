"""
Program to run the simulator based on the parameters provided by the user.

This program uses the `megha_sim` module to run the simulator as per the
Megha architecture and display/log the actions and results of the simulation.
"""

import os
import sys
import time
from typing import Final

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

    logger.info("Simulator Info , Creating logs for trace file: "
                f"{WORKLOAD_FILE_NAME}")
    logger.info("Simulator Info , Received CMD line arguments.")

    NETWORK_DELAY = 0.0005  # same as sparrow

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_GMS, NUM_LMS,
                   PARTITION_SIZE, SERVER_CPU, SERVER_RAM, SERVER_STORAGE)
    print("Simulator Info , Simulation running")
    logger.info("Simulator Info , Simulation running")
    s.run()
    time_elapsed = time.time() - t1
    print("Simulation ended in ", time_elapsed, " s ")
    logger.info(f"Simulator Info , Simulation ended in {time_elapsed} s ")

    print(simulator_globals.jobs_completed)
    logger.info(f"Simulator Info , {simulator_globals.jobs_completed=}")

    print(f"Number of Jobs completed: {len(simulator_globals.jobs_completed)}")
    logger.info(
        "Simulator Info , Number of Jobs completed: "
        f"{len(simulator_globals.jobs_completed)}")
