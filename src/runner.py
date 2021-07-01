"""
Program to run the simulator based on the parameters provided by the user.

This program uses the `megha_sim` module to run the simulator as per the
Megha architecture and display/log the actions and results of the simulation.
"""

import sys
import time

from megha_sim import Simulation, simulator_globals
from megha_sim import SimulatorLogger

if __name__ == "__main__":
    WORKLOAD_FILE = sys.argv[1]
    CONFIG_FILE = sys.argv[2]
    NUM_GMS = int(sys.argv[3])
    NUM_LMS = int(sys.argv[4])
    PARTITION_SIZE = int(sys.argv[5])
    # currently set to 1 because of comparison with Sparrow
    SERVER_CPU = float(sys.argv[6])
    SERVER_RAM = float(sys.argv[7])  # ditto
    SERVER_STORAGE = float(sys.argv[8])  # ditto

    logger = SimulatorLogger(__name__).get_logger()
    logger.info("Received CMD line arguments.")

    NETWORK_DELAY = 0.0005  # same as sparrow

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_GMS, NUM_LMS,
                   PARTITION_SIZE, SERVER_CPU, SERVER_RAM, SERVER_STORAGE)
    print("Simulation running")
    s.run()
    print("Simulation ended in ", (time.time() - t1), " s ")

    # print(simulator_utils.globals.jobs_completed)
    print(len(simulator_globals.jobs_completed))
