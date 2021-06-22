import sys
import time

from simulation.simulation import Simulation
from simulator_utils.globals import jobs_completed

#################MAIN########################

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

    NETWORK_DELAY = 0.0005  # same as sparrow

    # not simulation's virtual time. This is just to understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_GMS, NUM_LMS,
                   PARTITION_SIZE, SERVER_CPU, SERVER_RAM, SERVER_STORAGE)
    print("Simulation running")
    s.run()
    print("Simulation ended in ", (time.time() - t1), " s ")

    print(jobs_completed)
