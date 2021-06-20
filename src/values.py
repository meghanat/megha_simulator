from typing import Final, Literal
from enum import Enum, unique


@unique
class InconsistencyType(Enum):
	INTERNAL_INCONSISTENCY = Literal[0]
	EXTERNAL_INCONSISTENCY = Literal[1]

###############################################################################

LM_HEARTBEAT_INTERVAL=30
NETWORK_DELAY = 0.0005  # Same as the Sparrow simulator

###############################################################################

class TaskDurationDistributions:
    CONSTANT : Final[int]
    MEAN : Final[int]
    FROM_FILE : Final[int]
    CONSTANT, MEAN, FROM_FILE  = range(3)
class EstimationErrorDistribution:
    CONSTANT : Final[int]
    RANDOM : Final[int]
    MEAN : Final[int]
    CONSTANT, RANDOM, MEAN = range(3)
