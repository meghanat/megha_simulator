from typing import Final, Literal
from enum import Enum, unique


@unique
class InconsistencyType(Enum):
    INTERNAL_INCONSISTENCY = Literal[0]
    EXTERNAL_INCONSISTENCY = Literal[1]

###############################################################################


LM_HEARTBEAT_INTERVAL = 30
NETWORK_DELAY = 0.0005  # Same as the Sparrow simulator

###############################################################################


class TaskDurationDistributions:
    CONSTANT: Final[int] = 0
    MEAN: Final[int] = 1
    FROM_FILE: Final[int] = 2
    # CONSTANT, MEAN, FROM_FILE = range(3)


class EstimationErrorDistribution:
    CONSTANT: Final[int] = 0
    RANDOM: Final[int] = 1
    MEAN: Final[int] = 2
    # CONSTANT, RANDOM, MEAN = range(3)
