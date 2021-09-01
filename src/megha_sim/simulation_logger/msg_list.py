"""File containing common messages entered in logging statements."""
from typing_extensions import Final


MATCHING_LOGIC_MSG: Final[str] = "Searching worker node."
MATCHING_LOGIC_REPARTITION_MSG: Final[str] = ("Searching worker node via"
                                              " `RepartitionEvent`.")
CLUSTER_SATURATED_MSG: Final[str] = "No resources available in the cluster."
