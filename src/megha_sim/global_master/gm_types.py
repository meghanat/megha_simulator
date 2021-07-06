from typing import List, Dict, NamedTuple, TypedDict


class NodeResources(TypedDict):
    """
    Typed dictionary class to describe the data format of each node resource.

    Args:
        TypedDict (TypedDict): TypedDict base class.
    """

    CPU: int
    RAM: int
    Disk: int
    constraints: List[int]


class PartitionResources(TypedDict):
    partition_id: str
    nodes: Dict[str, NodeResources]


class OrganizedPartitionResources(TypedDict):
    lm_id: str
    partition_id: str
    free_nodes: Dict[str, NodeResources]
    busy_nodes: Dict[str, NodeResources]


class LMResources(TypedDict):
    LM_id: str
    partitions: Dict[str, PartitionResources]


class ConfigFile(TypedDict):
    LMs: Dict[str, LMResources]


class PartitionKey(NamedTuple):
    gm_id: str
    lm_id: str
