from typing import (List, Dict, NamedTuple, Optional,
                    Tuple, TypedDict, Generic,
                    TypeVar)


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


FreeSlotsCount = int


KT = TypeVar('KT')
VT = TypeVar('VT')


class MySortedDict(Generic[KT, VT]):
    def __init__(self, key) -> None:
        ...

    def peekitem(self, index: int) -> Tuple[KT, VT]:
        ...

    def get(self, key: KT) -> Optional[VT]:
        ...

    def __setitem__(self, key: KT, value: VT) -> None:
        ...

    def __getitem__(self, key: KT) -> VT:
        ...

    def __delitem__(self, key: KT) -> None:
        ...


OrderedPartition = MySortedDict[FreeSlotsCount,
                                Dict[PartitionKey,
                                     OrganizedPartitionResources]]
