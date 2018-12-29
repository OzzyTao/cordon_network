from .tabular.records import TransactionRecords
from .node import DataWrapper
class EdgeDataWrapper(DataWrapper):
    def __init__(self, movement_records, static_data=None):
        if not isinstance(movement_records, TransactionRecords):
            raise Exception('PresenceRecords should be provided to a node')
        super().__init__(movement_records,static_data)
