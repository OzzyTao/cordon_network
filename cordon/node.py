from .tabular.records import PresenceRecords, Records, PresenceFieldsMapping

class DataWrapper(object):
    def __init__(self, movement_records, static_data=None):
        if not isinstance(movement_records, Records):
            raise Exception('PresenceRecords should be provided to a node')
        self.data = movement_records
        self.static_data = static_data

    def get_records_individual(self, id_value):
        return self.data.id_filter(id_value)

    def add_record(self, row_dict):
        self.data.add_row_dict(row_dict)

class NodeDataWrapper(DataWrapper):
    def __init__(self, movement_records, static_data=None):
        if not isinstance(movement_records, PresenceRecords):
            raise Exception('PresenceRecords should be provided to a node')
        super().__init__(movement_records,static_data)
