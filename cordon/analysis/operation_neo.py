from pandas import DataFrame

class Operation:
    def __init__(self, func):
        self.func = func

    def _getElements(self, graph, filter_str):
        return DataFrame(graph.query(filter_str))

    def applyTo(self, graph, filter_str, group_by):
        records = self._getElements(graph,filter_str)
        groups = records.groupby(group_by)
        for name, group in groups:
            value = self.func(group)


class LocalOperation(Operation):
    def _getElements(self, graph, filter_str, group_by):
         df = DataFrame(graph.query(filter_str))

