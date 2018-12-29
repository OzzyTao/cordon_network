import unittest
from cordon.network import CordonNetwork
from cordon.tabular.records import PresenceFieldsMapping, TransactionFieldsMapping
from pandas import DataFrame

class CordonNetworkTestCase(unittest.TestCase):
    def setUp(self):
        self.network = CordonNetwork()
        self.network.add_node('1')
        self.network.add_node('2')
        self.network.add_edge('1','2')
        node_field_mapping = PresenceFieldsMapping(id='id',start_time='s_t',end_time='e_t',val='value')
        df = DataFrame({'id':[1,2],'s_t':[1,2],'e_t':[2,3],'value':[1,1]})
        self.network.set_node_records('1',node_field_mapping,records_df=df,static_data={'region':'A'})
        df2 = DataFrame({'id':[1,2],'s_t':[2,3],'e_t':[3,4],'value':[1,1]})
        self.network.set_node_records('2',node_field_mapping,records_df=df2,static_data={'region':'B'})
        # edge_field_mapping = TransactionFieldsMapping(id='id',time='t')
        # df3 = DataFrame({'id':[1,2],'t':[2,3]})
        # self.network.set_edge_records(('1','2'),edge_field_mapping,records_df=df3)

    def test_something(self):
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
