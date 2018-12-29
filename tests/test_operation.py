import unittest
from cordon.analysis.operation import LocalOperation, ZonalOperation, FocalOpeartion
from cordon.network import CordonNetwork
from cordon.tabular.records import PresenceFieldsMapping, TransactionFieldsMapping
from pandas import DataFrame

class LocalOperationTestCase(unittest.TestCase):
    def setUp(self):
        self.network = CordonNetwork()
        self.network.add_node('1')
        self.network.add_node('2')
        self.network.add_edge('1', '2')
        node_field_mapping = PresenceFieldsMapping(id='id', start_time='s_t', end_time='e_t', val='value')
        df = DataFrame({'id': [1, 2], 's_t': [1, 2], 'e_t': [2, 3], 'value': [1, 1]})
        self.network.set_node_records('1', node_field_mapping, records_df=df, static_data={'region': 'A'})
        df2 = DataFrame({'id': [1, 2], 's_t': [2, 3], 'e_t': [3, 4], 'value': [1, 1]})
        self.network.set_node_records('2', node_field_mapping, records_df=df2, static_data={'region': 'B'})

    def test_local_operation(self):
        def summary(node_record_list,edge_record_list, local_data):
            df = local_data.data.df
            return df.assign(plusone=df['value']+1)
        local_operation = LocalOperation(summary)
        self.assertEqual(local_operation.get_input_elements(self.network,focus_node='1'),(['1',],[]))
        node_field_mapping = PresenceFieldsMapping(id='id', start_time='s_t', end_time='e_t', val='value', val2='plusone')
        local_operation.apply(self.network, in_place=True,roles=node_field_mapping)
        value = self.network.get_node_records('1').data.df['plusone'].iloc[0]
        self.assertEqual(value,2)

    def test_focal_operation(self):
        def transaction(node_record_list,edge_record_list,local_data):
            result= []
            pending_rows = []
            for node_records in node_record_list:
                for index, row in node_records.iterrows():
                    found = False
                    for i,r in enumerate(pending_rows):
                        if r['id'] == row['id']:
                            if r['s_t'] == row['e_t']:
                                result.append({'id':r['id'],'time':r['s_t']})
                                pending_rows.pop(i)
                                found= True
                                break
                            elif r['e_t']==row['s_t']:
                                result.append({'id':r['id'],'time':r['e_t']})
                                pending_rows.pop(i)
                                found = True
                                break
                    if not found:
                        pending_rows.append(row)
            return DataFrame(result)
        focal_operation = FocalOpeartion(transaction,1)
        self.assertEqual(focal_operation.get_input_elements(self.network,focus_edge=('1','2')),['1','2'])
        node_results, edge_results = focal_operation.apply(self.network, on_edge=True, on_node=False, in_place=False)
        self.assertEqual(len(node_results),0)
        self.assertEqual(len(edge_results),1)












if __name__ == '__main__':
    unittest.main()
