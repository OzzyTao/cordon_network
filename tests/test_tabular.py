import unittest
from cordon.tabular.records import PresenceFieldsMapping, TransactionFieldsMapping, PresenceRecords, TransactionRecords
from pandas import DataFrame



class PresenceTestCase(unittest.TestCase):
    def setUp(self):
        self.field_mapping = PresenceFieldsMapping(start_time='s_t',end_time='e_t',id='id',val='value')
        df = DataFrame({'id':[1,2,3,4,1],
                        's_t':[1,2,3,4,5],
                        'e_t':[2,3,4,5,6],
                        'value':[0,3,7,2,1]})
        self.records = PresenceRecords(self.field_mapping,df)

    def test_has_id(self):
        self.assertEqual(self.field_mapping.has_identity(),True)

    def test_match_dict(self):
        raw_dict = {'id':1,'s_t':1,'e_t':2,'value':3}
        self.assertEqual(self.field_mapping.is_compatible_with(raw_dict),True)

    def test_match_dataframe(self):
        df = DataFrame({'id':[1,2,3,4,1],
                        's_t':[1,2,3,4,5],
                        'e_t':[2,3,4,5,6],
                        'value':[0,3,7,2,1]})
        self.assertEqual(self.field_mapping.match_dataframe(df),True)

    def test_field_translate(self):
        self.assertEqual(self.field_mapping.translate('start_time'),'s_t')
        self.assertEqual(self.field_mapping.translate('val'),'value')

    def test_add_feature(self):
        self.records.add_feature('test',data=[0]*5)
        self.assertEqual(self.field_mapping.translate('test'),'test')
        self.records.add_feature('period_length',func=lambda v:v['e_t']-v['s_t'],role='length')
        self.assertEqual(self.field_mapping.translate('length'),'period_length')


class TransactionTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
