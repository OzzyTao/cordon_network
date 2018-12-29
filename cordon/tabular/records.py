from pandas import DataFrame
import pandas as pd

class DataConfigurationError(Exception):
    pass

class ParameterError(Exception):
    pass

class ColumnMissing(Exception):
    pass

class FieldsMapping(object):
    MANDATORY_FIELDS = []
    SEMANTIC_FIELDS = ['id']
    def __init__(self,**kwargs):
        self.name_map = {}
        self.name_map.update(kwargs)
        provided_fields = kwargs.keys()
        for field in self.MANDATORY_FIELDS:
            if field not in provided_fields:
                raise DataConfigurationError('please specify the column name for '+field)

    def has_identity(self):
        return 'id' in self.name_map

    def match_dataframe(self, df):
        fields = self.name_map.values()
        return set(fields).issubset(set(list(df)))

    def translate(self, role):
        if role in self.name_map:
            return self.name_map[role]
        else:
            raise ColumnMissing('column {} does not exist'.format(role))

    def __eq__(self, other):
        return self.name_map == other.name_map

    def is_compatible_with(self, raw_record):
        allowed_fields = set(self.name_map.values())
        provided_fields = set(raw_record.keys())
        return allowed_fields==provided_fields


class PresenceFieldsMapping(FieldsMapping):
    MANDATORY_FIELDS = ['start_time', 'end_time']
    def add_field(self, role, name=None):
        dict_copy = dict(self.name_map)
        if name:
            dict_copy.update({role:name})
        else:
            dict_copy.update({role:role})
        return PresenceFieldsMapping(**dict_copy)


class TransactionFieldsMapping(FieldsMapping):
    MANDATORY_FIELDS = ['time']
    def add_field(self, role, name=None):
        dict_copy = dict(self.name_map)
        if name:
            dict_copy.update({role: name})
        else:
            dict_copy.update({role: role})
        return TransactionFieldsMapping(**dict_copy)


class Records(object):
    def __init__(self, mapping, table_df):
        self.df = table_df
        self.fields_mapping = mapping

    def id_filter(self, id_value):
        if self.fields_mapping.has_identity():
            return Records(self.df[self.fields_mapping['id']==id_value],self.fields_mapping)
        else:
            raise ColumnMissing('No column for id')

    def add_feature(self, name, data=None, func=None, role=None):
        if data:
            self.df.assign(**{name:data})
        elif func and callable(func):
            self.df[name] = self.df.apply(func,axis=1)
        else:
            raise ParameterError('either data or func has to be provided')
        if role:
            self.fields_mapping.name_map[role] = name
        else:
            self.fields_mapping.name_map[name] = name

    def update(self, mapping, df):
        self.df = df
        self.fields_mapping = mapping

    def add_row_dict(self, row_dict):
        if self.fields_mapping.is_compatible_with(row_dict):
            if self.df:
                self.df = self.df.append(row_dict)
            else:
                self.df = pd.DataFrame([row_dict])


class PresenceRecords(Records):
    def __init__(self,mapping, table_df=None):
        if table_df and not isinstance(table_df, DataFrame):
            raise ParameterError('First parameter should be a DataFrame instance')
        if not isinstance(mapping, PresenceFieldsMapping):
            raise ParameterError('Second parameter should be a PresenceFieldsMapping instance')
        if not mapping.match_dataframe(table_df):
            raise ParameterError('Dataframe and the provided fields do not match')
        super().__init__(table_df,mapping)



class TransactionRecords(Records):
    def __init__(self, mapping, table_df=None):
        if table_df and not isinstance(table_df, DataFrame):
            raise ParameterError('First parameter should be a DataFrame instance')
        if not isinstance(mapping, TransactionFieldsMapping):
            raise ParameterError('Second parameter should be a TransactionFieldsMapping instance')
        if not mapping.match_dataframe(table_df):
            raise ParameterError('Dataframe and the provided fields do not match')
        super().__init__(table_df,mapping)

