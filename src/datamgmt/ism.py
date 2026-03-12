import pandas as pd
import numpy as np
from .admin import Database



class ISM(Database):
    def __init__(self):
        self.table_name = 'econ_hist'
        self.constraints = ['date', 'id']
        
        Database.__init__(self, self.table_name, self.constraints)

    def data(self):
        mfg_data = pd.read_excel('C:/Users/rodic/Scripts/0.workbench/z.support/ISM Manufacturing.xlsx')
        series_ids = ['date', 'ISMMFGINDX', 'ISMMFGNEW', 'ISMMFGPRICE', 'ISMMFGEMP']
        mfg_data.columns = series_ids

        serv_data = pd.read_excel('C:/Users/rodic/Scripts/0.workbench/z.support/ISM Services.xlsx')
        series_ids = ['date', 'ISMSERVINDX', 'ISMSERVNEW', 'ISMSERVPRICE', 'ISMSERVEMP']
        serv_data.columns = series_ids

        data = mfg_data.merge(serv_data, on='date', how='left')

        data = data.set_index('date').stack().reset_index()
        data = data.sort_values(by=['level_1', 'date'])
        data.columns = ['date', 'id', 'value']
        data['realtime_start'] = ['' for x in data.date]
        data['realtime_end'] = ['' for x in data.date]

        data = data.dropna(subset=['value'])
        data = data[['id', 'realtime_start', 'realtime_end', 'date', 'value']]
        data['date'] = data['date'].astype('str')
        

        self.data_ = data
        self.raw_data = self.data_.to_dict(orient='records')

        cols = list(data.columns)

        self.columns = data[cols].columns
        self.dtypes = data.dtypes.items()

        return data
    
    def update_sequence(self):
        self.data()
        self.create_table()
        self.upsert_async()

        return True

class ISMSeriesMeta(Database):
    def __init__(self):
        self.table_name = 'econ_series_meta'
        self.constraints = ['id']
        
        Database.__init__(self, self.table_name, self.constraints)

    def data(self):
        series_data = [
            {'id':'ISMMFGINDX', 'title':'ISM Manufacturing Index'},
            {'id':'ISMMFGNEW', 'title':'ISM Manufacturing New Orders'},
            {'id':'ISMMFGPRICE', 'title':'ISM Manufacturing Price'},
            {'id':'ISMMFGEMP', 'title':'ISM Manufacturing Employment'},
            {'id':'ISMSERVINDX', 'title':'ISM Services Index'},
            {'id':'ISMSERVNEW', 'title':'ISM Services New Orders'},
            {'id':'ISMSERVPRICE', 'title':'ISM Services Price'},
            {'id':'ISMSERVEMP', 'title':'ISM Services Employment'}
            ]

        data = pd.DataFrame(series_data)
        columns = {
                    'realtime_start':'str', 
                    'realtime_end':'str', 
                    'observation_start':'str', 
                    'observation_end':'str', 
                    'frequency':'str', 
                    'units':'str', 
                    'seasonal_adjustment':'str', 
                    'last_updated':'str', 
                    'popularity':'int', 
                    'notes':'str'
                }

        for column in columns.keys():
            if columns[column] == 'str':
                data[column] = ['' for x in data.id]
            else:
                data[column] = [0 for x in data.id]


        data = data[[
                    'id', 
                    'title',
                    'realtime_start', 
                    'realtime_end', 
                    'observation_start', 
                    'observation_end', 
                    'frequency', 
                    'units', 
                    'seasonal_adjustment', 
                    'last_updated', 
                    'popularity', 
                    'notes' 
                    ]]
        
        self.data_ = data 
        self.raw_data = self.data_.to_dict(orient='records')

        cols = list(data.columns)

        self.columns = data[cols].columns
        self.dtypes = data.dtypes.items()

        return data                      

    def update_sequence(self):
        self.data()
        self.create_table()
        self.upsert_async()

        return True


class ISMSeriesRelease(Database):
    def __init__(self):
        self.table_name = 'econ_series_release'
        self.constraints = ['series_id']
        
        Database.__init__(self, self.table_name, self.constraints)

    def data(self):
        series_data = [
            {'series_id':'ISMMFGINDX', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMMFGNEW', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMMFGPRICE', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMMFGEMP', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMSERVINDX', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMSERVNEW', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMSERVPRICE', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'},
            {'series_id':'ISMSERVEMP', 'name': 'Institute for Supply Management', 'link':'https://www.ismworld.org/'}
            ]

        data = pd.DataFrame(series_data)
        columns = {
                    'id':'int', 
                    'press_release':'bool'
                }

        for column in columns.keys():
            if columns[column] == 'str':
                data[column] = ['' for x in data.series_id]
            elif columns[column] == 'bool':
                data[column] = [False for x in data.series_id]
            else:
                data[column] = [0 for x in data.series_id]


        data = data[[
                    'id', 
                    'name',
                    'press_release', 
                    'link',
                    'series_id'
                    ]]
        
        self.data_ = data 
        self.raw_data = self.data_.to_dict(orient='records')

        cols = list(data.columns)

        self.columns = data[cols].columns
        self.dtypes = data.dtypes.items()

        return data                      

    def update_sequence(self):
        self.data()
        self.create_table()
        self.upsert_async()

        return True

    
