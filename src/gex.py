import pandas as  pd
from .admin import Database


class DIX(Database):
    def __init__(self):
        self.table_name = 'dix'
        self.constraints = ['date']
        
        Database.__init__(self, self.table_name, self.constraints)

    def data(self):
        data = pd.read_csv("https://squeezemetrics.com/monitor/static/DIX.csv")
        data['date'] = pd.to_datetime(data['date'])
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