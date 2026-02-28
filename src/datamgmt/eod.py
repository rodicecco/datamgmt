import pandas as pd
from . import source
from .admin import Database
import re
from datetime import date, datetime, timedelta, timezone
import time
from zoneinfo import ZoneInfo


#Update set that only takes tickers in the indexes listed above
def priority_update_set():
    resp = ['VVIX.INDX', 'VIX.INDX', 'VIX1D.INDX', 'GSPC.INDX','MOVE.INDX','VIX9D.INDX','VIX3M.INDX', 'SKEW.INDX','SPY']
    return resp


rem_ints = lambda x: re.sub(r'\d+', '', x)

#Function to move any integers to the end of the string when creating
#a sql table (Avoids conflict)
def move_integers_to_end(input_string):
    # Use regular expressions to find the leading integers
    match = re.match(r'(\d+)(.*)', input_string)
    if match:
        numbers = match.group(1)
        rest_of_string = match.group(2)
        # Concatenate the rest of the string with the leading integers at the end
        result = rest_of_string + numbers
    else:
        result = input_string
    return result


#Function to convert date to UTC unix timestamp
def date_convert_in(date_str:str):
    dt_est = datetime.fromisoformat(date_str)
    dt_est = dt_est.replace(tzinfo=ZoneInfo("America/New_York"))
    dt_utc = dt_est.astimezone(ZoneInfo("UTC"))
    unix_timestamp = int(dt_utc.timestamp())
    print(dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f"), unix_timestamp)
    return unix_timestamp


#Function to convert UTC unix timestamp into EST datetime format
def date_convert_out(unix_timestamp: int):
    dt_utc = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    dt_est = dt_utc.astimezone(ZoneInfo("America/New_York"))
   
    return pd.to_datetime(dt_est)


#Class to organize and post to database intraday price data
#for instruments in the EODData api
#NOTE: DESIGN SO IT CAN BE USED FOR INDEXES AS WELL AS STOCKS/ETFS
class Intraday(Database):

    def __init__(self, symbol, from_date='2022-05-16 00:00'):
        self.symbol = symbol
        self.source = source.EODData()
        self.table_name = 'intraday'
        self.from_date = from_date
        self.to_date = datetime.now()
        self.constraints = ['datetime', 'symbol']
        self.max_req = 100
        

        Database.__init__(self, self.table_name, self.constraints)

    def update_date(self):
        try:
            query = f'''SELECT MAX(datetime) AS max_datetime
                                        FROM {self.table_name}
                                        WHERE symbol = '{self.symbol}';'''
            with self.connection() as conn:
                cur = conn.cursor()
                cur.execute(query)
                resp = cur.fetchone()


            return resp[0].strftime("%Y-%m-%d %H:%M:%S.%f")
        except:
            #If table does not exist, return a very old date
            return self.from_date

    def get_data_part(self, from_date, to_date):
        obj = self.source
        from_date = date_convert_in(from_date)
        now_str = to_date
        to_date = date_convert_in(now_str)
        data = obj.intraday([self.symbol], interval="1m" ,from_date=from_date, to=to_date)
        frame = pd.DataFrame(data)
        frame['datetime'] = [date_convert_out(x) for x in frame.timestamp]
        frame.set_index('datetime', inplace=True)
        return frame[['close','open', 'high', 'low', 'volume']]


    def data(self, **kwargs):
        from_date = self.update_date()
        to_date = self.to_date

        intervals = pd.date_range(start=from_date, end=to_date, freq=f'{self.max_req}D')
        intervals = intervals.to_list()
        intervals = [x.strftime("%Y-%m-%d %H:%M:%S.%f") for x in intervals]
        intervals.append(to_date.strftime("%Y-%m-%d %H:%M:%S.%f"))
        data_lis = []
        

        for date_init, date_end in zip(intervals, intervals[1:]):
            data = self.get_data_part(date_init, date_end)
            data_lis.append(data)
        
        frame = pd.concat(data_lis)
        frame['symbol'] = self.symbol
        frame = frame.tz_localize(None)
        frame = frame.reset_index()
        

        self.data_ = frame
        self.raw_data = self.data_.to_dict(orient='records')
        
        cols = list(frame.columns)

        self.columns = frame[cols].columns
        self.dtypes = frame.dtypes.items()
        

        return frame

    #Function to run updates on the set specified
    def update_sequence(self):
        self.data()
        self.create_table()
        self.upsert_async()

        return True

#Class to organize and post to database historical price data
#for instruments in the EODData api
class Historical(Database):

    def __init__(self, symbols:list, from_date='1900-02-01'):
        self.source = source.EODData()
        self.endpoint = self.source.historical
        self.table_name = 'historical'
        self.constraints = ['date', 'symbol']
        self.symbols = symbols
        self.from_date = from_date


        self.ct = 0
        self.limit = 80
        self.sleep_time = 10
        self.sleep_ct = 3

        Database.__init__(self, self.table_name, self.constraints)

    def prep_raw(self, raw_data):
        diclis = []
        for _ in raw_data:
            for entry in raw_data[_]:
                temp = entry
                temp['symbol'] = _
                diclis.append(temp)
        return diclis
            
    #Function that creates dataframe and cleans data for final
    #posting in the database
    def data(self, symbols:list, filter:str=False, **kwargs):
        raw_data = self.source.historical(symbols, **kwargs)
        self.raw_data = self.prep_raw(raw_data)

        frame = pd.DataFrame()

        for _ in raw_data:
            temp = pd.DataFrame(raw_data[_])
            temp['symbol'] = _
            frame = pd.concat([frame, temp])

        if filter == False:
            frame = frame
        else:
            frame = frame.groupby(self.constraints).last()[filter]
            frame = frame.unstack('symbols').reset_index()
        
        self.data_ = frame
        
        cols = list(frame.columns)
        cols.remove('symbol')
        cols.append('symbol')

        self.columns = frame[cols].columns
        self.dtypes = frame.dtypes.items()

        return frame
    
    #Function to run updates on the set specified
    def update_sequence(self):
        symbols = self.symbols
        from_date = self.from_date

        steps = self.limit

        ct=self.ct
        sleep_ct = 0
        while ct <= len(symbols):
            symbols_set = symbols[ct:min(ct+steps, len(symbols))]
            print(ct, ct+steps)
            self.data(symbols_set, from_date=from_date)
            self.create_table()
            self.upsert_exec()

            ct+=steps
            sleep_ct+=1
            if sleep_ct == self.sleep_ct:
                time.sleep(self.sleep_time)
                sleep_ct = 0

        return True

