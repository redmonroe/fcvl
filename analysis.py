import pandas as pd
from backend import PopulateTable
from pprint import pprint

class Analysis:
    
    def __init__(self, 
                 db=None, 
                 tables=None
                 ):
        self.db = db
        self.tables = tables
        self.finalmonth = self.tables[0]
        self.populate = PopulateTable()
        self.df = None
        
    def print_df(self):
        pprint(self.df)
        
        
    def sum_over_range_by_type(self, column=None, period_strt=None, period_end=None):
        first_dt, _ = self.populate.make_first_and_last_dates(date_str=period_strt)
        _, last_dt = self.populate.make_first_and_last_dates(date_str=period_end)

        df = pd.DataFrame([row for row in self.finalmonth.select().
                           where(self.finalmonth.month >= first_dt).
                           where(self.finalmonth.month <= last_dt).namedtuples()
                           ])
        df[column] = pd.to_numeric(df[column], errors='coerce')
        self.df = df.groupby('month', as_index=False).agg({column: 'sum',})