from config import Config
from calendar import monthrange
import dataset
import pandas as pd
from datetime import datetime
from dateutil.parser import parse

class Checklist:

    def __init__(self, db=None):
        if db == None:
            self.db = Config.test_checklist_db
        else:
            self.db = db
        
        self.tablename = 'checklist'
        self.init_status = None

    def check_cl_exist(self):
        months_ytd = self.limit_date()
        items_in_db = self.show_checklist()

        if len(items_in_db) == 0: # cl is empty
            self.init_status = 'empty_db'
        elif len(items_in_db) > len(months_ytd):
            self.init_status = 'issue: double entry?, reset?'
        elif len(items_in_db) == len(months_ytd):
            self.init_status = 'proceed'
        else:
            self.init_status = 'other'

        return self.init_status

    def limit_date(self):
        range_month = datetime.now().strftime('%m')
        str_month = datetime.now().strftime('%b').lower()
        date_info = monthrange(int(Config.current_year), int(range_month))
        last_day = date_info[1]
        month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-{range_month}-{last_day}',
              freq='MS').strftime("%b").tolist()
        month_list = [item.lower() for item in month_list]

        return month_list
    
    def make_checklist(self, month_list=None, mode=None):
        print(f'\nmaking checklist for the year {Config.current_year}')
        table = self.db[self.tablename]
        if mode == 'autoreset':
            print('\n autoreset: checklist')
            table.drop()

            month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-12-31', 
                  freq='MS').strftime("%b").tolist()

        if mode == 'iterative_cl':
            month_list = month_list

        for month in month_list:  
            table.insert(dict(
                year=f'{Config.current_year}', 
                month=month.lower(),
                base_docs=False, 
                rs_exist=False,
                yfor=False, 
                mfor=False, 
                rr_proc=False,
                dep_proc=False, 
                depdetail_proc=False, 
                opcash_proc=False,
                grand_total_ok=False,
                ))

    def get_checklist(self):
        checklist = self.db[self.tablename]
        return checklist

    def drop_checklist(self):
        print('\ndropping checklist')
        checklist = self.db[self.tablename]
        checklist.drop()
        return self.db, self.tablename

    def show_checklist(self, verbose=None, col_str=None):
        return_list = []
        check_items = [item for item in self.db[self.tablename]]
        if col_str:
            for item in check_items:
                print(item)
                return_list.append(item[col_str])
            return check_items, return_list

        if verbose:
            for item in check_items:
                print(item)
        return check_items

    def fix_date(self, date):
        dt_object = parse(date)
        year = str(dt_object.year)
        month = str(dt_object.month)

        month = dt_object.strftime('%b').lower()

        return year, month

    def fix_date2(self, month):
        dt_object = parse(month)
        month = str(dt_object.month)

        month = dt_object.strftime('%m')

        return month

    '''for some reason I cannot update dict key to pass in argument to update by column name; very frustrating'''
    def check_opcash(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], opcash_proc=True)
                self.db[self.tablename].update(data, ['id'])

    def check_mfor(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], mfor=True)
                self.db[self.tablename].update(data, ['id'])

    def check_rr_proc(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], rr_proc=True)
                self.db[self.tablename].update(data, ['id'])
  
    def check_dep_proc(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], dep_proc=True)
                self.db[self.tablename].update(data, ['id'])

    def check_depdetail_proc(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], depdetail_proc=True)
                self.db[self.tablename].update(data, ['id'])

    def check_basedocs_proc(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], base_docs=True)
                self.db[self.tablename].update(data, ['id'])

    def check_grand_total_ok(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], grand_total_ok=True)
                self.db[self.tablename].update(data, ['id'])

    def get_complete_cl_month(self):
        checklist = self.db[self.tablename]
        complete_cl_month = [item['year'] + '-' + self.fix_date2(item['month']) for item in checklist if item['grand_total_ok'] == True]
        return complete_cl_month


