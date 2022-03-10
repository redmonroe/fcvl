from config import Config
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

    def make_checklist(self):
        table = self.db[self.tablename]
        table.drop()

        month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-12-31', 
              freq='MS').strftime("%b").tolist()

        for month in month_list:  
            table.insert(dict(
                year=f'{Config.current_year}', 
                month=month.lower(),
                rs_exist=False,
                yfor=False, 
                mfor=False, 
                rr_proc=False,
                dep_proc=False, 
                depdetail_proc=False, 
                opcash_proc=False,
                dep_rec=False,
                rs_write=False,
                ))

    def get_checklist(self):
        checklist = self.db[self.tablename]
        return checklist

    def drop_checklist(self):
        checklist = self.db[self.tablename]
        checklist.drop()
        print('checklist dropped')

    def show_checklist(self, col_str=None):
        return_list = []
        check_items = [item for item in self.db[self.tablename]]
        if col_str:
            for item in check_items:
                print(item)
                return_list.append(item[col_str])
            return check_items, return_list
        return check_items

    def fix_date(self, date):
        dt_object = parse(date)
        year = str(dt_object.year)
        month = str(dt_object.month)

        month = dt_object.strftime('%b').lower()

        return year, month

    def check_opcash(self, date, col1=None):
        '''for some reason I cannot update dict key to pass in argument to update by column name; very frustrating'''
        # data[col1] = data.pop('col1')

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
  
    def check_dep_proc(self, date, col1=None):
        year, month = self.fix_date(date)
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            if item['year'] == year and item['month'] == month: 
                data = dict(id=item['id'], dep_proc=True)
                self.db[self.tablename].update(data, ['id'])

if __name__ == '__main__':
    clist = Checklist(db=Config.test_checklist_db)
    # clist.make_checklist()
    return_list, list1 = clist.show_checklist(col_str='id')