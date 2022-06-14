import sys
from datetime import datetime as dt

from backend import Mentry, Payment, PopulateTable, db
from db_utils import DBUtils

# we can do a manual entry and then to persist it for testing we will
# use an entry on Config 

# process config corrections

# we would want to select based on 
    # year
    # month
    # db/class
    # id
    # we have some foreign keys so we may have to look at dependencies & cascades

class ManualEntry:

    def __init__(self, db=None):
        self.populate = PopulateTable()
        self.tables_list = self.populate.return_tables_list()
        self.db = db

    def main(self):
        self.connect_to_db()

        months = set([rec.date_posted for rec in Payment.select()])
        year = list(months)[0].year
        months = set([month.month for month in months])
        selection = self.selection_ui(list1=months, header1=year, header2='months', header3=Payment)
        rows = self.what_rows_payments(target=selection)

        selection = self.selection_ui(list1=rows, header1=year, header2='transactions', header3=Payment)

        selected_item = self.find_by_id(selection=selection)

        choice = input('press 1 to update and Z to delete: ')

        if choice == 1:
            """this goes nowhere rn"""
            modified_item = self.update_ui(selected_item=selected_item)
        elif choice == 'Z':
            modified_item = self.delete_ui(selected_item=selected_item)

    def delete_ui(self, selected_item=None):
        print('delete ui')
        payment = Payment.delete_by_id(selected_item.id)
        selected_item = selected_item.__data__
        self.record_delete_to_db(selected_item=selected_item)
    
    def update_ui(self, selected_item=None):
        '''this does nothing right now'''
        count = 1
        for key, value in selected_item.__dict__['__data__'].items():
            print(count, key, value)
            count += 1              
        breakpoint()
            
    def selection_ui(self, list1=None, header1=None, header2=None, header3=None):
        print(f'showing {header2} in {header1} for table {header3}')

        choices = []
        for count, item in enumerate(list1, 1):
            print(count, item)
            tup = (count, item)
            choices.append(tup)
        
        choice = int(input('please enter a number from the above list: '))
        selection = [item for count, item in choices if count == choice][0]
        print(f'you chose {selection}')
        return selection            
    
    def find_by_id(self, selection=None):
        selected_record = Payment.get(Payment.id==selection[0])
        return selected_record
    
    def what_tables_available(self):
        tables_list = self.populate.return_tables_list()
        return tables_list

    def what_rows_payments(self, target=None):
        rows = [(row.id, row.tenant, row.amount, dt.strftime(row.date_posted, '%Y-%m-%b')) for row in Payment.select().where(Payment.date_code==target).namedtuples()]
        return rows

    def what_years_available(self, selection=None):
        years = set([rec.date_posted for rec in selection.select()])
        years = set([year.year for year in years])
    
        if len(years) <= 1:
            return list(years)
        else:
            return list(years)

    def connect_to_db(self):
        DBUtils.pw_connect_to_db(db=self.db, tables_list=self.tables_list)

    def record_delete_to_db(self, selected_item=None):
        mentry = Mentry.create(obj_type='Payment', ch_type='delete', original_item=str(selected_item), change_time=dt.now())
        mentry.save()
