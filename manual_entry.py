from backend import db, PopulateTable, Payment
from datetime import datetime as dt
import sys

# we can do a manual entry and then to persist it for testing we will
# use an entry on Config 

# we would want to select based on 
    # year
    # month
    # db/class
    # id
    # we have some foreign keys so we may have to look at dependencies & cascades

class ManualEntry:

    def __init__(self):
        self.populate = PopulateTable()

    def main(self):
        months = set([rec.date_posted for rec in Payment.select()])
        year = list(months)[0].year
        months = set([month.month for month in months])
        selection = self.selection_ui(list1=months, header1=year, header2='months', header3=Payment)
        rows = self.what_rows_payments(target=selection)

        selection = self.selection_ui(list1=rows, header1=year, header2='transactions', header3=Payment)

        selected_item = self.find_by_id(selection=selection)
        breakpoint()

        modified_item = self.selection_ui(list1=selected_item, header1=year, header2='transactions', header3=Payment)
        
            
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