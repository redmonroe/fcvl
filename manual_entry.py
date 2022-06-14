from backend import db, BaseModel, PopulateTable, Payment
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
        print('\n starting manual entry flow')
        tables_list = self.what_tables_available()
        selection = self.selection_ui(list1=tables_list)
        years = self.what_years_available(selection=selection)

        if len(years) > 1:
            selection = self.selection_ui(list1=years)
        else:
            print(f'working with year {years[0]}')
            
    def selection_ui(self, list1=None):
        choices = []
        for count, item in enumerate(list1, 1):
            print(count, item)
            tup = (count, item)
            choices.append(tup)
        
        choice = int(input('please enter a number from the above list: '))
        selection = [item for count, item in choices if count == choice][0]
        print(f'you chose {selection}')
        return selection

    def what_tables_available(self):
        tables_list = self.populate.return_tables_list()
        return tables_list

    def what_years_available(self, selection=None):
        years = set([rec.date_posted for rec in selection.select()])
        years = set([year.year for year in years])
    
        if len(years) <= 1:
            return list(years)
        else:
            return list(years)