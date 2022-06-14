from backend import db, PopulateTable

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
        xxx = self.what_years_available(selection=selection)
        breakpoint()

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
        # tables_list = db.get_tables()
        # can I get globals from other module? mapping dict
        return tables_list

    def what_years_available(self, selection=None):
        table_name = 'Payment'
        table = globals()[table_name].create(amount=10.00)
        breakpoint()

# class Table(Model):
#     text = TextField()

#     class Meta:
#         database = DB

# table_name = 'Table'
# table = globals()[table_name].create(text='lorem ipsum')