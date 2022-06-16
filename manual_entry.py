import sys
from datetime import datetime as dt
from operator import attrgetter

from backend import Mentry, Payment, PopulateTable, db
from config import Config
from db_utils import DBUtils

# this works for payments; col name issues prevent the query from working properly for other types; I can address this

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

        tables_list = self.what_tables_available()
        tables_zip = self.map_table_obj_to_table_str(tables_list=tables_list)

        selection1 = self.selection_ui_tables(list1=tables_zip, header1=year, header2='months')

        selection2 = self.selection_ui(list1=months, header1=year, header2='months', header3=selection1._meta.__dict__['name'])

        rows = self.what_rows_payments(target=selection2, obj=selection1)

        selection = self.selection_ui(list1=rows, header1=year, header2='rows', header3=selection1._meta.__dict__['name'])
        breakpoint()

        selected_item = self.find_by_id(selection=selection)

        choice = input('press 1 to update and Z to delete: ')

        if choice == 1:
            """this goes nowhere rn"""
            modified_item = self.update_ui(selected_item=selected_item)
        elif choice == 'Z':
            modified_item = self.delete_ui(selected_item=selected_item)

    def apply_persisted_changes(self):
        self.connect_to_db()
        self.find_persisted_changes_from_config()

    def delete_ui(self, selected_item=None):
        print('delete ui')
        payment = Payment.delete_by_id(selected_item.id)
        selected_item = selected_item.__data__
        self.record_delete_to_db(selected_item=selected_item)

    def delete_ui_dynamic(self, obj_type=None, selected_item=None):
        print('delete ui dynamic')
        item = obj_type.delete_by_id(selected_item.id)
        self.record_delete_to_db(selected_item=str(selected_item))
    
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

    def selection_ui_tables(self, list1=None, header1=None, header2=None,):
        print(f'showing {header2} in {header1}')

        choices = []
        count = 1
        for item in list1:
            table_name = list(item.keys())[0]
            if table_name in ['mentry', 'payment', 'ntpayment']:
                table_obj = list(item.values())[0]
                print(count, table_name)
                tup = (count, table_obj)
                choices.append(tup)
                count += 1
        
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

    def map_table_obj_to_table_str(self, tables_list=None):
        tables_list_str = [table._meta.__dict__['name'] for table in tables_list ]
        tables_zip = [{name_str: obj} for obj, name_str in zip(tables_list, tables_list_str)]
        return tables_zip

    def what_rows_payments(self, target=None, obj=None):
        rows = [(row.id, row.tenant, row.amount, dt.strftime(row.date_posted, '%Y-%m-%d')) for row in obj.select().where(obj.date_code==target).namedtuples()]
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

    def find_persisted_changes_from_config(self):
        for item in Config.persisted_changes:
            obj_type = item['obj_type']
            model_name= self.get_name_from_obj(obj_type=obj_type)

            col_name1 = item['col_name1'][0]
            col_value1 = item['col_name1'][1]
            col_name2 = item['col_name2'][0]
            col_value2 = item['col_name2'][1]
            col_name3 = item['col_name3'][0]
            col_value3 = item['col_name3'][1]
            result = [rec for rec in model_name.select().
                where(attrgetter(col_name1)(model_name) == col_value1).
                where(attrgetter(col_name2)(model_name) == col_value2).
                where(attrgetter(col_name3)(model_name) == col_value3).
                namedtuples()]
            
            try:
                result = result[0]
            except IndexError as e:
                print('You probably already deleted the transaction.  Check mentry db for further information.')
                print(e)
                print('exiting program')
                break

            if item['action'] == 'delete':
                self.delete_ui_dynamic(obj_type=model_name, selected_item=result)

    def get_name_from_obj(self, obj_type=None):
        model_name = getattr(sys.modules[__name__], obj_type)
        return model_name
