import random
import sys
from datetime import datetime as dt
from operator import attrgetter

from backend import (Findexer, Mentry, NTPayment, Payment, PopulateTable, StatusEffect,
                     Subsidy, TenantRent, db)
from config import Config
from db_utils import DBUtils


class ManualEntry:

    def __init__(self, db=None):
        self.persisted_changes = Config.persisted_changes
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

        selection1 = self.selection_ui_tables(
            list1=tables_zip, header1=year, header2='months')

        selection2 = self.selection_ui(
            list1=months, header1=year, header2='months', header3=selection1._meta.__dict__['name'])

        rows = self.what_rows_payments(target=selection2, obj=selection1)

        selection = self.selection_ui(
            list1=rows, header1=year, header2='rows', header3=selection1._meta.__dict__['name'])

        selected_item = self.find_by_id(selection=selection)

        choice = input('press 1 to update and Z to delete: ')

        if choice == 1:
            """this goes nowhere rn"""
            modified_item = self.update_ui(selected_item=selected_item)
        elif choice == 'Z':
            modified_item = self.delete_ui(selected_item=selected_item)

    def apply_persisted_changes(self, 
                                explicit_month_to_load=None,
                                last_range_month=None):
        print('\napplying persistent changes')
        self.connect_to_db()
        self.find_persisted_changes_from_config(explicit_month_to_load=explicit_month_to_load,
                                                last_range_month=last_range_month)

    def delete_ui(self, selected_item=None, obj_str=None):
        print('delete ui')
        payment = Payment.delete_by_id(selected_item.id)
        selected_item = selected_item.__data__
        self.record_entry_to_manentry(
            obj_type='Payment', action='delete', txn_date=txn_date, selected_item=str(selected_item))

    def delete_ui_dynamic(self, obj_type=None, obj_str=None, selected_item=None):
        item = obj_type.delete_by_id(selected_item.id)
        txn_date = selected_item.date_posted
        self.record_entry_to_manentry(
            obj_type=obj_str, action='delete', txn_date=txn_date, selected_item=str(selected_item))
        print('ok')

    def update_amount_dynamic(self, target_attribute=None, obj_type=None, obj_str=None, updated_amount=None, selected_item=None):
        item = obj_type.get(selected_item.id)
        item.__setattr__(target_attribute, updated_amount)
        if type(item) == Payment or type(item) == Subsidy:
            txn_date = item.date_posted
        elif type(item) == TenantRent:
            txn_date = item.rent_date
        item.save()
        try:
            self.record_entry_to_manentry(
                obj_type=obj_str, action='updated_amount', txn_date=txn_date, selected_item=str(item))
            print('ok')
        except UnboundLocalError as e:
            print(f'{type(item)} not currently supported')
            print('you can add support in ManualEntry.update_amount_dynamic')
            breakpoint()

    def update_ui(self, selected_item=None):
        '''this does nothing right now'''
        count = 1
        for key, value in selected_item.__dict__['__data__'].items():
            print(count, key, value)
            count += 1

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
        selected_record = Payment.get(Payment.id == selection[0])
        return selected_record

    def what_tables_available(self):
        tables_list = self.populate.return_tables_list()
        return tables_list

    def map_table_obj_to_table_str(self, tables_list=None):
        tables_list_str = [table._meta.__dict__['name']
                           for table in tables_list]
        tables_zip = [{name_str: obj}
                      for obj, name_str in zip(tables_list, tables_list_str)]
        return tables_zip

    def what_rows_payments(self, target=None, obj=None):
        rows = [(row.id, row.tenant, row.amount, dt.strftime(row.date_posted, '%Y-%m-%d'))
                for row in obj.select().where(obj.date_code == target).namedtuples()]
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

    def record_entry_to_manentry(self, obj_type=None, action=None, selected_item=None, txn_date=None):
        mentry = Mentry.create(obj_type=obj_type, ch_type=action, original_item=str(
            selected_item), txn_date=txn_date, change_time=dt.now())
        mentry.save()

    def _print_persisted_item(self, item=None):
        try:
            print(
                f"\n{item['action']} {item['obj_type']} for {item['col_name1'][1]} for ${item['col_name2'][1]} on {item['col_name3'][1]} to ${item['col_name4'][1]}.")
        except KeyError as e:
            print(
                f"\n{item['action']} {item['obj_type']} for {item['col_name1'][1]} for ${item['col_name2'][1]} on {item['col_name3'][1]}.")
        return item

    def _get_model_name(self, item=None):
        obj_str = item['obj_type']
        model_name = self.get_name_from_obj(obj_type=obj_str)
        return obj_str, model_name

    def _handle_create_entry(self, **kwargs):
        new_model_row = kwargs['model_name'].create(payee=kwargs['payee'],
                                                    amount=kwargs['amount'],
                                                    date_posted=kwargs['date_posted'],
                                                    date_code=kwargs['date_code'],
                                                    deposit_id=random.randint(
                                                        0, 10000),
                                                    genus=kwargs['genus'],)
        new_model_row.save()

    def find_persisted_changes_from_config(self, 
                                           explicit_month_to_load=None, 
                                           last_range_month=None,
                                           ):
        if explicit_month_to_load:
            first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=explicit_month_to_load)
            self.persisted_changes = [item for item in self.persisted_changes if dt.strptime(
                item['col_name3'][1], '%Y-%m-%d') >= first_dt and dt.strptime(
                item['col_name3'][1], '%Y-%m-%d') <= last_dt]
            
        if last_range_month:
            _, last_dt = self.populate.make_first_and_last_dates(date_str=last_range_month)
            first_dt, _ = self.populate.make_first_and_last_dates(date_str='2022-01')
            self.persisted_changes = [item for item in self.persisted_changes if dt.strptime(
                item['col_name3'][1], '%Y-%m-%d') >= first_dt and dt.strptime(
                item['col_name3'][1], '%Y-%m-%d') <= last_dt]
        
        for item in self.persisted_changes:
            item = self._print_persisted_item(item=item)

            obj_str, model_name = self._get_model_name(item=item)
            
            col_name1, col_value1 = [item['col_name1'][0], item['col_name1'][1]]
            col_value1 = item['col_name1'][1]
            col_name2 = item['col_name2'][0]
            col_value2 = item['col_name2'][1]
            col_name3 = item['col_name3'][0]
            col_value3 = item['col_name3'][1]
            
            if item['action'] not in ['delete', 'update_findexer']:
                updated_amount_name, updated_amount = [item['col_name4'][0], item['col_name4'][1]]
            
            if obj_str == 'Findexer':
                new_data = {col_name2:
                            col_value2,
                            }

                query = model_name.update(
                    **new_data).where(Findexer.doc_id == col_value1)
                query.execute()
                self.record_entry_to_manentry(obj_type=obj_str, action=item[
                    'action'], selected_item=str(col_value1), txn_date=item['col_name3'][1])
                # do I need to propagate changes to opcash or another table?
            elif obj_str == 'StatusEffect':
                ending_bool, ending_time = [item['col_name5'][0], item['col_name5'][1]]
                if ending_time == 'False':
                data = {col_name1: col_value1,
                        col_name2: col_value2,
                        col_name3: col_value3,
                        updated_amount_name: updated_amount,
                        }
                query = model_name.create(**data)
                query.save()
                # i used to use break here but removed it once I had multiple tenants with SE
            
            
            elif item['action'] == 'create':
                self._handle_create_entry(model_name=model_name,
                                          obj_str=obj_str,
                                          payee=col_value1,
                                          amount=col_value2,
                                          date_posted=col_value3,
                                          date_code=col_value3.split('-')[1],
                                          genus=updated_amount)
                mentry = Mentry.create(obj_type=obj_str,
                                       ch_type=item['action'],
                                       original_item=str(None),
                                       txn_date=col_name3[1],
                                       change_time=dt.now())
                mentry.save()
            else:
                # TODO: can this all be an UrQuery item
                result = [rec for rec in model_name.select().
                          where(attrgetter(col_name1)(model_name) == col_value1).
                          where(attrgetter(col_name2)(model_name) == col_value2).
                          where(attrgetter(col_name3)(model_name) == col_value3).
                          namedtuples()]
            try:
                result = result[0]
                if item['action'] == 'delete':
                    self.delete_ui_dynamic(
                        obj_type=model_name, obj_str=obj_str, selected_item=result)
                elif item['action'] == 'update_amount':
                    self.update_amount_dynamic(target_attribute=col_name2,
                                               selected_item=result,
                                               obj_type=model_name,
                                               obj_str=obj_str,
                                               updated_amount=updated_amount, )

            except (IndexError, TypeError) as e:
                print('You probably already deleted the transaction OR transaction "has not happened" yet in program time.  Check mentry db for further information.')
                print(e)

    def get_name_from_obj(self, obj_type=None):
        try:
            model_name = getattr(sys.modules[__name__], obj_type)
        except AttributeError as e:
            print(e)
            raise
        return model_name
