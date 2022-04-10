import datetime
import logging
import os

import pandas as pd
from numpy import nan
from peewee import *

from config import Config

basedir = os.path.abspath(os.path.dirname(__file__))

db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db', pragmas={'foreign_keys': 1})

class BaseModel(Model):
    class Meta:
        database = db

class Tenant(BaseModel):
    tenant_name = CharField(primary_key=True, unique=True)
    active = CharField(default=True) # 
    beg_bal_date = DateField(default='2022-01-01')
    beg_bal_amount = DecimalField(default=0.00)

class Unit(BaseModel):
    unit_name = CharField(unique=True)
    status = CharField(default='occupied') 
    tenant = ForeignKeyField(Tenant, backref='unit')

    @staticmethod
    def find_vacants():
        all_units = Config.units
        # get all units
        query = [unit for unit in Unit.select().namedtuples()]
        unit_status = [unit.unit_name for unit in Unit.select().namedtuples()]
        vacant_list = list(set(all_units) - set(unit_status))

        return vacant_list

class Payment(BaseModel):
    payment_date = DateField(default='2022-01-01')
    payment_amount = DecimalField(default=0.00)
    tenant = ForeignKeyField(Tenant, backref='payments')

class PopulateTable:

    def basic_load(self, filename, mode=None):
        df = pd.read_excel(filename, header=16)

        t_name = df['Name'].tolist()
        unit = df['Unit'].tolist()
        explicit_move_outs = df['Move out'].fillna(value='0').tolist()

        rent_roll_dict = dict(zip(t_name, unit))
        rent_roll_dict = {k.lower(): v for k, v in rent_roll_dict.items() if k is not nan}
        
        mo_len_list = list(set([it for it in explicit_move_outs]))
        if len(mo_len_list) > 1:
            admin_mo, actual_mo = self.catch_move_outs(t_name=t_name, unit=unit, explicit_move_outs=explicit_move_outs)

            if admin_mo != []:
                print(f'You have a likely admin move out or move outs see {admin_mo}')
            if actual_mo != []:
                rent_roll_dict = self.remove_actual_move_outs(rr_dict=rent_roll_dict, actual_mo=actual_mo)
    
        if filename == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_03_2022.xlsx':
            assert 'johnson, thomas' not in rent_roll_dict.keys()
       
        all_units_dict = {k: v for k, v in rent_roll_dict.items()}
        
        rent_roll_dict = {k: v for k, v in rent_roll_dict.items() if k != 'vacant'}

        insert_many_list = [{'tenant_name': name} for (name, unit) in rent_roll_dict.items()]
        insert_many_list_units = [{'unit_name': unit, 'tenant': name} for (name, unit) in rent_roll_dict.items()]

        if mode == 'execute':
            query = Tenant.insert_many(insert_many_list)
            query.execute()
            query = Unit.insert_many(insert_many_list_units)
            query.execute()

        return rent_roll_dict

    def balance_load(self, filename):
        df = pd.read_excel(filename)
        t_name = df['name'].tolist()
        beg_bal = df['balance'].tolist()
        rent_roll_dict = dict(zip(t_name, beg_bal))
        rent_roll_dict = {k.lower(): v for k, v in rent_roll_dict.items() if k != 'vacant'}
        rent_roll_dict = {k: v for k, v in rent_roll_dict.items() if k != 'vacant'}

        ten_list = [tenant for tenant in Tenant.select()]
        for tenant in ten_list:
            for name, bal in rent_roll_dict.items():
                if tenant.tenant_name == name:
                    tenant.beg_bal_amount = bal
                    tenant.save()

        # could also use bulk update query: https://docs.peewee-orm.com/en/latest/peewee/api.html

    def payment_load_simple(self, filename):
        df = pd.read_excel(filename)
        insert_many_list1 = []
        for index, row in df.iterrows():
            tup = ()
            tup = (row['name'], row['date'], row['amount'])
            insert_many_list1.append(tup)
        insert_many_list = [{'tenant': name, 'payment_date': date, 'payment_amount': amount} for (name, date, amount) in insert_many_list1]
        query = Payment.insert_many(insert_many_list)
        query.execute()

    def load_units(self, filename, verbose=False):
        insert_many_list = []
        for item in Config.units:
            insert_many_list.append({'unit_name': item})

        query = Unit.insert_many(insert_many_list)
        query.execute()

    def load_tenants(self, filename):
        df = pd.read_excel(filename, header=16)

        t_name = df['Name'].tolist()
        unit = df['Unit'].tolist()

        rent_roll_dict = dict(zip(t_name, unit))
        rent_roll_dict = {k.lower(): v for k, v in rent_roll_dict.items() if k is not nan}
        rent_roll_dict = {k: v for k, v in rent_roll_dict.items() if k != 'vacant'}
        insert_many_list = [{'tenant_name': name, 'unit': unit} for (name, unit) in rent_roll_dict.items()]
        insert_many_list_units = [{'unit_name': unit, 'tenant': name, 'status': 'occupied'} for (name, unit) in rent_roll_dict.items()]

        query = Tenant.insert_many(insert_many_list)
        query.execute()
        query = Unit.insert_many(insert_many_list_units)
        query.execute()

        return rent_roll_dict

    def find_mi_and_mo(self, start_set=None, end_set=None):

        move_ins = list(end_set - start_set) # catches move in
        move_outs = list(start_set - end_set) # catches move out

        return move_ins, move_outs

    def insert_move_ins(self, move_ins=None):
        for new_tenant in move_ins:
            nt = Tenant.create(tenant_name=new_tenant)
            nt.save()

    def catch_move_outs(self, t_name=None, unit=None, explicit_move_outs=None):
         # catch 'VACANT': '02/06/2022'
        move_out_df = pd.DataFrame(
                        {'Name': t_name,
                        'Unit': unit,
                        'Move out': explicit_move_outs, 
                        })

        vacant_move_out_iter = [(row['Name'], row['Unit'], row['Move out']) for (index, row) in move_out_df.iterrows() if row['Name'] == 'VACANT' and row['Move out'] != '0']

        occupied_move_out_iter = [(row['Name'].lower(), row['Unit'], row['Move out']) for (index, row) in move_out_df.iterrows() if row['Name'] != 'VACANT' and row['Move out'] != '0']

        return vacant_move_out_iter, occupied_move_out_iter

    def remove_actual_move_outs(self, rr_dict=None, actual_mo=None):
        for row in actual_mo:
            if row[0] in rr_dict:
                del rr_dict[row[0]]

        return rr_dict