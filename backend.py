import datetime
import logging
import os

import pandas as pd
from numpy import nan
from peewee import *

from config import Config
from build_rs import BuildRS

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
    tenant = ForeignKeyField(Tenant, backref='payments')
    amount = CharField()
    date_posted = DateField()
    date_code = IntegerField()
    unit = CharField()
    deposit_id = IntegerField()

class NTPayment(BaseModel):
    payee = CharField()
    amount = CharField()
    date_posted = DateField()
    date_code = IntegerField()
    genus = CharField(default='other')    
    deposit_id = IntegerField()

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
            admin_mo, actual_mo = self.catch_move_outs_in_target_file(t_name=t_name, unit=unit, explicit_move_outs=explicit_move_outs)

            if admin_mo != []:
                print(f'You have a likely admin move out or move outs see {admin_mo}')
            if actual_mo != []:
                rent_roll_dict = self.remove_actual_move_outs_from_target_rent_roll(rr_dict=rent_roll_dict, actual_mo=actual_mo)
       
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

    def payment_load_full(self, filename):
        df = self.read_excel_payments(path=filename)
        df = self.remove_nan_lines(df=df)
        grand_total = self.grand_total(df=df)
        tenant_payment_df, ntp_df = self.return_and_remove_ntp(df=df, col='unit', remove_str=0)

        ntp_sum = sum(ntp_df['amount'].astype(float).tolist())  # can split up ntp further here
        
        if len(ntp_df) > 0:
            insert_nt_list = [{
                'payee': name.lower(),
                'amount': amount, 
                'date_posted': datetime.datetime.strptime(date_posted, '%m/%d/%Y'),  
                'date_code': date_code, 
                'genus': genus, 
                'deposit_id': deposit_id, 
                } for (deposit_id, genus, name, date_posted, amount, date_code) in ntp_df.values]
            query = NTPayment.insert_many(insert_nt_list)
            query.execute()
        
        insert_many_list = [{
            'tenant': name.lower(),
            'amount': amount, 
            'date_posted': datetime.datetime.strptime(date_posted, '%m/%d/%Y'),  
            'date_code': date_code, 
            'unit': unit, 
            'deposit_id': deposit_id, 
             } for (deposit_id, unit, name, date_posted, amount, date_code) in tenant_payment_df.values]

        query = Payment.insert_many(insert_many_list)
        query.execute()

        return grand_total, ntp_sum, tenant_payment_df

    def read_excel_payments(self, path):
        df = pd.read_excel(path, header=9)

        columns = ['deposit_id', 'unit', 'name', 'date_posted', 'amount', 'date_code']
        
        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        dt_code = [datetime.datetime.strptime(item, '%m/%d/%Y') for item in date if type(item) == str]
        dt_code = [str(datetime.datetime.strftime(item, '%m')) for item in dt_code]

        zipped = zip(bde, unit, name, date, pay, dt_code)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def grand_total(self, df):
        grand_total = sum(df['amount'].astype(float).tolist())
        return grand_total

    def return_and_remove_ntp(self, df, col=None, remove_str=None):
        ntp_item = df.loc[df[col] == remove_str]
        for item in ntp_item.index:
            df.drop(labels=item, inplace=True)
        return df, ntp_item

    def group_df(self, df, just_return_total=False):
        df = df.groupby(['name', 'unit']).sum()
        if just_return_total:
            df = df[0]
        return df   

    def remove_nan_lines(self, df=None):
        df = df.dropna(thresh=2)
        df = df.fillna(0)
        return df

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
        '''compares list of tenants at start of month to those at end'''
        '''explicit move-outs from excel have been removed from end of month rent_roll_dict 
        and should initiated a discrepancy in the following code by making end list diverge from start list'''
        '''these tenants are explicitly marked as active='False' in the tenant table in func() deactivate_move_outs'''
        move_ins = list(end_set - start_set) # catches move in
        move_outs = list(start_set - end_set) # catches move out

        return move_ins, move_outs

    def insert_move_ins(self, move_ins=None):
        for new_tenant in move_ins:
            nt = Tenant.create(tenant_name=new_tenant)
            nt.save()

    def catch_move_outs_in_target_file(self, t_name=None, unit=None, explicit_move_outs=None):
         # catch 'VACANT': '02/06/2022'
        move_out_df = pd.DataFrame(
                        {'Name': t_name,
                        'Unit': unit,
                        'Move out': explicit_move_outs, 
                        })

        vacant_move_out_iter = [(row['Name'], row['Unit'], row['Move out']) for (index, row) in move_out_df.iterrows() if row['Name'] == 'VACANT' and row['Move out'] != '0']

        occupied_move_out_iter = [(row['Name'].lower(), row['Unit'], row['Move out']) for (index, row) in move_out_df.iterrows() if row['Name'] != 'VACANT' and row['Move out'] != '0']

        return vacant_move_out_iter, occupied_move_out_iter

    def remove_actual_move_outs_from_target_rent_roll(self, rr_dict=None, actual_mo=None):
        '''removes from rent roll dict insertion'''
        for row in actual_mo:
            if row[0] in rr_dict:
                del rr_dict[row[0]]

        return rr_dict

    def deactivate_move_outs(self, move_outs=None):
        for row in move_outs:
            tenant = Tenant.get(Tenant.tenant_name == row)
            tenant.active = False
            tenant.save()

    def make_first_and_last_dates(self, date_str=None):
        import calendar
        dt_obj = datetime.datetime.strptime(date_str, '%Y-%m')
        dt_obj_first = dt_obj.replace(day = 1)
        dt_obj_last = dt_obj.replace(day = calendar.monthrange(dt_obj.year, dt_obj.month)[1])

        return dt_obj_first, dt_obj_last

    def check_db_tp_and_ntp(self, grand_total=None):
        all_tp = [float(rec.amount) for rec in Payment.select()]
        all_ntp = [float(rec.amount) for rec in NTPayment.select()]

        assert sum(all_ntp) + sum(all_tp) == grand_total

        return all_tp, all_ntp

    def get_all_tenants_beg_bal(self):
        # doesn't need tenant active at this point
        detail_beg_bal_all = [(row.tenant_name, row.amount, row.beg_bal_amount) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount, Payment.amount).join(Payment).namedtuples()] 

        return detail_beg_bal_all

    def check_for_duplicate_payments(detail_beg_bal_all=None):
        pay_names = [row[0] for row in detail_beg_bal_all]
        if len(pay_names) != len(set(pay_names)):
            different_names = [name for name in pay_names if pay_names.count(name) > 1]
        return different_names