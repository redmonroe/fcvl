import datetime
import logging
import os

import pandas as pd
from numpy import nan
from peewee import *
import math

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

class TenantRent(BaseModel):
    tenant = ForeignKeyField(Tenant, backref='rent')
    unit = CharField()
    rent_amount = DecimalField(default=0.00)
    rent_date = DateField()

class SubsidyRent(BaseModel):
    pass

class ContractRent(BaseModel):
    pass

class Damages(BaseModel):
    pass

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

    init_cutoff_date = '2022-01'

    def rent_roll_load_wrapper(self, path=None, date=None):

        period_start_tenant_names = set([name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()])

        if date == self.init_cutoff_date: # skip compare on init month
            nt_list = self.basic_load(filename=path, mode='execute', date=date)
        else: 
            nt_list = self.basic_load(filename=path, mode='return_only', date=date)
        
        rent_roll_set = set([row.name for row in nt_list])

        if date != self.init_cutoff_date: # this is the main loop
            mis, mos = self.find_mi_and_mo(start_set=period_start_tenant_names,end_set=rent_roll_set)
            self.insert_move_ins(move_ins=mis)
            self.deactivate_move_outs(move_outs=mos)

        return nt_list, rent_roll_set, period_start_tenant_names

    def basic_load(self, filename, mode=None, date=None):
        from collections import namedtuple

        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)

        Row = namedtuple('row', 'name unit rent mo date')
        explicit_move_outs = []
        nt_list = []
        for index, rec in df.iterrows():
            if rec['Move out'] != fill_item:
                explicit_move_outs.append(rec['Move out'])
            row = Row(rec['Name'].lower(), rec['Unit'], rec['Actual Rent Charge'], rec['Move out'] , datetime.datetime.strptime(date, '%Y-%m'))
            nt_list.append(row)

        actual_rent_sum_to_bal = float(((nt_list.pop(-1)).rent).replace(',', ''))
        mo_rent_addbacks = 0
        mo_len_list = list(set([row.mo for row in nt_list if row.mo != fill_item]))
        # rent_roll_dict = {row.name: row.unit for row in nt_list if row.name != fill_item}
        if len(mo_len_list) > 0:
            admin_mo, actual_mo = self.catch_move_outs_in_target_file(nt_list=nt_list, fill_item=fill_item)

            if admin_mo != []:
                print(f'You have a likely admin move out or move outs see {admin_mo}')

            if actual_mo != []:
                nt_list, mo_rent_addbacks = self.remove_actual_move_outs_from_target_rent_roll(nt_list=nt_list, actual_mo=actual_mo)
       
        nt_list = [row for row in nt_list if row.name != 'vacant']

        insert_many_list = [{'tenant_name': row.name} for row in nt_list if row.name != 'vacant']
        insert_many_list_units = [{'unit_name': row.unit, 'tenant': row.name} for row in nt_list if row.name != 'vacant']
  
        actual_rent_charged  = sum([float(row.rent) for row in nt_list])
        if mo_rent_addbacks:
            actual_rent_charged += mo_rent_addbacks
        if actual_rent_charged != actual_rent_sum_to_bal:
            breakpoint()
        insert_many_rent = [{'tenant': row.name, 'unit': row.unit, 'rent_amount': row.rent, 'rent_date': row.date} for row in nt_list if row.name != 'vacant']  
     

        if mode == 'execute':
            query = Tenant.insert_many(insert_many_list)
            query.execute()
            query = Unit.insert_many(insert_many_list_units)
            query.execute()
            query = TenantRent.insert_many(insert_many_rent)
            query.execute()
        
        return nt_list

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
        breakpoint()

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

    def catch_move_outs_in_target_file(self, nt_list=None, fill_item=None):
         # catch 'VACANT': '02/06/2022'

        vacant_move_out_iter = [(row.name, row.unit, row.mo) for row in nt_list if row.name == 'vacant' and row.mo != fill_item]
        

        occupied_move_out_iter = [(row.name, row.unit, row.mo) for row in nt_list if row.name != 'vacant' and row.mo != fill_item]

        return vacant_move_out_iter, occupied_move_out_iter

    def remove_actual_move_outs_from_target_rent_roll(self, nt_list=None, actual_mo=None):
        '''removes from rent roll dict insertion'''
        removed_charges_list = []

        for row in nt_list:
            for rec in actual_mo:
                if row.name == rec[0]:
                    removed_charges_list.append(float(row.rent))
                    nt_list.remove(row)
        
        return nt_list, sum(removed_charges_list)

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

    def check_db_tp_and_ntp(self, grand_total=None, dt_obj_first=None, dt_obj_last=None):
        all_tp = [float(rec.amount) for rec in Payment.select().
                where(Payment.date_posted >= dt_obj_first).
                where(Payment.date_posted <= dt_obj_last)]
        all_ntp = [float(rec.amount) for rec in NTPayment.select().
                where(NTPayment.date_posted >= dt_obj_first).
                where(NTPayment.date_posted <= dt_obj_last)]

        assert sum(all_ntp) + sum(all_tp) == grand_total

        return all_tp, all_ntp

    def get_all_tenants_beg_bal(self):
        # doesn't need tenant active at this point
        detail_beg_bal_all = [(row.tenant_name, row.amount, row.beg_bal_amount) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount, Payment.amount).join(Payment).namedtuples()] 

        return detail_beg_bal_all

    def check_for_multiple_payments(self, detail_beg_bal_all=None, dt_obj_first=None, dt_obj_last=None):
        pay_names = [row.tenant for row in Payment().
                select().
                where(Payment.date_posted >= dt_obj_first).
                where(Payment.date_posted <= dt_obj_last).
                join(Tenant).namedtuples()]
        if len(pay_names) != len(set(pay_names)):
            different_names = [name for name in pay_names if pay_names.count(name) > 1]
            return different_names
        return []

    def get_beg_bal_sum_by_period(self, style=None, dt_obj_first=None, dt_obj_last=None):
        if style == 'initial':
            sum_beg_bal_all = [float(row.beg_bal_amount) for row in Tenant.select(
                Tenant.active, Tenant.beg_bal_amount).
                where(Tenant.active=='True').
                namedtuples()]     
        else:
            sum_beg_bal_all = [float(row.beg_bal_amount) for row in Tenant.select(
                Tenant.active, Tenant.beg_bal_amount, Payment.date_posted).
                where(Payment.date_posted >= dt_obj_first).
                where(Payment.date_posted <= dt_obj_last).
                where(Tenant.active=='True').
                join(Payment).namedtuples()]      

        return sum(sum_beg_bal_all)

    def match_tp_db_to_df(self, df=None, dt_obj_first=None, dt_obj_last=None):
        sum_this_month_db = sum([float(row.amount) for row in 
            Payment.select(Payment.amount).
            where(Payment.date_posted >= dt_obj_first).
            where(Payment.date_posted <= dt_obj_last)])

        sum_this_month_df = sum(df['amount'].astype(float).tolist())
        assert sum_this_month_db == sum_this_month_df

        return sum_this_month_db, sum_this_month_df

    def get_sum_tp_by_tenant(self, dt_obj_first=None, dt_obj_last=None):
        '''what happens on a moveout'''
        sum_payment_list = list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        Tenant.beg_bal_amount, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted >= dt_obj_first).
        where(Payment.date_posted <= dt_obj_last).
        join(Payment).namedtuples()]))

        return sum_payment_list

    def get_end_bal_by_tenant(self, dt_obj_first=None, dt_obj_last=None):
        sum_payment_list = self.get_sum_tp_by_tenant(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)
        end_bal_list = [(rec[0], float(rec[1]) - rec[2]) for rec in sum_payment_list]
        return end_bal_list

    def get_total_collections_by_month(self, dt_obj_first=None, dt_obj_last=None):
        total_collections = sum([float(row.rent_amount) for row in TenantRent().
        select(TenantRent.rent_amount).
        where(TenantRent.rent_date >= dt_obj_first).
        where(TenantRent.rent_date <= dt_obj_last)])

        return total_collections