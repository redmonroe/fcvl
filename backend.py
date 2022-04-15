import calendar
import datetime
import logging
import math
import os
from collections import namedtuple
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from sqlite3 import IntegrityError
import pandas as pd
import pytest
from numpy import nan
from peewee import *
from peewee import JOIN, fn

from build_rs import BuildRS
from checklist import Checklist
from config import Config
from file_indexer import FileIndexer

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
    # tenant = CharField()
    t_name = ForeignKeyField(Tenant, backref='charge')
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

    def init_tenant_load(self, filename=None, date=None):
        nt_list, total_tenant_charges, explicit_move_outs = self.init_load_ten_unit_ten_rent(filename=filename, date=date)

        return nt_list, total_tenant_charges, explicit_move_outs

    def init_load_ten_unit_ten_rent(self, filename=None, date=None):
        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)
        nt_list, explicit_move_outs = self.nt_from_df(df=df, date=date, fill_item=fill_item)

        total_tenant_charges = float(((nt_list.pop(-1)).rent).replace(',', ''))

        nt_list = self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)

        ten_insert_many = [{'tenant_name': row.name} for row in nt_list]
        
        units_insert_many = [{'unit_name': row.unit, 'tenant': row.name} for row in nt_list]

        rent_insert_many = [{'t_name': row.name, 'unit': row.unit, 'rent_amount': row.rent, 'rent_date': row.date} for row in nt_list if row.name != 'vacant']  

        query = Tenant.insert_many(ten_insert_many)
        query.execute()
        query = TenantRent.insert_many(rent_insert_many)
        query.execute()
        query = Unit.insert_many(units_insert_many)
        query.execute()

        return nt_list, total_tenant_charges, explicit_move_outs

    def after_jan_load(self, filename=None, date=None):

        ''' order matters'''
        ''' get tenants from jan end/feb start'''
        ''' get rent roll from feb end in nt_list from df'''

        period_start_tenant_names = [(name.tenant_name, name.unit_name) for name in Tenant.select(Tenant.tenant_name, Unit.unit_name).where(Tenant.active==True).join(Unit).namedtuples()]

        
        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)
        nt_list, explicit_move_outs = self.nt_from_df(df=df, date=date, fill_item=fill_item)

        total_tenant_charges = float(((nt_list.pop(-1)).rent).replace(',', ''))

        period_end_tenant_names = [(row.name, row.unit) for row in self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)]


        computed_mis, computed_mos = self.find_rent_roll_changes_by_comparison(start_set=set(period_start_tenant_names), end_set=set(period_end_tenant_names))
        # breakpoint()
        cleaned_mos = self.merge_move_outs(explicit_move_outs=explicit_move_outs, computed_mos=computed_mos)
        self.insert_move_ins(move_ins=computed_mis)

        if cleaned_mos != []:
            self.deactivate_move_outs(move_outs=cleaned_mos)

        ''' now we should have updated list of active tenants'''
        cleaned_nt_list = [row for row in self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)]

        insert_many_rent = [{'t_name': row.name, 'unit': row.unit, 'rent_amount': row.rent, 'rent_date': row.date} for row in cleaned_nt_list]  

        query = TenantRent.insert_many(insert_many_rent)
        query.execute()
        '''Units: now we should check whether end of period '''
        return cleaned_nt_list, total_tenant_charges, cleaned_mos

    def merge_move_outs(self, explicit_move_outs=None, computed_mos=None):
        explicit_move_outs = [name for name in explicit_move_outs if name != 'vacant']
        cleaned_mos = []
        if explicit_move_outs != []:
            cleaned_mos = explicit_move_outs + computed_mos
        return cleaned_mos

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

    def nt_from_df(self, df, date, fill_item):
        Row = namedtuple('row', 'name unit rent mo date')
        explicit_move_outs = []
        nt_list = []
        for index, rec in df.iterrows():
            if rec['Move out'] != fill_item:
                explicit_move_outs.append(rec['Name'].lower())
            row = Row(rec['Name'].lower(), rec['Unit'], rec['Actual Rent Charge'], rec['Move out'] , datetime.datetime.strptime(date, '%Y-%m'))
            nt_list.append(row)

        return nt_list, explicit_move_outs

    def return_nt_list_with_no_vacants(self, keyword=None, nt_list=None):
        return [row for row in nt_list if row.name != keyword]
    
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

    def find_rent_roll_changes_by_comparison(self, start_set=None, end_set=None):
        '''compares list of tenants at start of month to those at end'''
        '''explicit move-outs from excel have been removed from end of month rent_roll_dict 
        and should initiated a discrepancy in the following code by making end list diverge from start list'''
        '''these tenants are explicitly marked as active='False' in the tenant table in func() deactivate_move_outs'''
        move_ins = list(end_set - start_set) # catches move in
        move_outs = list(start_set - end_set) # catches move out

        return move_ins, move_outs

    def insert_move_ins(self, move_ins=None):
        for name, unit in move_ins:
            nt = Tenant.create(tenant_name=name)
            unit = Unit.create(unit_name=unit, status='occupied', tenant=name)

            nt.save()
            unit.save()

    def deactivate_move_outs(self, move_outs=None):
        for name in move_outs:
            tenant = Tenant.get(Tenant.tenant_name == name)
            tenant.active = False
            tenant.save()

            unit_to_deactivate = Unit.get(Unit.tenant == name)
            unit_to_deactivate.delete_instance()

    '''these should be moved to a QueryX class'''
    def make_first_and_last_dates(self, date_str=None):
        dt_obj = datetime.datetime.strptime(date_str, '%Y-%m')
        dt_obj_first = dt_obj.replace(day = 1)
        dt_obj_last = dt_obj.replace(day = calendar.monthrange(dt_obj.year, dt_obj.month)[1])

        return dt_obj_first, dt_obj_last

    def get_all_tenants_beg_bal(self):
        '''returns a list of all tenants and their all time beginning balances'''
        '''does not consider active status at this point'''
        detail_beg_bal_all = [(row.tenant_name, row.amount, row.beg_bal_amount) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount, Payment.amount).join(Payment).namedtuples()] 

        return detail_beg_bal_all

    def check_db_tp_and_ntp(self, grand_total=None, dt_obj_first=None, dt_obj_last=None):
        '''checks if there are any payments in the database for the month'''
        '''contains its own assertion; this is an important part of the process'''
        all_tp = [float(rec.amount) for rec in Payment.select().
                where(Payment.date_posted >= dt_obj_first).
                where(Payment.date_posted <= dt_obj_last)]
        all_ntp = [float(rec.amount) for rec in NTPayment.select().
                where(NTPayment.date_posted >= dt_obj_first).
                where(NTPayment.date_posted <= dt_obj_last)]

        assert sum(all_ntp) + sum(all_tp) == grand_total

        return all_tp, all_ntp

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

    def get_payments_by_tenant_by_period(self, dt_obj_first=None, dt_obj_last=None):   
        '''what happens on a moveout'''
        '''why do I have to get rid of duplicates here?  THEY SHOULD NOT BE IN DATABASE TO BEGIN WITH'''
        '''for example, yancy made two payments for 18 and 279 but instead we have two payments in db for 297'''
        '''I do not want to have to filter duplicates on output'''

        payment_list_by_period = list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        Tenant.beg_bal_amount, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted >= dt_obj_first).
        where(Payment.date_posted <= dt_obj_last).
        join(Payment).namedtuples()]))

        return payment_list_by_period 

    def get_rent_charges_by_tenant_by_period(self, dt_obj_first=None, dt_obj_last=None):   
        '''what happens on a moveout'''

        charges_detail_by_period = [(rec.tenant_name, rec.rent_amount) for rec in Tenant.select(
        Tenant.tenant_name, 
        TenantRent.rent_amount,
        fn.SUM(TenantRent.rent_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(TenantRent.rent_date >= dt_obj_first).
        where(TenantRent.rent_date <= dt_obj_last).
        join(TenantRent).namedtuples()]

        return charges_detail_by_period 

    def get_end_bal_by_tenant(self, dt_obj_first=None, dt_obj_last=None):
        sum_payment_list = self.get_payments_by_tenant_by_period(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)
        end_bal_list = [(rec[0], float(rec[1]) - rec[2]) for rec in sum_payment_list]
        return end_bal_list

    def record_type_loader(self, rtype, func1, list1, hash_no):
        '''really nice for loading up the recordtypes used here'''
        for rec in rtype:
            for item in list1:
                if item[0] == rec.name:
                    setattr(rec, func1, float(item[hash_no]))

        return rtype

    def get_beg_bal_by_tenant(self):
        beg_bal_all = [(row.tenant_name, float(row.beg_bal_amount)) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount).
            namedtuples()]

        return beg_bal_all

    def net_position_by_tenant_by_month(self, dt_obj_first=None, dt_obj_last=None):

        '''am I trying to a current balance or just a snapshot, how does this play out'''
        from recordtype import recordtype # i edit the source code here, so requirements won't work if this is every published, after 3.10

        Position = recordtype('Position', 'name alltime_beg_bal payment_total charges_total end_bal start_date end_date', default=0)

        tenant_list = [name.tenant_name for name in Tenant.select()]
        position_list1 = [Position(name=name, start_date=dt_obj_first, end_date=dt_obj_last) for name in tenant_list]

        beg_bal_all = self.get_beg_bal_by_tenant()
        position_list1 = self.record_type_loader(position_list1, 'alltime_beg_bal', beg_bal_all, 1)

        payment_list_by_period = self.get_payments_by_tenant_by_period(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)        
        position_list1 = self.record_type_loader(position_list1, 'payment_total', payment_list_by_period, 2)

        charges_detail_by_period = self.get_rent_charges_by_tenant_by_period(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)
        position_list1 = self.record_type_loader(position_list1, 'charges_total', charges_detail_by_period, 1)

        for row in position_list1:
            row.end_bal = row.alltime_beg_bal + row.charges_total - row.payment_total

        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal

        return position_list1, cumsum

    def get_total_rent_charges_by_month(self, dt_obj_first=None, dt_obj_last=None):

        total_collections = sum([float(row.rent_amount) for row in TenantRent().
        select(TenantRent.rent_amount).
        where(TenantRent.rent_date >= dt_obj_first).
        where(TenantRent.rent_date <= dt_obj_last)])

        return total_collections

class Operation(PopulateTable):

    create_tables_list = [Tenant, Unit, Payment, NTPayment, TenantRent]

    path = Config.TEST_RS_PATH
    findex_db = Config.test_findex_db
    findex_tablename = Config.test_findex_name
    checkl_db = Config. test_checklist_db 
    checkl_tablename = Config.test_checklist_name
    populate = PopulateTable()
    tenant = Tenant()
    unit = Unit()
    checkl = Checklist(checkl_db, checkl_tablename)
    findex = FileIndexer(path=path, db=findex_db, table=findex_tablename, checklist_obj=checkl)
    init_cutoff_date = '2022-01'

    def run(self):
        db.connect()
        db.drop_tables(models=self.create_tables_list)
        db.create_tables(self.create_tables_list)
        self.findex.build_index_runner()
        records = self.findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']
        processed_rentr_dates_and_paths = [(item[1], item[3]) for item in rent_roll_list]
        processed_rentr_dates_and_paths.sort()

        for date, path in processed_rentr_dates_and_paths:
            nt_list, rent_roll_set, period_start_tenant_names = self.rent_roll_load_wrapper(path=path, date=date)

            # for item in Tenant().select().where(Tenant.active=='True').join(Unit):
            #     print(date, item.tenant_name)

            for item in TenantRent().select():
                print(date, 'RENT', item.tenant, item.rent_amount)

            for item in nt_list:
                print(date, item)

            # dt_obj_first, dt_obj_last = self.make_first_and_last_dates(date_str=date)
            # total_collections = self.get_total_collections_by_month(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

            # assert total_collections == 15469.0


# operation = Operation()
# operation.run()
