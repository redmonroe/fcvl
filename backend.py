import calendar
import datetime
import json
import logging
import math
import os
from calendar import monthrange
from collections import namedtuple
from datetime import datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from pprint import pprint
from sqlite3 import IntegrityError

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta
from numpy import nan
from peewee import *
from peewee import JOIN, fn
from recordtype import \
    recordtype  # i edit the source code here, so requirements won't work if this is ever published, after 3.10, collection.abc change

from config import Config
from records import record

basedir = os.path.abspath(os.path.dirname(__file__))

db = Config.TEST_DB

class BaseModel(Model):
    class Meta:
        database = db

class Tenant(BaseModel):
    tenant_name = CharField(primary_key=True, unique=True)
    active = CharField(default=True) # 
    move_in_date = DateField(default='0')
    move_out_date = DateField(default='0')
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
    tenant = ForeignKeyField(Tenant, backref='damage')
    dam_amount = CharField()
    dam_date = DateField()
    dam_type = CharField()

    @staticmethod
    def load_damages():
        damages_2022 = Config.damages_2022
        for item in damages_2022:
            for name, packet in item.items():
                dam = Damages(tenant=name, dam_amount=packet[0], dam_date=packet[1], dam_type=packet[2]) 
                dam.save()

class Payment(BaseModel):
    tenant = ForeignKeyField(Tenant, backref='payments')
    amount = CharField()
    date_posted = DateField()
    date_code = IntegerField()
    unit = CharField()
    deposit_id = IntegerField()

class OpCash(BaseModel):
    stmt_key = CharField(primary_key=True, unique=True)    
    date = DateField()
    rr = CharField(default='0')
    hap = CharField(default='0')
    dep_sum = CharField(default='0')

class OpCashDetail(BaseModel):
    stmt_key = ForeignKeyField(OpCash, backref='detail')
    date1 = DateField()
    amount = CharField(default='0')

class NTPayment(BaseModel):
    payee = CharField()
    amount = CharField()
    date_posted = DateField()
    date_code = IntegerField()
    genus = CharField(default='other')    
    deposit_id = IntegerField()

class Findexer(BaseModel):
    doc_id = AutoField()
    fn = CharField()
    file_ext = CharField(default='0')
    doc_type = CharField(default='untyped`')
    status = CharField(default='raw')
    indexed = CharField(default='false')
    period = CharField(default='0')
    hap = CharField(default='0')
    rr = CharField(default='0')
    depsum = CharField(default='0')
    deplist = CharField(default='0')
    path = CharField()

class StatusRS(BaseModel):
    status_id = AutoField()
    current_date = DateField()
    proc_file = CharField(default='0')
    # what months are ready to write
        # what is the threshold?
    # what files have been processed?
    # what is reconciling? what is not?  How do we fix this? 
    # if we are mid-month and prior months is closed then we should be able to try to use
        # nbofi mid month scrape

    def set_current_date(self, mode=None):
        if db.is_closed() == True:
            db.connect()
        if mode == 'autodrop':
            print(f'StatusRS set to {mode}')
            db.drop_tables(models=[StatusRS, StatusObject, Findexer])
            db.create_tables(models=[StatusRS, StatusObject, Findexer])
        date1 = datetime.now()
        query = StatusRS.create(current_date=date1)
        query.save()   

    def show(self, mode=None):
        most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]

        months_ytd = self.months_in_ytd()

        report_list = self.get_processed_by_month(month_list=months_ytd)

        self.write_processed_to_db(ref_rec=most_recent_status, report_list=report_list)

        if mode != 'just_asserting_empty':
            self.assert_reconcile_payments(month_list=months_ytd, ref_rec=most_recent_status)

        if most_recent_status:
            print(f'current date: {most_recent_status.current_date}\n')

        if months_ytd:
            print(f'current month: {months_ytd[-1]}')
            print(f'months ytd {Config.current_year}: {months_ytd}\n')

        if report_list:
            look_list = []
            for month, item in zip(months_ytd, report_list):
                look_dict = {fn: (tup[0], tup[1], tup[2]) for fn, tup in item.items() if month == tup[0]}
                ready_to_write_dt = self.is_ready_to_write(month=month, dict1=look_dict)
                look_list.append(look_dict)
                print(f'For period {month} these files have been processed: \n {[*look_dict.keys()]} \n Ready to Write? {[*ready_to_write_dt.values()][0]}' )
    
        return most_recent_status 

    def is_ready_to_write(self, month=None, dict1=None):
        count = 0
        ready_to_process = False
        for fn, tup in dict1.items():
            if tup[2] == 'deposits' and tup[0] == month:
                count += 1
            if tup[2] == 'rent' and tup[0] == month:
                count += 1
            if tup[2] == 'opcash' and tup[0] == month:
                count += 1

        if count == 3:
            ready_to_process = True
            send_to_write = {month: ready_to_process}
        else:
            send_to_write = {month: ready_to_process}

        return send_to_write

    def months_in_ytd(self, style=None):
        range_month = datetime.now().strftime('%m')
        str_month = datetime.now().strftime('%b').lower()
        date_info = monthrange(int(Config.current_year), int(range_month))
        last_day = date_info[1]
        
        if style == 'three_letter_month':
            month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-{range_month}-{last_day}',freq='MS').strftime("%b").tolist()
            month_list = [item.lower() for item in month_list]
        else:
            month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-{range_month}-{last_day}',freq='MS').strftime("%Y-%m").tolist()
            month_list = [item for item in month_list]

        return month_list

    def get_processed_by_month(self, month_list=None):
        report_list = []
        for month in month_list:
            reports_by_month = {rec.fn: (month, rec.path, rec.doc_type) for rec in Findexer().select().where(Findexer.period==month).where(Findexer.status=='processed').namedtuples()}
            report_list.append(reports_by_month)
        return report_list

    def write_processed_to_db(self, ref_rec=None, report_list=None):
        mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)

        dump_list = []
        for item in report_list:
            for fn, tup in item.items():
                dict1 = {}
                dict1[fn] = tup[0] 
                dump_list.append(dict1)

        mr_status.proc_file = json.dumps(dump_list)
        mr_status.save()

    def assert_reconcile_payments(self, month_list=None, ref_rec=None):
        populate = PopulateTable()
        for month in month_list:
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=month)
        
            ten_payments = sum([float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)])
            ntp = sum(populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt))
            opcash = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)

            if opcash != []:
                opcash_deposits = float(opcash[0][4])
                sum_from_payments = ten_payments + ntp

                if opcash_deposits == sum_from_payments:
                    mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)                
                    s_object = StatusObject.create(key=mr_status.status_id, month=month, processed=True, tenant_reconciled=True)
                    s_object.save()
            else:
                mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)                
                s_object = StatusObject.create(key=mr_status.status_id, month=month, processed=False, tenant_reconciled=False)
                s_object.save()

class StatusObject(BaseModel):
    key = ForeignKeyField(StatusRS, backref='zzzzzz')
    month = CharField(default='0')
    processed = BooleanField(default=False)
    tenant_reconciled = BooleanField(default=False)
        
class QueryHC():

    def make_first_and_last_dates(self, date_str=None):
        dt_obj = datetime.strptime(date_str, '%Y-%m')
        dt_obj_first = dt_obj.replace(day = 1)
        dt_obj_last = dt_obj.replace(day = calendar.monthrange(dt_obj.year, dt_obj.month)[1])

        return dt_obj_first, dt_obj_last

    def check_db_tp_and_ntp(self, grand_total=None, first_dt=None, last_dt=None):
        '''checks if there are any payments in the database for the month'''
        '''contains its own assertion; this is an important part of the process'''
        all_tp = [float(rec.amount) for rec in Payment.select().
                where(Payment.date_posted >= first_dt).
                where(Payment.date_posted <= last_dt)]
        all_ntp = [float(rec.amount) for rec in NTPayment.select().
                where(NTPayment.date_posted >= first_dt).
                where(NTPayment.date_posted <= last_dt)]

        assert sum(all_ntp) + sum(all_tp) == grand_total

        return all_tp, all_ntp

    def check_for_multiple_payments(self, detail_beg_bal_all=None, first_dt=None, last_dt=None):
        pay_names = [row.tenant for row in Payment().
                select().
                where(Payment.date_posted >= first_dt).
                where(Payment.date_posted <= last_dt).
                join(Tenant).namedtuples()]
        if len(pay_names) != len(set(pay_names)):
            different_names = [name for name in pay_names if pay_names.count(name) > 1]
            return different_names
        return []

    def get_all_tenants_beg_bal(self):
        '''returns a list of all tenants and their all time beginning balances'''
        '''does not consider active status at this point'''
        return [(row.tenant_name, row.amount, row.beg_bal_amount) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount, Payment.amount).join(Payment).namedtuples()] 

    def get_beg_bal_sum_by_period(self, style=None, first_dt=None, last_dt=None):
        if style == 'initial':
            sum_beg_bal_all = [float(row.beg_bal_amount) for row in Tenant.select(
                Tenant.active, Tenant.beg_bal_amount).
                where(Tenant.active=='True').
                namedtuples()]     
        else:
            sum_beg_bal_all = [float(row.beg_bal_amount) for row in Tenant.select(
                Tenant.active, Tenant.beg_bal_amount, Payment.date_posted).
                where(Payment.date_posted >= first_dt).
                where(Payment.date_posted <= last_dt).
                where(Tenant.active=='True').
                join(Payment).namedtuples()]      

        return sum(sum_beg_bal_all)

    def get_end_bal_by_tenant(self, first_dt=None, last_dt=None):
        sum_payment_list = self.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)
        return [(rec[0], float(rec[1]) - rec[2]) for rec in sum_payment_list]

    def get_beg_bal_by_tenant(self):
        return [(row.tenant_name, float(row.beg_bal_amount)) for row in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount).
            namedtuples()]

    def get_opcash_by_period(self, first_dt=None, last_dt=None):
        return [(row.stmt_key, row.date, row.rr, row.hap, row.dep_sum) for row in OpCash.select(OpCash.stmt_key, OpCash.date, OpCash.rr, OpCash.hap, OpCash.dep_sum).
        where(OpCash.date >= first_dt).
        where(OpCash.date <= last_dt).namedtuples()]

    def get_opcashdetail_by_stmt(self, stmt_key=None):
        return [row for row in OpCashDetail.select().join(OpCash).where(OpCashDetail.stmt_key == stmt_key).namedtuples()]

    def match_tp_db_to_df(self, df=None, first_dt=None, last_dt=None):
        sum_this_month_db = sum([float(row.amount) for row in 
            Payment.select(Payment.amount).
            where(Payment.date_posted >= first_dt).
            where(Payment.date_posted <= last_dt)])

        sum_this_month_df = sum(df['amount'].astype(float).tolist())
        assert sum_this_month_db == sum_this_month_df

        return sum_this_month_db, sum_this_month_df

    def get_payments_by_tenant_by_period(self, first_dt=None, last_dt=None):   
        '''what happens on a moveout'''
        '''why do I have to get rid of duplicates here?  THEY SHOULD NOT BE IN DATABASE TO BEGIN WITH'''
        '''for example, yancy made two payments for 18 and 279 but instead we have two payments in db for 297'''
        '''I do not want to have to filter duplicates on output'''

        return list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        Tenant.beg_bal_amount, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted >= first_dt).
        where(Payment.date_posted <= last_dt).
        join(Payment).namedtuples()]))

    def get_ntp_by_period(self, first_dt=None, last_dt=None):   
        return list([float(rec.amount) for rec in NTPayment().
        select(NTPayment.amount).
        where(NTPayment.date_posted >= first_dt).
        where(NTPayment.date_posted <= last_dt).
        namedtuples()])

    def get_rent_charges_by_tenant_by_period(self, first_dt=None, last_dt=None):   
        '''what happens on a moveout'''
        return [(rec.tenant_name, rec.rent_amount) for rec in Tenant.select(
        Tenant.tenant_name, 
        TenantRent.rent_amount,
        fn.SUM(TenantRent.rent_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(TenantRent.rent_date >= first_dt).
        where(TenantRent.rent_date <= last_dt).
        join(TenantRent).namedtuples()]

    def get_total_rent_charges_by_month(self, first_dt=None, last_dt=None):
        return sum([float(row.rent_amount) for row in TenantRent().
        select(TenantRent.rent_amount).
        where(TenantRent.rent_date >= first_dt).
        where(TenantRent.rent_date <= last_dt)])

    def record_type_loader(self, rtype, func1, list1, hash_no):
        '''really nice for loading up the recordtypes used here'''
        for rec in rtype:
            for item in list1:
                if item[0] == rec.name:
                    setattr(rec, func1, float(item[hash_no]))
        return rtype

    def sum_lifetime_tenant_payments(self, dt_obj_last=None):
        '''ugly workaround hidden in here: yancy double pay fix, prevents real lifetime balance from getting through'''
        return list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        Tenant.beg_bal_amount, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted <= dt_obj_last).
        join(Payment).namedtuples()]))

    def sum_lifetime_tenant_charges(self, dt_obj_last=None):
        return [(rec.tenant_name, rec.total_charges) for rec in Tenant.select(
        Tenant.tenant_name, 
        TenantRent.rent_amount,
        fn.SUM(TenantRent.rent_amount).over(partition_by=[Tenant.tenant_name]).alias('total_charges')).
        where(TenantRent.rent_date <= dt_obj_last).
        join(TenantRent).namedtuples()]

    def sum_lifetime_tenant_damages(self, dt_obj_last=None):
        return [(rec.tenant_name, rec.total_damages) for rec in Tenant.select(
        Tenant.tenant_name, 
        Damages.dam_amount,
        fn.SUM(Damages.dam_amount).over(partition_by=[Tenant.tenant_name]).alias('total_damages')).
        where(Damages.dam_date <= dt_obj_last).
        join(Damages).namedtuples()]

    def net_position_by_tenant_by_month(self, first_dt=None, last_dt=None, after_first_month=None):
        '''heaviest business logic here'''
        '''returns relatively hefty object with everything you'd need to write the report/make the sheets'''
        '''model we are using now is do alltime payments, charges, and alltime beg_bal'''

        Position = recordtype('Position', 'name alltime_beg_bal payment_total charges_total damages_total end_bal start_date end_date', default=0)

        tenant_list = [name.tenant_name for name in Tenant.select()]
        position_list1 = [Position(name=name, start_date=first_dt, end_date=last_dt) for name in tenant_list]

        alltime_beg_bal = self.get_beg_bal_by_tenant() # ALLTIME STARING BEG BALANCES

        position_list1 = self.record_type_loader(position_list1, 'alltime_beg_bal', alltime_beg_bal, 1)

        all_tenant_payments_by_tenant = self.sum_lifetime_tenant_payments(dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(position_list1, 'payment_total', all_tenant_payments_by_tenant, 2)

        all_tenant_charges_by_tenant = self.sum_lifetime_tenant_charges(dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(position_list1, 'charges_total', all_tenant_charges_by_tenant, 1)

        all_tenant_damages_by_tenant = self.sum_lifetime_tenant_damages(dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'damages_total', all_tenant_damages_by_tenant, 1)

        '''this is the work right here'''
        for row in position_list1:
            row.end_bal = row.alltime_beg_bal + row.charges_total + row.damages_total - row.payment_total

        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal
       
        return position_list1, cumsum

class PopulateTable(QueryHC):

    ntp_cases = ['laundry', 'insurance', 'other', 'sd']

    def init_tenant_load(self, filename=None, date=None):
        nt_list, total_tenant_charges, explicit_move_outs = self.init_load_ten_unit_ten_rent(filename=filename, date=date)

        return nt_list, total_tenant_charges, explicit_move_outs

    def init_load_ten_unit_ten_rent(self, filename=None, date=None):
        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)
        nt_list, explicit_move_outs = self.nt_from_df(df=df, date=date, fill_item=fill_item)

        first_dt, last_dt = self.make_first_and_last_dates(date_str=date)

        total_tenant_charges = float(((nt_list.pop(-1)).rent).replace(',', ''))

        nt_list = self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)

        ten_insert_many = [{'tenant_name': row.name, 'move_in_date': datetime.strptime(row.mi_date, '%m/%d/%Y')} for row in nt_list]

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

        period_start_tenant_names = [(name.tenant_name, name.unit_name, datetime(name.move_in_date.year, name.move_in_date.month, name.move_in_date.day)) for name in Tenant.select(Tenant.tenant_name, Tenant.move_in_date, Unit.unit_name).where(Tenant.active==True).join(Unit).namedtuples()]
        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)
        nt_list, explicit_move_outs = self.nt_from_df(df=df, date=date, fill_item=fill_item)

        total_tenant_charges = float(((nt_list.pop(-1)).rent).replace(',', ''))

        period_end_tenant_names = [(row.name, row.unit, datetime.strptime(row.mi_date, '%m/%d/%Y')) for row in self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)]

        '''we could get move-ins if move-date is in month range'''

        computed_mis, computed_mos = self.find_rent_roll_changes_by_comparison(start_set=set(period_start_tenant_names), end_set=set(period_end_tenant_names))
    
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
        tenant_payment_df, ntp_df = self.return_and_remove_ntp(df=df, col='unit', remove_str=0, drop_col='description')

        ntp_sum = sum(ntp_df['amount'].astype(float).tolist())  # can split up ntp further here

        if len(ntp_df) > 0:
            insert_nt_list = self.ntp_split(ntp_df=ntp_df)
            query = NTPayment.insert_many(insert_nt_list)
            query.execute()
        
        insert_many_list = [{
            'tenant': name.lower(),
            'amount': amount, 
            'date_posted': datetime.strptime(date_posted, '%m/%d/%Y'),  
            'date_code': date_code, 
            'unit': unit, 
            'deposit_id': deposit_id, 
             } for (deposit_id, unit, name, date_posted, amount, date_code) in tenant_payment_df.values]

        query = Payment.insert_many(insert_many_list)
        query.execute()

        return grand_total, ntp_sum, tenant_payment_df

    def ntp_split(self, ntp_df=None):

        '''4 types that cover 99% of cases:
            - payments from current tenants
            - laundry
            - insurance
            - sd(if not sure, use other)
            - other
            '''

        insert_iter = []
        for (deposit_id, genus, name, date_posted, amount, date_code, description) in ntp_df.values:
            for item in description.split(' '):
                if item in self.ntp_cases:
                    insert_iter.append({
                        'payee': description.lower(), 
                        'amount': amount, 
                        'date_posted': datetime.strptime(date_posted, '%m/%d/%Y'),  
                        'date_code': date_code, 
                        'genus': description, 
                        'deposit_id': deposit_id, 
                        })

        return insert_iter

    def read_excel_payments(self, path):
        df = pd.read_excel(path, header=9)

        columns = ['deposit_id', 'unit', 'name', 'date_posted', 'amount', 'date_code', 'description']
        
        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        description = df['Description'].str.lower().tolist()
        dt_code = [datetime.strptime(item, '%m/%d/%Y') for item in date if type(item) == str]
        dt_code = [str(datetime.strftime(item, '%m')) for item in dt_code]

        zipped = zip(bde, unit, name, date, pay, dt_code, description)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def nt_from_df(self, df, date, fill_item):
        Row = namedtuple('row', 'name unit rent mo date mi_date')
        explicit_move_outs = []
        nt_list = []
        for index, rec in df.iterrows():
            if rec['Move out'] != fill_item:
                explicit_move_outs.append(rec['Name'].lower())
            row = Row(rec['Name'].lower(), rec['Unit'], rec['Actual Rent Charge'], rec['Move out'] , datetime.strptime(date, '%Y-%m'), rec['Move in'])
            nt_list.append(row)
  
        return nt_list, explicit_move_outs

    def return_nt_list_with_no_vacants(self, keyword=None, nt_list=None):
        return [row for row in nt_list if row.name != keyword]
    
    def grand_total(self, df):
        return sum(df['amount'].astype(float).tolist())

    def return_and_remove_ntp(self, df, col=None, remove_str=None, drop_col=None):
        ntp_item = df.loc[df[col] == remove_str]
        for item in ntp_item.index:
            df.drop(labels=item, inplace=True)

        df = df.drop(drop_col, axis=1)
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
        for name, unit, move_in_date in move_ins:
            nt = Tenant.create(tenant_name=name, move_in_date=move_in_date)
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

    def transfer_opcash_to_db(self, file_list=None):
        for item in file_list:
            oc = OpCash.create(stmt_key=item[0], date=datetime.strptime(item[1], '%Y-%m'), rr=item[4], hap=item[3], dep_sum=item[5])
            oc.save()

            for lst in json.loads(item[6])[0]:
                ocd = OpCashDetail.create(stmt_key=item[0], date1=datetime.strptime(lst[0], '%m/%d/%Y'), amount=lst[1])
                ocd.save()

