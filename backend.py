import calendar
import datetime
import json
import logging
import math
import os
from calendar import monthrange
from collections import namedtuple
from datetime import datetime, timedelta
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
from letters import Letters
from records import record
from utils import Utils

basedir = os.path.abspath(os.path.dirname(__file__))

db = Config.TEST_DB

class BaseModel(Model):
    class Meta:
        database = db

class Tenant(BaseModel):
    tenant_name = CharField(primary_key=True) # removed unique = True
    active = CharField(default=True) # 
    move_in_date = DateField(default='0')
    move_out_date = DateField(default='0')
    beg_bal_date = DateField(default='2022-01-01')
    beg_bal_amount = DecimalField(default=0.00)
    unit = CharField(default='0')

class Unit(BaseModel):
    unit_name = CharField(unique=True)
    status = CharField(default='occupied') 
    tenant = CharField(default='vacant')
    last_occupied = DateField(default='0')

    @staticmethod
    def find_vacants():
        vacant_units = [name.unit_name for name in Unit.select().order_by(Unit.unit_name).where(Unit.status=='vacant').namedtuples()]
        return vacant_units

    @staticmethod
    def get_all_units():
        return [name.unit_name for name in Unit.select()]

    def get_all_occupied_units_at_date(**kwargs):
        all_units = [(name.tenant, name.unit_name, name.last_occupied) for name in Unit.select().
            where(Unit.last_occupied==kwargs['last_dt']).
            order_by(Unit.unit_name).namedtuples()]
        return all_units 

class TenantRent(BaseModel):
    t_name = ForeignKeyField(Tenant, backref='charge')
    unit = CharField()
    rent_amount = DecimalField(default=0.00)
    rent_date = DateField()

class MoveIn(BaseModel):
    mi_date = DateField('0')
    name = CharField(default='move_in_name')

class IncomeMonth(BaseModel):
    year = CharField()
    month = CharField()
    hap = CharField()

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

class Subsidy(BaseModel):
    tenant = ForeignKeyField(Tenant, backref='subsidies')
    sub_amount = CharField()
    date_posted = DateField()

class LP_EndBal(BaseModel):
    tenant = ForeignKeyField(Tenant, backref='lp_endbal')
    sub_amount = CharField()
    date_posted = DateField()

class ContractRent(BaseModel):
    tenant = ForeignKeyField(Tenant, backref='contract_rent')
    sub_amount = CharField()
    date_posted = DateField()

class OpCash(BaseModel):
    stmt_key = CharField(primary_key=True, unique=True)    
    date = DateField()
    rr = CharField(default='0')
    hap = CharField(default='0')
    dep_sum = CharField(default='0')
    corr_sum = CharField(default='0')

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
    recon = CharField(default='no')
    indexed = CharField(default='false')
    c_date = CharField(default='0')
    period = CharField(default='0')
    corr_sum = CharField(default='0')
    hap = CharField(default='0')
    rr = CharField(default='0')
    depsum = CharField(default='0')
    deplist = CharField(default='0')
    path = CharField()

class BalanceLetter(BaseModel):
    letter_id = AutoField()
    target_month_end = DateField()
    tenant = ForeignKeyField(Tenant, backref='b_letter')
    end_bal = CharField()

class StatusRS(BaseModel):
    status_id = AutoField()
    current_date = DateField()
    proc_file = CharField(default='0') 

class StatusObject(BaseModel):
    key = ForeignKeyField(StatusRS, backref='zzzzzz')
    month = CharField(default='0')
    opcash_processed = BooleanField(default=False)
    tenant_reconciled = BooleanField(default=False)
    scrape_reconciled = BooleanField(default=False)
    rs_reconciled = BooleanField(default=False)

class ScrapeDetail(BaseModel):
    period = CharField(default='0')
    scrape_date = DateField('0')
    scrape_dep_date = DateField('0')
    amount = CharField(default='0')
    dep_type = CharField(default='undef_str')

class Mentry(BaseModel):
    obj_type = CharField(default='0')
    ch_type = CharField(default='0')
    change_time = DateField()
    original_item = CharField(default='0')

class Reconciler:

    def findex_reconcile_onesite_deposits_to_scrape_or_oc(self):
        """this func exists to perform a relatively complicated reconciliation between deposits.xls from onesite and the scrape excel sheet and/or the monthly opcash

        we need to determine whether it makes more sense to change scrape payment summary (should find 'amount' col) or to add back the amount if scrape & opcash amounts agree

        """
        deposits_xls = self.findex_reconcile_helper(typ='deposits')
        scrapes = self.findex_reconcile_helper(typ='scrape')
        opcashes = self.findex_reconcile_helper(typ='opcash')

        scrape_match = self.findex_iteration_helper(list1=deposits_xls, list2=scrapes, target_str='0', fill_str='empty')
        opcash_match = self.findex_iteration_helper(list1=deposits_xls, list2=opcashes, target_str='0', fill_str='empty')

        # find failed deposits reports from onesite; these give scrape nothing to reconcile against and should fail
        dep_xls_prob = [item for item in scrape_match if item[1] == 'empty']
        
        # remove failed deposits reports   
        opcash_match = [item for item in opcash_match if item[1] != 'empty']
        scrape_match = [item for item in scrape_match if item[1] != 'empty']
                
        match_both = list(set(scrape_match).intersection(set(opcash_match)))
        scrape_only = list(set(scrape_match).difference(opcash_match))
        opcash_only = list(set(opcash_match).difference(scrape_match))

        self.findex_reconcile_helper_writer(list1=scrape_only, recon_str='scrape_only')
        self.findex_reconcile_helper_writer(list1=opcash_only, recon_str='opcash_only')
        self.findex_reconcile_helper_writer(list1=dep_xls_prob, recon_str='dep_prob')
        self.findex_reconcile_helper_writer(list1=match_both, recon_str='FULL')


    def findex_reconcile_helper_writer(self, list1=None, recon_str=None):
        for item in list1:
            deposit_id = [(row.doc_id, row.period) for row in Findexer.select().
                where(Findexer.doc_type == 'deposits').
                where(Findexer.period == item[0]).namedtuples()][0]
            deposit = Findexer.get(deposit_id[0])
            deposit.recon = recon_str
            deposit.save()        
        
    def findex_reconcile_helper(self, typ=None):
        return [(row.period, row.depsum) for row in Findexer.select().
            where(Findexer.doc_type == typ).namedtuples()]
    
    def findex_iteration_helper(self, list1=None, list2=None, target_str=None, fill_str=None):
        return_list = []
        for item in list1:
            for it in list2:
                if item[0] == it[0]:
                    if item[1] == target_str:
                        return_list.append((item[0], fill_str))
                    else:
                        return_list.append((item[0], item[1]))
        return return_list
    
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

    

class QueryHC(Reconciler):

    def return_tables_list(self):
        return [Mentry, IncomeMonth, LP_EndBal, ContractRent, Subsidy, BalanceLetter, StatusRS, StatusObject, OpCash, OpCashDetail, Damages, Tenant, Unit, Payment, NTPayment, TenantRent, Findexer, ScrapeDetail, MoveIn]

    def make_first_and_last_dates(self, date_str=None):
        dt_obj = datetime.strptime(date_str, '%Y-%m')
        dt_obj_first = dt_obj.replace(day = 1)
        dt_obj_last = dt_obj.replace(day = calendar.monthrange(dt_obj.year, dt_obj.month)[1])

        return dt_obj_first, dt_obj_last

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

    def ur_query_wrapper(self):

        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str='2022-01')
        populate.ur_query(model='Tenant', query_fields={'move_in_date': first_dt, 'move_out_date': last_dt})

    def ur_query(self, model=None, query_fields=None):
        # data = {'field_a': 1, 'field_b': 33, 'field_c': 'test'}
        import operator
        import sys
        from functools import reduce
        model = getattr(sys.modules[__name__], model)
        clauses = []
        for key, value in query_fields.items():
            field = model._meta.fields[key]
            clauses.append(field == value)

        expr = reduce(operator.and_, clauses) # from itertools import reduce in Py3
        breakpoint()

    def get_all_status_objects(self):
        return [row for row in StatusObject.select().namedtuples()]

    def get_all_findexer_by_type(self, type1=None):
        rows = [(row.fn, row.period) for row in Findexer.select().where(Findexer.doc_type == type1).namedtuples()]
        return rows

    def get_all_findexer_recon_status(self, type1=None):
        rows = [(row.recon, row.period) for row in Findexer.select().where(Findexer.doc_type == type1).namedtuples()]
        return rows

    def get_all_tenants_beg_bal(self, cumsum=False):
        '''returns a list of all tenants and their all time beginning balances'''
        '''does not consider active status at this point'''
        return [row.beg_bal_amount for row in Tenant.select(
            fn.SUM(Tenant.beg_bal_amount)).
            namedtuples()][0] 

    def get_current_tenants_by_month(self, last_dt=None, first_dt=None):
        '''must be a tenant on first day of month; midmonths move-ins need an adjustment'''
        tenants = [row.tenant_name for row in Tenant.select().
                    where(
                        (Tenant.move_in_date <= first_dt) |                
                        (Tenant.move_out_date >=last_dt))
                .namedtuples()]
        return tenants

    def get_current_vacants_by_month(self, last_dt=None, first_dt=None):
        return[(row.tenant, row.unit_name) for row in Unit.select().order_by(Unit.unit_name).where(
        (Unit.last_occupied=='0')        
        ).namedtuples()]

    def get_rent_roll_by_month_at_first_of_month_basic(self, last_dt=None, first_dt=None):
        current_tenants = [(row.tenant_name, row.move_in_date) for row in Tenant().select().
            where(
                (Tenant.move_in_date <= first_dt) &
                ((Tenant.move_out_date=='0') | (Tenant.move_out_date>=first_dt)))
                .namedtuples()]
        return current_tenants

    def get_rent_roll_by_month_at_first_of_month(self, first_dt=None, last_dt=None):
        '''lots of work in this func'''
        tenants_mi_on_or_before_first = [(rec.tenant_name, rec.unit) for rec in Tenant().select(Tenant, Unit).
            join(Unit, JOIN.LEFT_OUTER, on=(Tenant.tenant_name==Unit.tenant)).
            where(
            (Tenant.move_in_date <= first_dt) &
            ((Tenant.move_out_date=='0') | (Tenant.move_out_date>=first_dt))).
            namedtuples()]

        occupied_units = [unit for (name, unit) in tenants_mi_on_or_before_first]

        all_units = Unit.get_all_units()

        for vacant_unit in set(all_units) - set(occupied_units):
            tup = ('vacant',  vacant_unit)
            tenants_mi_on_or_before_first.append(tup)

        vacants = [item for item in tenants_mi_on_or_before_first if item[0] == 'vacant']
        tenants = [item[0] for item in tenants_mi_on_or_before_first if item[0] != 'vacant']

        return tenants_mi_on_or_before_first, vacants, tenants

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

    def get_damages_by_month(self, first_dt=None, last_dt=None):
        damages = [(row.tenant, row.dam_amount, row.dam_date, row.dam_type) for row in Damages().select().
        where(Damages.dam_date>=first_dt).
        where(Damages.dam_date<=last_dt).
        namedtuples()]
        return damages

    def consolidated_get_stmt_by_month(self, first_dt=None, last_dt=None):
        opcash_sum = self.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        opcash_detail = self.get_opcashdetail_by_stmt(stmt_key=opcash_sum[0][0])
        return opcash_sum, opcash_detail

    def get_opcash_sum_by_period(self, first_dt=None, last_dt=None):
        return [row.dep_sum for row in OpCash.select().
        where(OpCash.date >= first_dt).
        where(OpCash.date <= last_dt).namedtuples()]
    
    def get_opcash_by_period(self, first_dt=None, last_dt=None):
        return [(row.stmt_key, row.date, row.rr, row.hap, row.dep_sum, row.corr_sum) for row in OpCash.select(OpCash.stmt_key, OpCash.date, OpCash.rr, OpCash.hap, OpCash.dep_sum, OpCash.corr_sum).
        where(OpCash.date >= first_dt).
        where(OpCash.date <= last_dt).namedtuples()]

    def get_opcashdetail_by_stmt(self, stmt_key=None):
        return [row for row in OpCashDetail.select().join(OpCash).where(OpCashDetail.stmt_key == stmt_key).namedtuples()]

    def get_move_ins_by_period(self, first_dt=None, last_dt=None):
        recs = [(row.mi_date, row.name) for row in MoveIn.select().where(MoveIn.mi_date>= first_dt).
        where(MoveIn.mi_date <= last_dt).namedtuples()]
        return recs 

    def get_scrape_detail_by_month_deposit(self, first_dt=None, last_dt=None):
        recs = [row for row in ScrapeDetail.select().where(ScrapeDetail.scrape_dep_date >= first_dt).
        where(ScrapeDetail.scrape_dep_date <= last_dt).
        where(ScrapeDetail.dep_type=='deposit').namedtuples()]
        return recs

    def get_scrape_detail_by_month_by_type(self, type1, first_dt=None, last_dt=None):
        recs = [row.amount for row in ScrapeDetail.select().where(ScrapeDetail.scrape_dep_date >= first_dt).
        where(ScrapeDetail.scrape_dep_date <= last_dt).
        where(ScrapeDetail.dep_type==type1).namedtuples()]
        return recs

    def get_status_object_by_month(self, first_dt=None, last_dt=None):
        month = first_dt.strftime('%m')
        year = first_dt.year
        period = str(year) + '-' + str(month)
        return [{'opcash_processed': item.opcash_processed, 'tenant_reconciled': item.tenant_reconciled, 'scrape_reconciled': item.scrape_reconciled} for item in StatusObject().select().where(StatusObject.month==period).namedtuples()]

    def get_processed_by_month(self, month_list=None):
        """returns list of dicts from Findexer of files that have been attribute 'processed' and packaged with month, path, and report_type(ie 'rent', 'deposits')"""
        report_list = []
        for month in month_list:
            reports_by_month = {rec.fn: (month, rec.path, rec.doc_type) for rec in Findexer().select().where(Findexer.period==month).where(Findexer.status=='processed').namedtuples()}
            report_list.append(reports_by_month)
        return report_list

    def match_tp_db_to_df(self, df=None, first_dt=None, last_dt=None):
        sum_this_month_db = sum([float(row.amount) for row in 
            Payment.select(Payment.amount).
            where(Payment.date_posted >= first_dt).
            where(Payment.date_posted <= last_dt)])

        sum_this_month_df = sum(df['amount'].astype(float).tolist())
        assert sum_this_month_db == sum_this_month_df

        return sum_this_month_db, sum_this_month_df

    def get_balance_letters_by_month(self, first_dt=None, last_dt=None):
        return [item.tenant_id for item in BalanceLetter().select().where(BalanceLetter.target_month_end==last_dt).namedtuples()]

    def get_payments_by_tenant_by_period(self, first_dt=None, last_dt=None, cumsum=None):   
        '''what happens on a moveout'''
        '''why do I have to get rid of duplicates here?  THEY SHOULD NOT BE IN DATABASE TO BEGIN WITH'''
        '''for example, yancy made two payments for 18 and 279 but instead we have two payments in db for 297'''
        '''I do not want to have to filter duplicates on output'''
        if cumsum:
            payments = list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
            Tenant.tenant_name, 
            Tenant.beg_bal_amount, 
            fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
            where(Payment.date_posted >= first_dt).
            where(Payment.date_posted <= last_dt).
            join(Payment).namedtuples()]))
            return sum([float(item[2]) for item in payments])
        else:
            return list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
            Tenant.tenant_name, 
            Tenant.beg_bal_amount, 
            fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
            where(Payment.date_posted >= first_dt).
            where(Payment.date_posted <= last_dt).
            join(Payment).namedtuples()]))

    def get_single_ten_pay_by_period(self, first_dt=None, last_dt=None, name=None):
        ten_pay_tup =  [(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted >= first_dt).
        where(Payment.date_posted <= last_dt).
        where(Tenant.tenant_name==name).
        join(Payment).namedtuples()][0]
        return ten_pay_tup

    def get_ntp_by_period(self, first_dt=None, last_dt=None):   
        return list([float(rec.amount) for rec in NTPayment().
        select(NTPayment.amount).
        where(NTPayment.date_posted >= first_dt).
        where(NTPayment.date_posted <= last_dt).
        namedtuples()])

    def get_ntp_by_period_and_type(self, first_dt=None, last_dt=None):
        return list([(float(rec.amount), rec.genus) for rec in NTPayment().
        select().
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

    def tenant_payments_this_period(self, first_dt=None, last_dt=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Payment.date_posted >= first_dt).
        where(Payment.date_posted <= last_dt).
        join(Payment).namedtuples()]))

    def tenant_last_endbal_this_period(self, first_dt=None, last_dt=None):
        last_dt = first_dt - timedelta(days=1)
        first_dt = last_dt.replace(day=1)
        return list(set([(rec.tenant_name, rec.sub_amount) for rec in Tenant.select(
        Tenant.tenant_name, LP_EndBal.sub_amount).
        where(LP_EndBal.date_posted >= first_dt).
        where(LP_EndBal.date_posted <= last_dt).
        join(LP_EndBal).namedtuples()]))

    def sum_lifetime_subsidy(self, dt_obj_last=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(Subsidy.sub_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Subsidy.date_posted <= dt_obj_last).
        join(Subsidy).namedtuples()]))

    def sum_lifetime_contract_rent(self, dt_obj_last=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(ContractRent.sub_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(ContractRent.date_posted <= dt_obj_last).
        join(ContractRent).namedtuples()]))

    def sum_lifetime_tenant_charges(self, dt_obj_last=None):
        return [(rec.tenant_name, rec.total_charges) for rec in Tenant.select(
        Tenant.tenant_name, 
        TenantRent.rent_amount,
        fn.SUM(TenantRent.rent_amount).over(partition_by=[Tenant.tenant_name]).alias('total_charges')).
        where(TenantRent.rent_date <= dt_obj_last).
        join(TenantRent).namedtuples()]
  
    def tenant_charges_this_period(self, first_dt=None, last_dt=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(TenantRent.rent_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(TenantRent.rent_date >= first_dt).
        where(TenantRent.rent_date <= last_dt).
        join(TenantRent).namedtuples()]))

    def tenant_damages_this_period(self, first_dt=None, last_dt=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(Damages.dam_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Damages.dam_date >= first_dt).
        where(Damages.dam_date <= last_dt).
        join(Damages).namedtuples()]))

    def subsidy_this_period(self, first_dt=None, last_dt=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(Subsidy.sub_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(Subsidy.date_posted >= first_dt).
        where(Subsidy.date_posted <= last_dt).
        join(Subsidy).namedtuples()]))

    def contract_this_period(self, first_dt=None, last_dt=None):
        return list(set([(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
        Tenant.tenant_name, 
        fn.SUM(ContractRent.sub_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
        where(ContractRent.date_posted >= first_dt).
        where(ContractRent.date_posted <= last_dt).
        join(ContractRent).namedtuples()]))

    def sum_lifetime_tenant_damages(self, dt_obj_last=None):
        return [(rec.tenant_name, rec.total_damages) for rec in Tenant.select(
        Tenant.tenant_name, 
        Damages.dam_amount,
        fn.SUM(Damages.dam_amount).over(partition_by=[Tenant.tenant_name]).alias('total_damages')).
        where(Damages.dam_date <= dt_obj_last).
        join(Damages).namedtuples()]

    def full_month_position_tenant_by_month(self, first_dt=None, last_dt=None):
        Position = recordtype('Position', 'name alltime_beg_bal lp_endbal payment_total charges_total damages_total end_bal start_date end_date unit subsidy contract_rent' , default=0)

        rr, vacants, tenants = self.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)
        position_list1 = [Position(name=item[0], start_date=first_dt, end_date=last_dt, unit=item[1]) for item in rr]

        alltime_beg_bal = self.get_beg_bal_by_tenant() # ALLTIME STARING BEG BALANCE
        position_list1 = self.record_type_loader(position_list1, 'alltime_beg_bal', alltime_beg_bal, 1)

        if str(first_dt.year) + '-' + str(first_dt.month) == '2022-1':
            positions_list1 = self.record_type_loader(position_list1, 'lp_endbal', alltime_beg_bal, 1)
        else:
            last_endbal_by_tenant = self.tenant_last_endbal_this_period(first_dt=first_dt, last_dt=last_dt)
            position_list1 = self.record_type_loader(position_list1, 'lp_endbal', last_endbal_by_tenant, 1)

        payments_by_tenant = self.tenant_payments_this_period(first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'payment_total', payments_by_tenant, 1)
        
        charges_by_tenant = self.tenant_charges_this_period(first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'charges_total', charges_by_tenant, 1)

        damages_by_tenant = self.tenant_damages_this_period(first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'damages_total', damages_by_tenant, 1)

        subsidy_by_tenant = self.subsidy_this_period(first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'subsidy', subsidy_by_tenant, 1)

        contract_by_tenant = self.contract_this_period(first_dt=first_dt, last_dt=last_dt)
        
        position_list1 = self.record_type_loader(position_list1, 'contract_rent', contract_by_tenant, 1)
  
        '''this is the work right here: do I want to put output in a database?'''
        for row in position_list1:
            row.end_bal = row.lp_endbal + row.charges_total + row.damages_total - row.payment_total
            if row.name != 'vacant':
                lp_end_bal = LP_EndBal.create(tenant=row.name, sub_amount=row.end_bal, date_posted=last_dt )
                lp_end_bal.save()
        
        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal
       
        return position_list1, cumsum

    def net_position_by_tenant_by_month(self, first_dt=None, last_dt=None, after_first_month=None):
        '''heaviest business logic here'''
        '''returns relatively hefty object with everything you'd need to write the report/make the sheets'''
        '''model we are using now is do alltime payments, charges, and alltime beg_bal'''

        Position = recordtype('Position', 'name alltime_beg_bal payment_total charges_total damages_total end_bal start_date end_date unit subsidy contract_rent' , default=0)

        rr, vacants, tenants = self.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)
        position_list1 = [Position(name=item[0], start_date=first_dt, end_date=last_dt, unit=item[1]) for item in rr]

        alltime_beg_bal = self.get_beg_bal_by_tenant() # ALLTIME STARING BEG BALANCES

        position_list1 = self.record_type_loader(position_list1, 'alltime_beg_bal', alltime_beg_bal, 1)

        all_tenant_payments_by_tenant = self.sum_lifetime_tenant_payments(dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(position_list1, 'payment_total', all_tenant_payments_by_tenant, 2)

        all_tenant_charges_by_tenant = self.sum_lifetime_tenant_charges(dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(position_list1, 'charges_total', all_tenant_charges_by_tenant, 1)

        all_tenant_damages_by_tenant = self.sum_lifetime_tenant_damages(dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'damages_total', all_tenant_damages_by_tenant, 1)

        all_subsidy_by_tenant = self.sum_lifetime_subsidy(dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'subsidy', all_subsidy_by_tenant, 1)

        all_krent_by_tenant = self.sum_lifetime_contract_rent(dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(position_list1, 'contract_rent', all_krent_by_tenant, 1)
  
        '''this is the work right here: do I want to put output in a database?'''
        for row in position_list1:
            row.end_bal = row.alltime_beg_bal + row.charges_total + row.damages_total - row.payment_total

        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal
       
        return position_list1, cumsum

class PopulateTable(QueryHC):

    def init_tenant_load(self, filename=None, date=None):
        nt_list, total_tenant_charges, explicit_move_outs = self.init_load_ten_unit_ten_rent(filename=filename, date=date)

        return nt_list, total_tenant_charges, explicit_move_outs

    def init_load_ten_unit_ten_rent(self, filename=None, date=None):
        fill_item = '0'
        df = pd.read_excel(filename, header=16)
        df = df.fillna(fill_item)
        nt_list_w_vacants, explicit_move_outs = self.nt_from_df(df=df, date=date, fill_item=fill_item)

        first_dt, last_dt = self.make_first_and_last_dates(date_str=date)

        total_tenant_charges = float(((nt_list_w_vacants.pop(-1)).rent).replace(',', ''))

        nt_list = self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list_w_vacants)

        ten_insert_many = [{'tenant_name': row.name, 'move_in_date': datetime.strptime(row.mi_date, '%m/%d/%Y'), 'unit': row.unit} for row in nt_list]

        units_insert_many = []
        for row in nt_list_w_vacants:
            if row.name == 'vacant':
                dict1 = {'unit_name': row.unit, 'status': 'vacant'}
            else:
                dict1 = {'unit_name': row.unit, 'tenant': row.name, 'last_occupied': last_dt}
            units_insert_many.append(dict1)

        # should include write to move-in even though no move'in in jan

        rent_insert_many = [{'t_name': row.name, 'unit': row.unit, 'rent_amount': row.rent.replace(',',''), 'rent_date': row.date} for row in nt_list if row.name != 'vacant']  

        subs_insert_many = [{'tenant': row.name, 'sub_amount': row.subsidy.replace(',', ''), 'date_posted': row.date} for row in nt_list if row.name != 'vacant']

        krent_insert_many = [{'tenant': row.name, 'sub_amount': row.contract.replace(',', ''), 'date_posted': row.date} for row in nt_list if row.name != 'vacant']

        query = Tenant.insert_many(ten_insert_many)
        query.execute()
        query = TenantRent.insert_many(rent_insert_many)
        query.execute()
        query = Unit.insert_many(units_insert_many)
        query.execute()
        query = Subsidy.insert_many(subs_insert_many)
        query.execute()
        query = ContractRent.insert_many(krent_insert_many)
        query.execute()
    
        return nt_list, total_tenant_charges, explicit_move_outs

    def after_jan_load(self, filename=None, date=None):
        ''' order matters'''
        ''' get tenants from jan end/feb start'''
        ''' get rent roll from feb end in nt_list from df'''

        first_dt, last_dt = self.make_first_and_last_dates(date_str=date)

        # join with expression not foreignkey
        period_start_tenant_names = [(name.tenant_name, name.unit_name, datetime(name.move_in_date.year, name.move_in_date.month, name.move_in_date.day)) for name in Tenant.select(Tenant.tenant_name, Tenant.move_in_date, Unit.unit_name).
            where(Tenant.move_in_date <= datetime.strptime(date, '%Y-%m')).
            join(Unit, on=Tenant.tenant_name==Unit.tenant).namedtuples()]

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
            self.deactivate_move_outs(date, move_outs=cleaned_mos)

        ''' now we should have updated list of active tenants'''
        cleaned_nt_list = [row for row in self.return_nt_list_with_no_vacants(keyword='vacant', nt_list=nt_list)]

        insert_many_rent = [{'t_name': row.name, 'unit': row.unit, 'rent_amount': row.rent.replace(',',''), 'rent_date': row.date} for row in cleaned_nt_list]  
        # breakpoint()
        '''update last_occupied for occupied: SLOW, Don't like'''
        for row in cleaned_nt_list:
            try:
                unit = Unit.get(Unit.tenant==row.name)
                unit.last_occupied = last_dt
            except Exception as e:
                unit = Unit.get(Unit.unit_name==row.unit)
                unit.last_occupied = '0'
            unit.save()

        subs_insert_many = [{'tenant': row.name, 'sub_amount': row.subsidy.replace(',',''), 'date_posted': row.date} for row in cleaned_nt_list if row.name != 'vacant']
        krent_insert_many = [{'tenant': row.name, 'sub_amount': row.contract.replace(',',''), 'date_posted': row.date} for row in cleaned_nt_list if row.name != 'vacant']

        query = TenantRent.insert_many(insert_many_rent)
        query.execute()
        query = Subsidy.insert_many(subs_insert_many)
        query.execute()
        query = ContractRent.insert_many(krent_insert_many)
        query.execute()
        '''Units: now we should check whether end of period '''
        return cleaned_nt_list, total_tenant_charges, cleaned_mos

    def merge_move_outs(self, explicit_move_outs=None, computed_mos=None):
        explicit_move_outs = [(row[0], row[1]) for row in explicit_move_outs if row[0] != 'vacant']
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
        
        # if filename == '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_first/deposits_04_2022.xlsx':
        #     breakpoint()
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
        '''oddly named descriptions are getting pushed out bc they are not in ntp_cases'''
        # breakpoint()

        insert_iter = []
        for (deposit_id, genus, name, date_posted, amount, date_code, description) in ntp_df.values:
            for description in self.ntp_classify(description):
                insert_iter.append({
                    'payee': description.lower(), 
                    'amount': amount, 
                    'date_posted': datetime.strptime(date_posted, '%m/%d/%Y'),  
                    'date_code': date_code, 
                    'genus': self.genus_renamer(description.lower()), 
                    'deposit_id': deposit_id, 
                    })

        return insert_iter
    
    def ntp_classify(self, description):
        description_split_list = [desc.lower() for desc in description.split(' ')]
        if 'laundry' in description_split_list:
            checked_description = 'laundry'
        else:
            checked_description = ('other' + '_' + f'({description})')
        return [checked_description]

    def genus_renamer(self, description):
        genus_list = description.split(' ')
        if 'laundry' not in genus_list:
            genus = 'other'
        else:
            genus = 'laundry'
        return genus

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
        Row = namedtuple('row', 'name unit rent mo date mi_date subsidy contract')
        explicit_move_outs = []
        nt_list = []
        for index, rec in df.iterrows():
            if rec['Move out'] != fill_item:
                explicit_move_outs.append((rec['Name'].lower(), datetime.strptime(rec['Move out'], '%m/%d/%Y')))
            row = Row(rec['Name'].lower(), rec['Unit'], rec['Actual Rent Charge'], rec['Move out'] , datetime.strptime(date, '%Y-%m'), rec['Move in'], rec['Actual Subsidy Charge'], rec['Lease Rent'])
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
            nt = Tenant.create(tenant_name=name, active='true', move_in_date=move_in_date, unit=unit)
            mi = MoveIn.create(mi_date=move_in_date, name=name)
            unit = Unit.get(unit_name=unit)
            unit.status = 'occupied'
            unit.last_occupied = move_in_date
            unit.tenant = name

            nt.save()
            mi.save()
            unit.save()

    def deactivate_move_outs(self, date, move_outs=None):
        first_dt, last_dt = self.make_first_and_last_dates(date_str=date)
        for name, date in move_outs:
            tenant = Tenant.get(Tenant.tenant_name == name)
            tenant.active = False
            tenant.move_out_date = date
            tenant.save()

            unit = Unit.get(Unit.tenant==name)
            unit.status = 'vacant'
            unit.tenant = 'vacant'
            unit.save()

    def transfer_opcash_to_db(self):
        file_list = [(item.fn, item.period, item.path, item.hap, item.rr, item.depsum, item.deplist, item.corr_sum) for item in Findexer().select().
            where(Findexer.doc_type == 'opcash').
            where(Findexer.status == 'processed').
            namedtuples()]

        for item in file_list:
            oc = OpCash.create(stmt_key=item[0], date=datetime.strptime(item[1], '%Y-%m'), rr=item[4], hap=item[3], dep_sum=item[5], corr_sum=item[7])
            oc.save()

            for lst in json.loads(item[6])[0]:
                ocd = OpCashDetail.create(stmt_key=item[0], date1=datetime.strptime(lst[0], '%m/%d/%Y'), amount=lst[1])
                ocd.save()

    def load_scrape_to_db(self, deposit_list=None, target_date=None):
        for line_item in deposit_list:
            for key, value in line_item.items():
                if key == 'date':
                    if isinstance(target_date, str):
                        period = target_date
                    else:
                        period = target_date.strftime('%Y-%m')
                    scrape_dep = ScrapeDetail(period=period, scrape_date=datetime.now(), scrape_dep_date=0, amount=0)
                    scrape_dep.scrape_dep_date = value 
                if key == 'amount':
                    scrape_dep.amount = value
                if key == 'dep_type':
                    scrape_dep.dep_type = value
                scrape_dep.save()    


class ProcessingLayer(StatusRS):

    def __init__(self):
        self.populate = PopulateTable()

    def set_current_date(self):
        date1 = datetime.now()
        query = StatusRS.create(current_date=date1)
        query.save()  

    def get_most_recent_status(self):
        populate = PopulateTable()
        most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0] # note: - = descending order syntax in peewee
        return most_recent_status

    def get_all_months_ytd(self):
        return Utils.months_in_ytd(Config.current_year)

    def write_manual_entries_from_config(self):
        from manual_entry import ManualEntry  # circular import workaround
        manentry = ManualEntry(db=db)
        manentry.apply_persisted_changes()

    def write_to_statusrs_wrapper(self):
        populate = PopulateTable()
        self.set_current_date()
        most_recent_status = self.get_most_recent_status()
        all_months_ytd = self.get_all_months_ytd()
        report_list = populate.get_processed_by_month(month_list=all_months_ytd)

        return all_months_ytd, report_list, most_recent_status

    def display_most_recent_status(self, mr_status=None, months_ytd=None, ):
        print(f'\n\n*****************************AUTORS: welcome!********************')
        print(f'current date: {mr_status.current_date} | current month: {months_ytd[-1]}\n')
        print('months ytd ' + Config.current_year + ': ' + '  '.join(m for m in months_ytd))
        print('\n')

    def find_complete_pw_months_and_iter_write(self, paperwork_complete_months=None):
        '''passing results of get_existing_sheets would reduce calls'''
        existing_sheets_dict = Utils.get_existing_sheets(service, full_sheet) 
        existing_sheets = [sheet for sheet in [*existing_sheets_dict.keys()] if sheet != 'intake']

        paperwork_complete_months_with_no_rs = list(set(paperwork_complete_months) - set(existing_sheets))
        pw_complete_ms = sorted(paperwork_complete_months_with_no_rs)
        ms.auto_control(source='StatusRS.show()', mode='iter_build', month_list=pw_complete_ms)

    def load_scrape_and_mark_as_processed(self, most_recent_status=None, target_mid_month=None):
        '''this function can be broken up even more'''
        print('load midmonth scrape from bank website')
        populate = PopulateTable()

        target_mm_date = datetime.strptime(list(target_mid_month.items())[0][0], '%Y-%m')

        all_relevant_scrape_txn_list, scrape_deposit_sum = self.load_scrape_wrapper(target_mid_month=target_mid_month)

        populate.load_scrape_to_db(deposit_list=all_relevant_scrape_txn_list, target_date=target_mm_date)

        first_dt = target_mm_date.replace(day = 1)
        most_recent_status.current_date.replace(day = 1)
        last_dt = target_mm_date.replace(day = calendar.monthrange(target_mm_date.year, target_mm_date.month)[1])

        '''if this function asserts ok, then we can write balance letters & rent receipts for current month'''

        all_tp, all_ntp = populate.check_db_tp_and_ntp(grand_total=scrape_deposit_sum, first_dt=first_dt, last_dt=last_dt)    

        if all_tp:
            target_month = list(target_mid_month.items())[0][0]
            mr_status_object = [item for item in StatusObject().select().where(StatusObject.month==target_month)][0]
            mr_status_object.scrape_reconciled = True
            mr_status_object.save()   
            return True
        else:
            return False

    def load_scrape_wrapper(self, target_mid_month=None):
        from file_indexer import FileIndexer  # circular import workaroud
        findex = FileIndexer()
        scrape_txn_list = findex.load_mm_scrape(list1=target_mid_month)
        scrape_deposit_sum = sum([float(item['amount']) for item in scrape_txn_list if item['dep_type'] == 'deposit'])

        return scrape_txn_list, scrape_deposit_sum

    def make_rent_receipts(self, first_incomplete_month=None):
        choice1 = input(f'\nWould you like to make rent receipts for period between {first_incomplete_month} ? Y/n ')
        if choice1 == 'Y':
            write_rr_letters = True
        else:
            write_rr_letters = False
        return write_rr_letters

    def make_balance_letters(self, first_incomplete_month=None):
        choice2 = input(f'\nWould you like to make balance letters for period between {first_incomplete_month} ? Y/n ')
        if choice2 == 'Y':
            write_bal_letters = True
        else:
            write_bal_letters = False
        return write_bal_letters

    def bal_letter_wrapper(self):
        print('generating balance letters')
        balance_letter_list, mr_good_month = self.generate_balance_letter_list_mr_reconciled()

        if balance_letter_list:
            print(f'balance letter list for {mr_good_month}: {balance_letter_list}')

    def rent_receipts_wrapper(self):
        print('generating rent receipts')
        receipts = Letters()
        receipts.rent_receipts()

    def show_balance_letter_list_mr_reconciled(self):
        """triggers off of most recent reconciled month"""
        query = QueryHC()
        mr_good_month = self.get_mr_good_month()
        if mr_good_month:
            first_dt, last_dt = query.make_first_and_last_dates(date_str=mr_good_month)

            balance_letters = [rec for rec in BalanceLetter.select(BalanceLetter, Tenant).
                where(Tenant.active==True).
                where(
                    (BalanceLetter.target_month_end>=first_dt) &
                    (BalanceLetter.target_month_end<=last_dt)).
                join(Tenant).
                namedtuples()]

            return balance_letters
        else:
            return []

    def get_mr_good_month(self):
        query = QueryHC()
        '''get most recent finalized month'''
        try:
            mr_good_month = [rec.month for rec in StatusObject().select(StatusObject.month).
            where(
                ((StatusObject.opcash_processed==1) &
                (StatusObject.tenant_reconciled==1)) |
                ((StatusObject.opcash_processed==0) &
                (StatusObject.scrape_reconciled==1))).
                namedtuples()][-1]
        except IndexError as e:
            print('bypassing error on mr_good_month', e)
            mr_good_month = False
            return mr_good_month

        return mr_good_month

    def is_there_mid_month(self, months_ytd, report_list):
        mid_month_list = []
        final_list = []
        for month, item in zip(months_ytd, report_list):
            look_dict = {fn: (tup[0], tup[1], tup[2]) for fn, tup in item.items() if month == tup[0]}
            ready_to_write_final_dt = self.is_ready_to_write_final(month=month, dict1=look_dict)
            final_list.append(ready_to_write_final_dt)
            if [*ready_to_write_final_dt.values()][0] == False:
                self.is_there_mid_month_print(month, look_dict, ready_to_write_final_dt)            
                mid_month_list.append(ready_to_write_final_dt)
            else:
                self.is_there_mid_month_print(month, look_dict, ready_to_write_final_dt)            
                mid_month_list = []

        final_list = [[*date.keys()][0] for date in final_list if [*date.values()][0] == True]
        return mid_month_list, final_list

    def is_there_mid_month_print(self, month, look_dict, rtwdt):
        if [*rtwdt.values()][0] == True:
            print(f'For period {month} these files have been processed: ')
            print(*list(look_dict.keys()), sep=', ')
            print(f'Ready to Write? {[*rtwdt.values()][0]}')
        else:
            print(f'For period {month} NO files have been processed.')

    def generate_balance_letter_list_mr_reconciled(self):
        query = QueryHC()

        mr_good_month = self.get_mr_good_month()

        if mr_good_month:
            first_dt, last_dt = query.make_first_and_last_dates(date_str=mr_good_month)
            position_list, cumsum = query.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
            
            bal_letter_list = []
            for rec in position_list:
                if float(rec.end_bal) >= float(100):
                    b_letter = BalanceLetter(tenant=rec.name, end_bal=rec.end_bal, target_month_end=last_dt)
                    b_letter.save()
                    tup = (rec.name, rec.end_bal, last_dt)
                    bal_letter_list.append(tup)
            return bal_letter_list, mr_good_month
        else:
            return [], None

    def is_ready_to_write_final(self, month=None, dict1=None):
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

    def write_processed_to_status_rs_db(self, ref_rec=None, report_list=None):
        """Function takes iter of processed files and writes them as json to statusRS db; as far as I can tell this does not do anything important at this time"""
        mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)

        dump_list = []
        for item in report_list:
            for fn, tup in item.items():
                dict1 = {}
                dict1[fn] = tup[0] 
                dump_list.append(dict1)

        mr_status.proc_file = json.dumps(dump_list)
        mr_status.save()

    def show_status_table(self, **kw):

        """ this exists to show the status of the FileIndexer table
        table cols:
            - months year to date
            - deposit detail reports by month
            - rentroll reports by month
            - scrapes available by month
            - tenant payments (from ?)
            - non-tenant payments (from what doc?)
            - total payments (from ??)
            - total deposits related to payments from oc
            - deposit corrections
            - what do I want to show whether reconciled?
        """
        from file_indexer import FileIndexer # circular import work around

        populate  = PopulateTable()
        findex = FileIndexer(path=kw['path'], db=kw['db'])

        months_ytd, unfin_month = findex.test_for_unfinalized_months()    

        status_objects = populate.get_all_status_objects() 
    
        deposits = self.status_table_finder_helper(months_ytd, type1='deposits')

        dep_recon = populate.get_all_findexer_recon_status(type1='deposits')
        
        deposit_rec = [(row.recon) for row in Findexer.select()]
        rents = self.status_table_finder_helper(months_ytd, type1='rent')
        scrapes = self.status_table_finder_helper(months_ytd, type1='scrape') 

        tp_list, ntp_list, total_list, opcash_amt_list, dc_list = self.make_mega_tup_list_for_table(months_ytd)

        dl_tup_list = list(zip(deposits, rents, scrapes, tp_list, ntp_list, total_list, opcash_amt_list, dc_list, dep_recon)) 
        
        header = ['month', 'deps', 'rtroll', 'scrapes', 'ten_pay', 'ntp', 'tot_pay',  'oc_dep', 'dc', 'pay_rec_f?']

        # ['oc_proc', 'ten_rec', 'rs_rec', 'scrape_rec']
        table = [header]
        for item, dep in zip(status_objects, dl_tup_list):
            row_list = []
            row_list.append(item.month)
            row_list.append(str(dep[0][0]))
            row_list.append(str(dep[1][0]))
            row_list.append(str(dep[2][0]))
            row_list.append(str(dep[3][0]))
            row_list.append(str(dep[4][0]))
            row_list.append(str(dep[5][0]))
            row_list.append(str(dep[6][0]))
            row_list.append(str(dep[7][0]))
            row_list.append(str(dep[8][0]))
            # row_list.append(str(item.opcash_processed))
            # row_list.append(str(item.tenant_reconciled))
            # row_list.append(str(item.rs_reconciled))
            # row_list.append(str(item.scrape_reconciled))
            table.append(row_list)
        
        print('\n'.join([''.join(['{:9}'.format(x) for x in r]) for r in table]))

    def status_table_finder_helper(self, months_ytd, type1=None):
        populate  = PopulateTable()
        output = populate.get_all_findexer_by_type(type1=type1)
        output_months = [month for name, month in output]
        return [(True, month) if month in output_months else (False, month) for month in months_ytd] 

    def make_mega_tup_list_for_table(self, months_ytd):
        tp_list = []
        ntp_list = []
        total_list = []
        opcash_amt_list = []
        dc_list = []

        for month in months_ytd:
            first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=month)
        
            ten_payments = sum([float(row[2]) for row in self.populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)])
            tp_tup = (ten_payments, first_dt)
            tp_list.append(tp_tup)

            ntp = sum(self.populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt))
            ntp_tup = (ntp, first_dt)
            ntp_list.append(ntp_tup)

            total = float(ten_payments) + float(ntp)
            total_list.append((total, first_dt))
            
            opcash = self.populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
            if opcash:
                oc_tup = (opcash[0][4], first_dt)
            else:
                oc_tup = (0, first_dt)
            opcash_amt_list.append(oc_tup)

            if opcash:
                dc_tup = (opcash[0][5], first_dt)
                dc_list.append(dc_tup)
            else:
                """branch if not opcash for month to try to get corr_amount from findexer"""
                '''should assert it with a month reading'''
                '''
                '''
                scrapes = self.populate.get_scrape_detail_by_month_by_type(type1='corr', first_dt=first_dt, last_dt=last_dt)

                dc_tup = (sum([float(n) for n in scrapes]), first_dt)
                print('current branch')
                dc_list.append(dc_tup)


        return tp_list, ntp_list, total_list, opcash_amt_list, dc_list  

    def assert_reconcile_payments(self, month_list=None, ref_rec=None):
        """takes list of months in year to date, gets tenant payments by period, non-tenant payments, and opcash information and reconciles the deposits on the opcash statement to the sum of tenant payments and non-tenant payments
        
        then updates existing StatusRS db and, most importantly, writes to StatusObject db whether opcash has been processed and whether tenant has reconciled: currently will reconcile a scrape against a deposit list sheets"""
        populate = PopulateTable()
        for month in month_list:
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=month)
        
            ten_payments = sum([float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)])
            ntp = sum(populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt))
            opcash = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)

            '''probably need to add the concept of "adjustments" in here'''
            sum_from_payments = ten_payments + ntp

            if opcash != []:
                opcash_deposits = float(opcash[0][4])

                if opcash_deposits == sum_from_payments:
                    mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)                
                    s_object = StatusObject.create(key=mr_status.status_id, month=month, opcash_processed=True, tenant_reconciled=True)
                    s_object.save()
            else:
                print('is scrape available?')
                """if scrape is available, does it reconcile to tenant deposits
                
                if scrape deposits + adjustments == tenant payments + adjust > we can mark tenant_reconciled & scrape as processed"""

                scrape_corr = populate.get_scrape_detail_by_month_by_type(type1='corr', first_dt=first_dt, last_dt=last_dt)

                scrape_dep = populate.get_scrape_detail_by_month_by_type(type1='deposit', first_dt=first_dt, last_dt=last_dt)

                scrape_corr = sum([float(item) for item in scrape_corr])

                scrape_dep_detail = populate.get_scrape_detail_by_month_deposit(first_dt=first_dt, last_dt=last_dt)

                if sum_from_payments == sum([float(item) for item in scrape_dep]):
                    print(f'scrape asserted ok for {month} {Config.current_year}')
                    mr_status = StatusRS().get(StatusRS.status_id==ref_rec.status_id)                
                    s_object = StatusObject.create(key=mr_status.status_id, month=month, scrape_reconciled=True, tenant_reconciled=True)
                    s_object.save()
                    
                else:
                    print(f'scrape did not reconcile for {month} {Config.current_year}')
    