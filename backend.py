import calendar
import json
import os
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from functools import reduce
from sqlite3 import IntegrityError

import pandas as pd
import xlrd
from peewee import *
from peewee import (JOIN, AutoField, BooleanField, CharField, DateField,
                    DecimalField)
from peewee import DoesNotExist as DNE
from peewee import ForeignKeyField, IntegerField
from peewee import IntegrityError as PIE
from peewee import Model, SqliteDatabase, fn
from recordtype import \
    recordtype

# i edit the source code here,
# so requirements won't work if this is ever published,
# after 3.10, collection.abc change

from config import Config
from letters import Letters
from reconciler import Reconciler
from utils import Utils

basedir = os.path.abspath(os.path.dirname(__file__))
db = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db


class Tenant(BaseModel):
    tenant_name = CharField(primary_key=True)  # removed unique = True
    active = CharField(default=True)
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
        vacant_units = [name.unit_name for name in Unit.select().order_by(
            Unit.unit_name).where(Unit.status == 'vacant').namedtuples()]
        return vacant_units

    @staticmethod
    def get_all_units():
        return [name.unit_name for name in Unit.select()]

    def get_all_occupied_units_at_date(**kwargs):
        all_units = [(name.tenant,
                      name.unit_name,
                      name.last_occupied) for name in Unit.select().
                     where(Unit.last_occupied == kwargs['last_dt']).
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
                dam = Damages(
                    tenant=name, dam_amount=packet[0],
                    dam_date=packet[1], dam_type=packet[2])
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
    id = AutoField()
    key = ForeignKeyField(StatusRS, backref='zzzzzz')
    month = CharField(default='0', unique=True)
    opcash_processed = BooleanField(default=False)
    tenant_reconciled = BooleanField(default=False)
    scrape_reconciled = BooleanField(default=False)
    rs_reconciled = BooleanField(default=False)
    excel_reconciled = BooleanField(default=False)
    bal_letters = BooleanField(default=False)
    rent_recipts = BooleanField(default=False)


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
    txn_date = DateField()
    original_item = CharField(default='0')


class WorkOrder(BaseModel):
    name = ForeignKeyField(Tenant, backref='work_orders', null=True)
    init_date = DateField(null=True)
    location = CharField(null=True)
    work_req = CharField(null=True)
    notes = CharField(null=True)
    status = CharField(null=True)
    date_completed = DateField(null=True)
    assigned_to = CharField(default='ron/bob')


class QueryHC(Reconciler):

    def return_tables_list(self):
        return [Mentry,
                IncomeMonth,
                LP_EndBal,
                ContractRent,
                Subsidy,
                BalanceLetter,
                StatusRS,
                StatusObject,
                OpCash,
                OpCashDetail,
                Damages,
                Tenant,
                Unit,
                Payment,
                NTPayment,
                TenantRent,
                Findexer,
                ScrapeDetail,
                MoveIn,
                WorkOrder]

    def get_start_tenants(self, date):
        return [(name.tenant_name, name.unit_name,
                datetime(name.move_in_date.year,
                 name.move_in_date.month,
                 name.move_in_date.day)) for name in
                Tenant.select(Tenant.tenant_name,
                              Tenant.move_in_date,
                              Unit.unit_name).where(
            Tenant.move_in_date <=
            datetime.strptime(date, '%Y-%m')).
            join(Unit, on=Tenant.tenant_name == Unit.tenant).
            namedtuples()]

    def make_first_and_last_dates(self, date_str=None):
        dt_obj = datetime.strptime(date_str, '%Y-%m')
        dt_obj_first = dt_obj.replace(day=1)
        dt_obj_last = dt_obj.replace(
            day=calendar.monthrange(dt_obj.year, dt_obj.month)[1])

        return dt_obj_first, dt_obj_last

    def get_all_status_objects(self):
        return [row for row in StatusObject.select().namedtuples()]

    def get_all_findexer_by_type(self, type1=None):
        rows = [(row.fn, row.period) for row in Findexer.select().where(
            Findexer.doc_type == type1).namedtuples()]
        return rows

    def get_all_findexer_recon_status(self, type1=None):
        rows = [(row.recon, row.period) for row in Findexer.select().where(
            Findexer.doc_type == type1).namedtuples()]
        return rows

    def get_rent_roll_by_month_at_first_of_month(self, 
                                                 first_dt=None, 
                                                 last_dt=None):
        '''lots of work in this func'''
        tenants_mi_on_or_before_first = [(rec.tenant_name,
                                         rec.unit) for rec in Tenant().
                                         select(Tenant, Unit).
                                         join(Unit, JOIN.LEFT_OUTER,
                                         on=(
                                             Tenant.tenant_name == Unit.tenant
                                         )).
                                         where(
            (Tenant.move_in_date <= first_dt) &
            ((Tenant.move_out_date == '0') |
             (Tenant.move_out_date >= first_dt))).
            namedtuples()]

        occupied_units = [
            unit for (name, unit) in tenants_mi_on_or_before_first]

        all_units = Unit.get_all_units()

        for vacant_unit in set(all_units) - set(occupied_units):
            tup = ('vacant',  vacant_unit)
            tenants_mi_on_or_before_first.append(tup)

        vacants = [
            item 
            for item in tenants_mi_on_or_before_first if item[0] == 'vacant']
        tenants = [item[0]
                   for item in tenants_mi_on_or_before_first 
                   if item[0] != 'vacant']

        return tenants_mi_on_or_before_first, vacants, tenants

    def get_beg_bal_by_tenant(self):
        return [(row.tenant_name, float(row.beg_bal_amount)) for row in Tenant.
                select(Tenant.tenant_name, Tenant.beg_bal_amount).
                namedtuples()]

    def get_mentries_by_month(self, first_dt=None, last_dt=None, type1=None):
        mentries = [(row.original_item) for row in Mentry.select().
                    where(Mentry.txn_date >= first_dt).
                    where(Mentry.txn_date <= last_dt).
                    where(Mentry.ch_type == type1).
                    namedtuples()]

        if type1 == 'delete' and mentries != []:
            return [float(item.replace("'", "").
                    split(',')[3].
                    split('=')[1]) for item in mentries][0]

        return mentries

    def get_opcash_by_period(self, first_dt=None, last_dt=None):
        return [(row.stmt_key, row.date, row.rr, row.hap, row.dep_sum, row.corr_sum) for row in OpCash.select(OpCash.stmt_key, OpCash.date, OpCash.rr, OpCash.hap, OpCash.dep_sum, OpCash.corr_sum).
                where(OpCash.date >= first_dt).
                where(OpCash.date <= last_dt).namedtuples()]

    def get_opcashdetail_by_stmt(self, stmt_key=None):
        return [row for row in OpCashDetail.select().join(OpCash).where(OpCashDetail.stmt_key == stmt_key).namedtuples()]

    def get_move_ins_by_period(self, first_dt=None, last_dt=None):
        recs = [(row.mi_date, row.name) for row in MoveIn.select().where(MoveIn.mi_date >= first_dt).
                where(MoveIn.mi_date <= last_dt).namedtuples()]
        return recs

    def get_scrape_detail_by_month_deposit(self, first_dt=None, last_dt=None):
        recs = [row for row in ScrapeDetail.select().where(ScrapeDetail.scrape_dep_date >= first_dt).
                where(ScrapeDetail.scrape_dep_date <= last_dt).
                where(ScrapeDetail.dep_type == 'deposit').namedtuples()]
        return recs

    def get_scrape_detail_by_month_by_type(self, type1, first_dt=None, last_dt=None):
        recs = [row.amount for row in ScrapeDetail.select().where(ScrapeDetail.scrape_dep_date >= first_dt).
                where(ScrapeDetail.scrape_dep_date <= last_dt).
                where(ScrapeDetail.dep_type == type1).namedtuples()]
        return recs

    def get_processed_by_month(self, month_list=None):
        """returns list of dicts from Findexer of files that have been attribute 'processed' and packaged with month, path, and report_type(ie 'rent', 'deposits')"""
        report_list = []
        for month in month_list:
            reports_by_month = {rec.fn: (month, rec.path, rec.doc_type) for rec in Findexer().select(
            ).where(Findexer.period == month).where(Findexer.status == 'processed').namedtuples()}
            report_list.append(reports_by_month)
        return report_list

    def get_balance_letters_by_month(self, first_dt=None, last_dt=None):
        return [item.tenant_id for item in BalanceLetter().select().where(BalanceLetter.target_month_end == last_dt).namedtuples()]

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
        ten_pay_tup = [(rec.tenant_name, rec.total_payments) for rec in Tenant.select(
            Tenant.tenant_name,
            fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
            where(Payment.date_posted >= first_dt).
            where(Payment.date_posted <= last_dt).
            where(Tenant.tenant_name == name).
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
        Position = recordtype(
            'Position', 'name alltime_beg_bal lp_endbal payment_total charges_total damages_total end_bal start_date end_date unit subsidy contract_rent', default=0)

        rr, vacants, tenants = self.get_rent_roll_by_month_at_first_of_month(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = [Position(
            name=item[0], start_date=first_dt, end_date=last_dt, unit=item[1]) for item in rr]

        alltime_beg_bal = self.get_beg_bal_by_tenant()  # ALLTIME STARING BEG BALANCE
        position_list1 = self.record_type_loader(
            position_list1, 'alltime_beg_bal', alltime_beg_bal, 1)

        if str(first_dt.year) + '-' + str(first_dt.month) == '2022-1':
            positions_list1 = self.record_type_loader(
                position_list1, 'lp_endbal', alltime_beg_bal, 1)
        else:
            last_endbal_by_tenant = self.tenant_last_endbal_this_period(
                first_dt=first_dt, last_dt=last_dt)
            position_list1 = self.record_type_loader(
                position_list1, 'lp_endbal', last_endbal_by_tenant, 1)

        payments_by_tenant = self.tenant_payments_this_period(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'payment_total', payments_by_tenant, 1)

        charges_by_tenant = self.tenant_charges_this_period(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'charges_total', charges_by_tenant, 1)

        damages_by_tenant = self.tenant_damages_this_period(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'damages_total', damages_by_tenant, 1)

        subsidy_by_tenant = self.subsidy_this_period(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'subsidy', subsidy_by_tenant, 1)

        contract_by_tenant = self.contract_this_period(
            first_dt=first_dt, last_dt=last_dt)

        position_list1 = self.record_type_loader(
            position_list1, 'contract_rent', contract_by_tenant, 1)

        '''this is the work right here: do I want to put output in a database?'''
        for row in position_list1:
            row.end_bal = row.lp_endbal + row.charges_total + \
                row.damages_total - row.payment_total
            if row.name != 'vacant':
                lp_end_bal = LP_EndBal.create(
                    tenant=row.name, sub_amount=row.end_bal, date_posted=last_dt)
                lp_end_bal.save()

        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal

        return position_list1, cumsum

    def net_position_by_tenant_by_month(self, first_dt=None, last_dt=None, after_first_month=None):
        '''heaviest business logic here'''
        '''returns relatively hefty object with everything you'd need to write the report/make the sheets'''
        '''model we are using now is do alltime payments, charges, and alltime beg_bal'''

        Position = recordtype(
            'Position', 'name alltime_beg_bal payment_total charges_total damages_total end_bal start_date end_date unit subsidy contract_rent', default=0)

        rr, vacants, tenants = self.get_rent_roll_by_month_at_first_of_month(
            first_dt=first_dt, last_dt=last_dt)
        position_list1 = [Position(
            name=item[0], start_date=first_dt, end_date=last_dt, unit=item[1]) for item in rr]

        alltime_beg_bal = self.get_beg_bal_by_tenant()  # ALLTIME STARING BEG BALANCES

        position_list1 = self.record_type_loader(
            position_list1, 'alltime_beg_bal', alltime_beg_bal, 1)

        all_tenant_payments_by_tenant = self.sum_lifetime_tenant_payments(
            dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(
            position_list1, 'payment_total', all_tenant_payments_by_tenant, 2)

        all_tenant_charges_by_tenant = self.sum_lifetime_tenant_charges(
            dt_obj_last=last_dt)

        position_list1 = self.record_type_loader(
            position_list1, 'charges_total', all_tenant_charges_by_tenant, 1)

        all_tenant_damages_by_tenant = self.sum_lifetime_tenant_damages(
            dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'damages_total', all_tenant_damages_by_tenant, 1)

        all_subsidy_by_tenant = self.sum_lifetime_subsidy(dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'subsidy', all_subsidy_by_tenant, 1)

        all_krent_by_tenant = self.sum_lifetime_contract_rent(
            dt_obj_last=last_dt)
        position_list1 = self.record_type_loader(
            position_list1, 'contract_rent', all_krent_by_tenant, 1)

        '''this is the work right here: do I want to put output in a database?'''
        for row in position_list1:
            row.end_bal = row.alltime_beg_bal + row.charges_total + \
                row.damages_total - row.payment_total

        cumsum = 0
        for row in position_list1:
            cumsum += row.end_bal

        return position_list1, cumsum

    def all_available_by_fk_by_period(self, target=None, first_dt=None, last_dt=None):
        record_cut_off_date = datetime(2022, 1, 1, 0, 0)

        damages_predicate = ((Damages.dam_date.between(
            first_dt, last_dt) & (Damages.tenant_id == target)))

        payment_predicate = ((Payment.date_posted.between(
            first_dt, last_dt) & (Payment.tenant_id == target)))

        contract_rent_predicate = ((ContractRent.date_posted.between(
            first_dt, last_dt) & (ContractRent.tenant_id == target)))

        subsidy_predicate = ((Subsidy.date_posted.between(
            first_dt, last_dt) & (Subsidy.tenant_id == target)))

        tenant_rent_predicate = ((TenantRent.rent_date.between(
            first_dt, last_dt) & (TenantRent.t_name_id == target)))

        select_expression1 = Tenant.select(Tenant.tenant_name,
                                           Tenant.unit,
                                           Tenant.beg_bal_amount,
                                           fn.SUM(Payment.amount).over(partition_by=[
                                               Tenant.tenant_name]).alias('t_payments'),
                                           fn.SUM(ContractRent.sub_amount).over(partition_by=[
                                               Tenant.tenant_name]).alias('t_contract_rent'),
                                           fn.SUM(Subsidy.sub_amount).over(partition_by=[
                                               Tenant.tenant_name]).alias('t_subsidy'),
                                           fn.SUM(TenantRent.rent_amount).over(partition_by=[
                                               Tenant.tenant_name]).alias('t_rent_charges'),
                                           fn.SUM(Damages.dam_amount).over(partition_by=[Tenant.tenant_name]).alias('t_damages'),)

        if record_cut_off_date == first_dt:
            print('getting beginning balance from Tenant table')
            record = [row for row in select_expression1.
                      join(Payment, JOIN.LEFT_OUTER, on=payment_predicate).
                      switch(Tenant).
                      join(ContractRent, JOIN.LEFT_OUTER, on=contract_rent_predicate).
                      switch(Tenant).
                      join(Subsidy, JOIN.LEFT_OUTER, on=subsidy_predicate).
                      switch(Tenant).
                      join(TenantRent, JOIN.LEFT_OUTER, on=tenant_rent_predicate).
                      switch(Tenant).
                      join(Damages, JOIN.LEFT_OUTER, on=damages_predicate).
                      where(
                          (Tenant.tenant_name == target)
                          # &
                          # (Payment.date_posted.between(first_dt, last_dt))
                          # &
                          # (ContractRent.date_posted.between(first_dt,last_dt))
                          # &
                          # (Subsidy.date_posted.between(first_dt,last_dt))
                          # &
                          # (TenantRent.rent_date.between(first_dt,last_dt))
                      ).
                      namedtuples()]
            print(record)
            # breakpoint()
        else:
            print('no beginning balance available yet')
            record = [row for row in Tenant.
                      select(

                          Tenant.tenant_name,
                          Tenant.unit,
                          # need to get beginning balance from elsewhere

                          fn.SUM(Payment.amount).over(partition_by=[
                              Tenant.tenant_name]).alias('t_payments'),

                          fn.SUM(ContractRent.sub_amount).over(partition_by=[
                              Tenant.tenant_name]).alias('t_contract_rent'),

                          fn.SUM(Subsidy.sub_amount).over(partition_by=[
                              Tenant.tenant_name]).alias('t_subsidy'),

                          fn.SUM(TenantRent.rent_amount).over(partition_by=[
                              Tenant.tenant_name]).alias('t_rent_charges'),

                          fn.SUM(Damages.dam_amount).over(partition_by=[
                              Tenant.tenant_name]).alias('t_damages'),

                      ).

                      join(Payment, JOIN.LEFT_OUTER).
                      switch(Tenant).
                      join(ContractRent, JOIN.LEFT_OUTER).
                      switch(Tenant).
                      join(Subsidy, JOIN.LEFT_OUTER).
                      switch(Tenant).
                      join(TenantRent, JOIN.LEFT_OUTER).
                      switch(Tenant).
                      # join_from(Damages, Tenant).
                      join(Damages, JOIN.LEFT_OUTER, on=damages_predicate).

                      where(
                          (Tenant.tenant_name == target)
                          &
                          (Payment.date_posted.between(first_dt, last_dt))
                          &
                          (ContractRent.date_posted.between(first_dt, last_dt))
                          &
                          (Subsidy.date_posted.between(first_dt, last_dt))
                          &
                          (TenantRent.rent_date.between(first_dt, last_dt))
                      ).
                      namedtuples()]
            print(record)

        # TODO
        """
        This is building the final stage of the db, this will write and be a final record of a month, that would be closed, we can drop some of the other tables at this point; we can also, use earlier stages as A STAGING AREA
        """
        # period_startbal + tenant_rent - tenantpayments + damages + adjustments = period_end_bal

        # if first period get_beg_bal from Tenant table
        # else

        breakpoint()


class UrQuery(QueryHC):

    def __init__(self, **kwargs):
        print('urquery')

    def ur_query(self,
                 model_str=None,
                 query_dict=None,
                 query_tup=None,
                 operators_list=None,
                 **kwargs):
        import operator
        model = getattr(sys.modules[__name__], model_str)

        if operators_list is None:
            operators = [operator.and_]
        else:
            operators = []
            for item in operators_list:
                if item == '>=':
                    operators.append(operator.ge)
                elif item == '<=':
                    operators.append(operator.le)
                elif item == '&':
                    operators.append(operator.and_)
                elif item == '==':
                    operators.append(operator.eq)

        clauses = []
        if query_dict:
            for key, value in query_dict.items():
                field = model._meta.fields[key]
                clauses.append(field == value)
            expr = reduce(operator.and_, clauses)
            return model.select().where(expr)
        elif query_tup:
            for item, op in zip(query_tup, operators):
                field = model._meta.fields[item[0]]
                clauses.append(op(field, item[1]))
            expr = reduce(operator.and_, clauses)
            return model.select().where(expr)
        else:
            return model.select()     # get_all


class PopulateTable(QueryHC):

    def _total_tenant_charges(self):
        return float(
            ((self.nt_list_w_vacants.pop(-1)).rent).replace(',', ''))

    def _write_vals(self, data, cls=None):
        model = getattr(sys.modules[__name__], cls)
        query = model.insert_many(data)
        try:
            query.execute()
        except TypeError as e:
            print(e)
            print('issue is with query write execution in InitLoad')

    def merge_move_outs(self, explicit_move_outs=None, computed_mos=None, date=None):

        if computed_mos != []:  # remove unit column from computed_mos
            computed_mos1 = [(item[0], item[2]) for item in computed_mos]
        explicit_move_outs = [(row[0], row[1])
                              for row in explicit_move_outs if row[0] != 'vacant']
        cleaned_mos = []
        if explicit_move_outs != []:
            cleaned_mos = explicit_move_outs + computed_mos

        return cleaned_mos

    def payment_load_full(self, filename):
        df = self.read_excel_payments(path=filename)
        df = self.remove_nan_lines(df=df)
        grand_total = self.grand_total(df=df)

        tenant_payment_df, ntp_df = self.return_and_remove_ntp(
            df=df, col='unit', remove_str=0, drop_col='description')

        # can split up ntp further here
        ntp_sum = sum(ntp_df['amount'].astype(float).tolist())

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
        description_split_list = [desc.lower()
                                  for desc in description.split(' ')]
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

        columns = ['deposit_id', 'unit', 'name', 'date_posted',
                   'amount', 'date_code', 'description']

        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        description = df['Description'].str.lower().tolist()
        dt_code = [datetime.strptime(item, '%m/%d/%Y')
                   for item in date if type(item) == str]
        dt_code = [str(datetime.strftime(item, '%m')) for item in dt_code]

        zipped = zip(bde, unit, name, date, pay, dt_code, description)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def nt_from_df(self, df, date, fill_item):
        Row = namedtuple(
            'row', 'name unit rent mo date mi_date subsidy contract')
        explicit_move_outs = []
        nt_list = []
        for index, rec in df.iterrows():
            try:
                if rec['Move out'] != fill_item:
                    explicit_move_outs.append(
                        (rec['Name'].lower(), datetime.strptime(rec['Move out'], '%m/%d/%Y')))
            except TypeError as e:
                print(e)
                print(
                    'adjusting move-out date for manual adjustment to move out on rent roll')
                explicit_move_outs.append(
                    (rec['Name'].lower(), rec['Move out'].to_pydatetime()))

            row = Row(rec['Name'].lower(), rec['Unit'], rec['Actual Rent Charge'], rec['Move out'], datetime.strptime(
                date, '%Y-%m'), rec['Move in'], rec['Actual Subsidy Charge'], rec['Lease Rent'])

            nt_list.append(row)

        return nt_list, explicit_move_outs

    def return_nt_list_with_no_vacants(self, keyword=None, nt_list=None):
        return [row for row in nt_list if row.name != keyword]

    def grand_total(self, df):
        amounts = df['amount'].tolist()
        amounts = [str(amount).replace(',', '') for amount in amounts]
        amounts = sum([float(amount) for amount in amounts])

        return amounts

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

    def compare_rentroll_chng(self, start_set=None, end_set=None):
        '''compares list of tenants at start of month to those at end'''
        '''explicit move-outs from excel have been removed from end of month rent_roll_dict
        and should initiated a discrepancy in the following code by making end list diverge from start list'''
        '''these tenants are explicitly marked as active='False' in the tenant table in func() deactivate_move_outs'''

        move_ins = list(end_set - start_set)  # catches move in
        move_outs = list(start_set - end_set)  # catches move out

        return move_ins, move_outs

    def insert_move_ins(self, move_ins=None, date=None, filename=None):
        for name, unit, move_in_date in move_ins:
            print('move-ins:', name, unit, move_in_date)
            try:
                with db.atomic():
                    nt = Tenant.create(
                        tenant_name=name, active='true',
                        move_in_date=move_in_date,
                        unit=unit)
            except PIE as e:
                print(e, 'new Tenant already entered into table', name)
                return Tenant.get(Tenant.tenant_name == name)

            try:
                with db.atomic():
                    mi = MoveIn.create(mi_date=move_in_date, name=name)
            except PIE as e:
                print(e, 'Move IN already entered into MI table', name)
                return MoveIn.get(MoveIn.name == name)

            try:
                with db.atomic():
                    unt = Unit.get(unit_name=unit)
            except PIE as e:
                print(e, 'UNIT already modified in Unit table', name)
                return Unit.get(Unit.unit_name == unit)

            unt.status = 'occupied'
            unt.last_occupied = move_in_date
            unt.tenant = name
            print(
                'Move-In: updating Unit, MoveIn, and Tenant tables for',
                f'{unt.tenant} for {unt.last_occupied}')

            try:
                nt.save()
            except PIE as e:
                print(e, 'issue with new tenant creation', unt.tenant)

            try:
                mi.save()
            except PIE as e:
                print(e, 'issue with move in creation', unt.tenant)

            try:
                unt.save()
            except PIE as e:
                print(e, 'issue with unit creation', unt.tenant)

    def deactivate_move_outs(self, date, move_outs=None):
        first_dt, last_dt = self.make_first_and_last_dates(date_str=date)
        if len(move_outs) <= 1:
            for name, date in move_outs:
                print('deactivating:',  name, date)
                tenant = Tenant.get(Tenant.tenant_name == name)
                tenant.active = False
                tenant.move_out_date = date
                tenant.save()

                unit = Unit.get(Unit.tenant == name)
                unit.status = 'vacant'
                unit.tenant = 'vacant'
                unit.save()
        else:
            for item in move_outs:
                try:
                    name = item[0]
                    date = item[1]
                    print('move outs:',  name, date)
                    tenant = Tenant.get(Tenant.tenant_name == name)
                    tenant.active = False
                    tenant.move_out_date = date
                    tenant.save()
                except (ValueError, IndexError, DNE) as e:
                    breakpoint()

                try:
                    unit = Unit.get(Unit.tenant == name)
                    unit.status = 'vacant'
                    unit.tenant = 'vacant'
                    unit.save()
                except (ValueError, IndexError, DNE) as e:
                    print(f'error deactivating unit for {name}')

    def transfer_opcash_from_findex_to_opcash_and_detail(self):
        """this function is repsonsible for moving information unpacked into findexer table into OpCash and OpCashDetail tables"""

        """
        SO HERE!!

        I AM NOT CHECKING SCRAPES AT ALL SO THOSE ARE NOT PULLED INTO OPCASH AND OPCASH_DETAIL

        COULD MERGE WITH SCRAPEDETAIL
        """
        file_list = [(item.fn, item.period, item.path, item.hap, item.rr, item.depsum, item.deplist, item.corr_sum) for item in Findexer().select().
                     where(Findexer.doc_type == 'opcash').
                     where(Findexer.status == 'processed').
                     namedtuples()]

        file_list = [(item.fn, item.period, item.path, item.hap, item.rr, item.depsum, item.deplist, item.corr_sum) for item in Findexer().select().
                     where(Findexer.doc_type == 'opcash').
                     where(Findexer.status == 'processed').
                     namedtuples()]

        for item in file_list:
            try:
                with db.atomic():
                    oc = OpCash.create(stmt_key=item[0], date=datetime.strptime(
                        item[1], '%Y-%m'), rr=item[4], hap=item[3], dep_sum=item[5], corr_sum=item[7])
                    oc.save()
            except IntegrityError as e:
                print('already created this record')
                print(e)

            for lst in json.loads(item[6]):
                amount = list(lst.values())[0]
                ocd = OpCashDetail.create(stmt_key=item[0], date1=datetime.strptime(
                    list(lst.keys())[0], '%Y-%m-%d'), amount=amount)
                ocd.save()

    def load_scrape_to_db(self, deposit_list=None, target_date=None):
        for line_item in deposit_list:
            for key, value in line_item.items():
                if key == 'date':
                    if isinstance(target_date, str):
                        period = target_date
                    else:
                        period = target_date.strftime('%Y-%m')
                    scrape_dep = ScrapeDetail(
                        period=period, scrape_date=datetime.now(), scrape_dep_date=0, amount=0)
                    scrape_dep.scrape_dep_date = value
                if key == 'amount':
                    scrape_dep.amount = value
                if key == 'dep_type':
                    scrape_dep.dep_type = value
                scrape_dep.save()


class AfterInitLoad(PopulateTable):

    def __init__(self, rentrolls, deposits):
        print('\n afterinitload')
        self.fill_item = '0'
        self.rentrolls = rentrolls
        self.deposits = deposits
        self._loop_over_rentrolls()
        self._loop_over_deposits()

    def _loop_over_deposits(self):
        for date, path in self.deposits:
            grand_total, ntp, tenant_payment_df = self.payment_load_full(
                filename=path)

    def _loop_over_rentrolls(self):
        for date, filename in self.rentrolls:
            (self.first_dt,
             self.last_dt) = self.make_first_and_last_dates(date_str=date)
            self.start_tenants = self.get_start_tenants(date)

            if filename[-1] == 'x':
                self.df = pd.read_excel(filename, header=16)
            else:
                filename = xlrd.open_workbook(filename,
                                              logfile=open(os.devnull, 'w'))
                self.df = pd.read_excel(filename, header=16)

            self.df = self.df.fillna(self.fill_item)

            (self.nt_list_w_vacants,
             self.explicit_move_outs) = self.nt_from_df(
                df=self.df, date=date, fill_item=self.fill_item)

            self.total_tenant_charges = self._total_tenant_charges()
            self.end_tenants = [(row.name, row.unit,
                                datetime.strptime(row.mi_date,
                                                  '%m/%d/%Y'))
                                for row in self.return_nt_list_with_no_vacants(
                                    keyword='vacant',
                                    nt_list=self.nt_list_w_vacants)]

            self.computed_mis, self.computed_mos = self.compare_rentroll_chng(
                start_set=set(self.start_tenants),
                end_set=set(self.end_tenants))

            self.cleaned_mos = self.merge_move_outs(
                explicit_move_outs=self.explicit_move_outs,
                computed_mos=self.computed_mos,
                date=date)

            self.insert_move_ins(move_ins=self.computed_mis,
                                 date=date, filename=filename)

            self._deactivate_move_outs(date)

            ''' now we have an updated list of active tenants
                and may again fetched this updated record
            '''
            self.cleaned_nt_list = [
                row for row in self.return_nt_list_with_no_vacants(
                    keyword='vacant', nt_list=self.nt_list_w_vacants)
            ]

            self.rents = [{'t_name': row.name,
                           'unit': row.unit,
                           'rent_amount': row.rent.replace(
                               ',', ''),
                           'rent_date': row.date}
                          for row in self.cleaned_nt_list]

            self._update_unit_table()

            self.subsidies = [{'tenant': row.name,
                               'sub_amount': row.subsidy.replace(
                                   ',', ''),
                               'date_posted': row.date}
                              for row in self.cleaned_nt_list if row.name
                              != 'vacant']
            self.contract_rents = [{'tenant': row.name,
                                   'sub_amount': row.contract.replace(
                                    ',', ''),
                                    'date_posted': row.date}
                                   for row in self.cleaned_nt_list if row.name
                                   != 'vacant']

            self._write_vals(self.rents, cls='TenantRent')
            self._write_vals(self.subsidies, cls='Subsidy')
            self._write_vals(self.contract_rents, cls='ContractRent')

    def _deactivate_move_outs(self, date):
        if self.cleaned_mos != []:
            self.deactivate_move_outs(date, move_outs=self.cleaned_mos)

    def _update_unit_table(self):
        '''update last_occupied for occupied: SLOW, Don't like'''
        for row in self.cleaned_nt_list:
            try:
                unit = Unit.get(Unit.tenant == row.name)
                unit.last_occupied = self.last_dt
            except Exception as e:
                print(e, 'using Exception to do too much coding')
                unit = Unit.get(Unit.unit_name == row.unit)
                unit.last_occupied = '0'
            unit.save()

    def return_rentroll_data(self):
        return (self.nt_list_w_vacants,
                self.total_tenant_charges,
                self.cleaned_mos,
                self.computed_mis)


class InitLoad(PopulateTable):

    def __init__(self, path=None, **kwargs):
        """loads initial tenants and tenant accounts from spreadsheet"""

        print('initload')
        self.path = path
        self.custom_load_file = self.path.joinpath(kwargs['custom_load_file'])
        self.records = [(item.fn, item.period, item.path)
                        for item in Findexer().
                        select().
                        where(Findexer.doc_type == 'rent').
                        where(Findexer.status == 'processed').
                        where(Findexer.period == '2022-01').
                        namedtuples()]
        self.first_dt, self.last_dt = self.make_first_and_last_dates(
            date_str=self.records[0][1])
        self.wb = xlrd.open_workbook(self.records[0][2],
                                     logfile=open(os.devnull, 'w'))
        self.df = pd.read_excel(self.wb, header=16)
        self.df = self.df.fillna('0')
        self.nt_list_w_vacants, self.explicit_move_outs = self.nt_from_df(
            df=self.df, date=self.records[0][1], fill_item='0')
        self.total_tenant_charges = self._total_tenant_charges()
        self.nt_list = self.return_nt_list_with_no_vacants(
            keyword='vacant', nt_list=self.nt_list_w_vacants)
        self.init_tenants = [{'tenant_name': row.name,
                              'move_in_date': datetime.strptime(
                                  row.mi_date, '%m/%d/%Y'),
                              'unit': row.unit} for row in self.nt_list]
        self.units = self._update_units()

        self.rents = [{'t_name': row.name,
                       'unit': row.unit,
                       'rent_amount': row.rent.replace(',', ''),
                       'rent_date': row.date}
                      for row in self.nt_list if row.name != 'vacant']

        self.subsidies = [{'tenant': row.name,
                           'sub_amount': row.subsidy.replace(
                               ',', ''),
                           'date_posted': row.date}
                          for row in self.nt_list if row.name != 'vacant']

        self.contract_rents = [{'tenant': row.name,
                                'sub_amount': row.contract.replace(
                                    ',', ''),
                                'date_posted': row.date}
                               for row in self.nt_list if row.name != 'vacant']

        self._write_vals(self.init_tenants, cls='Tenant')
        self._write_vals(self.units, cls='Unit')
        self._write_vals(self.rents, cls='TenantRent')
        self._write_vals(self.subsidies, cls='Subsidy')
        self._write_vals(self.contract_rents, cls='ContractRent')

        self._balance_load_from_excel()

    def _update_units(self):
        # TODO:WHAT DDOES THIS FUNCTION REALLY DO?
        units = []
        for row in self.nt_list_w_vacants:
            if row.name == 'vacant':
                dict1 = {'unit_name': row.unit, 'status': 'vacant'}
            else:
                dict1 = {'unit_name': row.unit,
                         'tenant': row.name, 'last_occupied': self.last_dt}
            units.append(dict1)
        return units

    def _balance_load_from_excel(self):
        # load tenant balances at 01012022
        df = pd.read_excel(self.custom_load_file)
        t_name = df['name'].tolist()
        beg_bal = df['balance'].tolist()
        rent_roll_dict = dict(zip(t_name, beg_bal))
        rent_roll_dict = {
            k.lower(): v for k, v in rent_roll_dict.items() if k != 'vacant'}
        rent_roll_dict = {k: v for k,
                          v in rent_roll_dict.items() if k != 'vacant'}

        ten_list = [tenant for tenant in Tenant.select()]
        for tenant in ten_list:
            for name, bal in rent_roll_dict.items():
                if tenant.tenant_name == name:
                    tenant.beg_bal_amount = bal
                    tenant.save()

    def return_init_results(self):
        return (self.nt_list,
                self.total_tenant_charges,
                self.explicit_move_outs,
                self.init_tenants,
                self.units,
                self.rents,
                self.subsidies,
                self.contract_rents)


class ProcessingLayer(StatusRS):

    def __init__(self, service=None, full_sheet=None, ms=None):
        self.populate = PopulateTable()
        self.service = service
        self.full_sheet = full_sheet
        self.month_sheet_object = ms

    def set_current_date(self):
        date1 = datetime.now()
        query = StatusRS.create(current_date=date1)
        query.save()

    def get_most_recent_status(self):
        populate = PopulateTable()
        most_recent_status = [item for item in StatusRS().select(
        ).order_by(-StatusRS.status_id).namedtuples()][0]  # note: - = descending order syntax in peewee
        return most_recent_status

    def write_manual_entries_from_config(self):
        from manual_entry import ManualEntry  # circular import workaround
        manentry = ManualEntry(db=db)
        manentry.apply_persisted_changes()

    def write_to_statusrs_wrapper(self):
        populate = PopulateTable()
        self.set_current_date()
        most_recent_status = self.get_most_recent_status()
        all_months_ytd = Utils.months_in_ytd(Config.current_year)
        report_list = populate.get_processed_by_month(
            month_list=all_months_ytd)

        return all_months_ytd, report_list, most_recent_status

    def display_most_recent_status(self, mr_status=None, months_ytd=None):
        print(f'\n\n***************************** welcome!********************')
        print(
            f'current date: {mr_status.current_date} | current month: {months_ytd[-1]}\n')
        print('months ytd ' + Config.current_year +
              ': ' + '  '.join(m for m in months_ytd))

    def find_complete_pw_months_and_iter_write(self, writeable_months=None, *args, **kwargs):
        '''passing results of get_existing_sheets would reduce calls'''

        """if sheet already exists for a month, that month will not be included in list to write)"""
        existing_sheets_dict = Utils.get_existing_sheets(
            self.service, self.full_sheet)
        existing_sheets = [sheet for sheet in [
            *existing_sheets_dict.keys()] if sheet != 'intake']

        pw_complete_ms = sorted(
            list(set(writeable_months) - set(existing_sheets)))

        if pw_complete_ms != []:
            self.month_sheet_object.auto_control(
                source='StatusRS.show()', mode='iter_build', month_list=pw_complete_ms)

            self.mark_rs_reconciled_status_object(month_list=pw_complete_ms)
        else:
            print(f'all paperwork complete months have been written, current sheets:')
            print(', '.join(existing_sheets))
            print('\n')
            exit

    def final_check_writeable_months(self, month_list=None):
        writeable_months = []
        for month in month_list:
            try:
                status_object = [item for item in StatusObject(
                ).select().where(StatusObject.month == month)][0]
                print(f'\n{month} is ready to write to rent sheets')
            except IndexError as e:
                print(f'{month} is not ready to write to rent sheets\n')

            if status_object.opcash_processed == True and status_object.tenant_reconciled == True and status_object.rs_reconciled == False:
                writeable_months.append(status_object.month)
            elif status_object.scrape_reconciled == True and status_object.tenant_reconciled == True and status_object.rs_reconciled == False:
                writeable_months.append(status_object.month)
        return writeable_months

    def mark_rs_reconciled_status_object(self, month_list=None):
        for month in month_list:
            status_object = [item for item in StatusObject(
            ).select().where(StatusObject.month == month)][0]
            status_object.rs_reconciled = 1
            status_object.save()

    def make_rent_receipts(self, first_incomplete_month=None):
        choice1 = input(
            f'\nWould you like to make rent receipts for period between {first_incomplete_month} ? Y/n ')
        if choice1 == 'Y':
            write_rr_letters = True
        else:
            write_rr_letters = False
        return write_rr_letters

    def make_balance_letters(self, first_incomplete_month=None):
        choice2 = input(
            f'\nWould you like to make balance letters for period between {first_incomplete_month} ? Y/n ')
        if choice2 == 'Y':
            write_bal_letters = True
        else:
            write_bal_letters = False
        return write_bal_letters

    def bal_letter_wrapper(self):
        print('generating balance letters')
        balance_letter_list, mr_good_month = self.generate_balance_letter_list_mr_reconciled()

        if balance_letter_list:
            print(
                f'balance letter list for {mr_good_month}: {balance_letter_list}')

    def rent_receipts_wrapper_version2(self):
        print('generating rent receipts')
        receipts = Letters()
        receipts.rent_receipts_plus_balance()

    def show_balance_letter_list_mr_reconciled(self):
        """triggers off of most recent reconciled month"""
        query = QueryHC()
        mr_good_month, _ = self.get_mr_good_month()
        if mr_good_month:
            first_dt, last_dt = query.make_first_and_last_dates(
                date_str=mr_good_month)

            balance_letters = [rec for rec in BalanceLetter.select(BalanceLetter, Tenant).
                               where(Tenant.active == True).
                               where(
                (BalanceLetter.target_month_end >= first_dt) &
                (BalanceLetter.target_month_end <= last_dt)).
                join(Tenant).
                namedtuples()]

            return balance_letters
        else:
            return []

    def get_mr_good_month(self):
        '''get most recent finalized month'''
        query = QueryHC()
        try:
            good_months = [rec.month for rec in StatusObject().select(StatusObject.month).
                           where(
                ((StatusObject.opcash_processed == 1) &
                 (StatusObject.tenant_reconciled == 1)) |
                ((StatusObject.opcash_processed == 0) &
                 (StatusObject.scrape_reconciled == 1))).
                namedtuples()]
        except IndexError as e:
            print('bypassing error on mr_good_month', e)
            good_months = False
            return good_months, []
        return good_months[-1], good_months

    def generate_balance_letter_list_mr_reconciled(self):
        query = QueryHC()

        mr_good_month, _ = self.get_mr_good_month()

        if mr_good_month:
            first_dt, last_dt = query.make_first_and_last_dates(
                date_str=mr_good_month)
            position_list, cumsum = query.net_position_by_tenant_by_month(
                first_dt=first_dt, last_dt=last_dt)

            bal_letter_list = []
            for rec in position_list:
                if float(rec.end_bal) >= float(100):
                    b_letter = BalanceLetter(
                        tenant=rec.name, end_bal=rec.end_bal, target_month_end=last_dt)
                    b_letter.save()
                    tup = (rec.name, rec.end_bal, last_dt)
                    bal_letter_list.append(tup)
            return bal_letter_list, mr_good_month
        else:
            return [], None

    def write_processed_to_status_rs_db(self, ref_rec=None, report_list=None):
        """Function takes iter of processed files and writes them as json to statusRS db; as far as I can tell this does not do anything important at this time"""
        mr_status = StatusRS().get(StatusRS.status_id == ref_rec.status_id)

        dump_list = []
        for item in report_list:
            for fn, tup in item.items():
                dict1 = {}
                dict1[fn] = tup[0]
                dump_list.append(dict1)

        mr_status.proc_file = json.dumps(dump_list)
        mr_status.save()

    def reconcile_and_inscribe_state(self, month_list=None, ref_rec=None, *args, **kwargs):
        """takes list of months in year to date, gets tenant payments by period, non-tenant payments, and opcash information and reconciles the deposits on the opcash statement to the sum of tenant payments and non-tenant payments
        then updates existing StatusRS db and, most importantly, writes to StatusObject db whether opcash has been processed and whether tenant has reconciled: currently will reconcile a scrape against a deposit list sheets"""
        populate = PopulateTable()
        for month in month_list:
            first_dt, last_dt = populate.make_first_and_last_dates(
                date_str=month)

            ten_payments = sum([float(row[2]) for row in populate.get_payments_by_tenant_by_period(
                first_dt=first_dt, last_dt=last_dt)])
            ntp = sum(populate.get_ntp_by_period(
                first_dt=first_dt, last_dt=last_dt))
            opcash = populate.get_opcash_by_period(
                first_dt=first_dt, last_dt=last_dt)
            # damages = populate.get_damages_by_month(first_dt=first_dt, last_dt=last_dt)
            delete_mentries = populate.get_mentries_by_month(
                first_dt=first_dt, last_dt=last_dt, type1='delete')

            '''probably need to add the concept of "adjustments" in here'''
            sum_from_payments = Reconciler.master_sum_from_payments_totaler(
                ten_payments=ten_payments, non_ten_pay=ntp, period=month)

            if sum_from_payments == 0:
                print(f'no tenant deposit report available for {month}\n')
            elif sum_from_payments != 0 and opcash != []:
                print(f'opcash available for {month}')
                bank_deposits = float(opcash[0][4])
                deposit_corrections = float(opcash[0][5])

                if kwargs['source'] == 'iter':
                    bank_deposits = Reconciler.adjust_bank_deposits(
                        bank_deposits=bank_deposits, deposit_corrections=deposit_corrections)

                # if kwargs['source'] == 'iter' and month == '2022-02':
                #     breakpoint()
                # if kwargs['source'] == 'build' and month == '2022-02':
                #     breakpoint()

                if Reconciler.backend_processing_layer_assert_bank_deposits_tenant_deposits(bank_deposits=bank_deposits, sum_from_payments_report=sum_from_payments, period=month, genus='opcash', source=kwargs['source']):
                    """critical reconciliation logic for statusObject"""
                    mr_status = StatusRS().get(StatusRS.status_id == ref_rec.status_id)

                    try:
                        s_object = StatusObject.create(
                            key=mr_status.status_id, month=month, opcash_processed=True, tenant_reconciled=True)
                        s_object.save()
                    except IntegrityError as e:
                        s_object_id = [row for row in StatusObject.select().where(
                            StatusObject.month == month).namedtuples()][0]
                        s_object = StatusObject.get(id=s_object_id.id)
                        s_object.opcash_processed = True
                        s_object.tenant_reconciled = True
                        s_object.save()
                        print(
                            e, 'opcash: this month has already been created in statusobject table')
            else:
                print(f'no op cash statement available for {month}.')

                """if scrape deposits + adjustments == tenant payments + adjust > we can mark tenant_reconciled & scrape as processed"""

                scrape_corr = populate.get_scrape_detail_by_month_by_type(
                    type1='corr', first_dt=first_dt, last_dt=last_dt)

                scrape_dep = populate.get_scrape_detail_by_month_by_type(
                    type1='deposit', first_dt=first_dt, last_dt=last_dt)

                scrape_corr = sum([float(item) for item in scrape_corr])

                scrape_dep_detail = populate.get_scrape_detail_by_month_deposit(
                    first_dt=first_dt, last_dt=last_dt)

                scrape_deposits = sum([float(item) for item in scrape_dep])

                if Reconciler.backend_processing_layer_assert_bank_deposits_tenant_deposits(bank_deposits=scrape_deposits, sum_from_payments_report=sum_from_payments, period=month, genus='scrape') and sum_from_payments != 0:
                    print(
                        f'scrape asserted ok for {month} {Config.current_year}')
                    mr_status = StatusRS().get(StatusRS.status_id == ref_rec.status_id)
                    try:
                        s_object = StatusObject.create(
                            key=mr_status.status_id, month=month, scrape_reconciled=True, tenant_reconciled=True)
                        s_object.save()
                    except IntegrityError as e:
                        s_object_id = [row for row in StatusObject.select().where(
                            StatusObject.month == month).namedtuples()][0]
                        s_object = StatusObject.get(id=s_object_id.id)
                        s_object.scrape_reconciled = True
                        s_object.tenant_reconciled = True
                        s_object.save()
                        print(
                            e, 'scrape: this month has already been created in statusobject table')
