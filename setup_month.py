import time
from datetime import datetime as dt
from pathlib import Path

import pandas as pd
from numpy import nan
from peewee import JOIN

from auth_work import oauth
from backend import (Damages, Findexer, NTPayment, OpCash, OpCashDetail,
                     Payment, PopulateTable, QueryHC, StatusObject, StatusRS,
                     Tenant, TenantRent, Unit, db)
from config import Config, my_scopes
from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
from utils import Utils


class MonthSheet:

    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Subsity Entitlement',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    G_SUM_KRENT = ["=sum(E2:E68)"]
    G_SUM_ACTSUBSIDY = ["=sum(F2:F68)"]
    G_SUM_ACTRENT = ["=sum(H2:H68)"]
    G_PAYMENT_MADE = ["=sum(K2:K68)"]
    G_CURBAL = ["=sum(L2:L68)"]
    G_DEPDETAIL = ["=sum(D82:D89)"]
    wrange_hap_partial = '!D81:D81'
    wrange_rr_partial = '!D80:D80'
    wrange_reconciled = '!E90:E90'
    wrange_ntp = '!K71:K71'

    def __init__(self, full_sheet, path, sleep, mode=None, test_service=None):

        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
            self.sleep = sleep
        else:
            self.service = oauth(my_scopes, 'sheet')
        
        self.file_input_path = path

    def auto_control(self, month_list=None):
        if month_list == None:
            month_list = [rec.month for rec in StatusObject().select().where(StatusObject.tenant_reconciled==1).namedtuples()]
        for date in month_list:
            # self.export_month_format(date)
            # self.write_rs_col(date)
            # self.write_deposit_detail(date)
            ntp = self.get_ntp_wrapper(date)
            self.check_totals_reconcile(date, ntp)
       
    def write_rs_col(self, date):
        gc = GoogleApiCalls()
        query = QueryHC()
        first_dt, last_dt = query.make_first_and_last_dates(date_str=date)

        np, cumsum = query.full_month_position_tenant_by_month(first_dt=first_dt, last_dt=last_dt)

        df = self.index_np_with_df(np)
        unit = df['unit'].tolist()
        tenant_names = self.capitalize_name(tenant_list=df['name'].tolist())
        beg_bal = df['lp_endbal'].tolist()
        charge_month = df['charge_month'].tolist()
        pay_month = df['pay_month'].tolist()
        dam_month = df['dam_month'].tolist()
        subsidy = df['subsidy'].tolist()
        contract_rent = df['contract_rent'].tolist()
        endbal = df['end_bal_m'].tolist()
        s_endbal = sum(endbal)
   
        gc.update_int(self.service, self.full_sheet, contract_rent, f'{date}!E2:E68', value_input_option='USER_ENTERED')        
        gc.update_int(self.service, self.full_sheet,subsidy, f'{date}!F2:F68', value_input_option='USER_ENTERED')        
        gc.update(self.service, self.full_sheet, unit, f'{date}!A2:A68')
        gc.update(self.service, self.full_sheet, tenant_names, f'{date}!B2:B68')   
        gc.update_int(self.service, self.full_sheet, beg_bal, f'{date}!D2:D68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, endbal, f'{date}!L2:L68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, charge_month, f'{date}!H2:H68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, pay_month, f'{date}!K2:K68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, dam_month, f'{date}!J2:J68', value_input_option='USER_ENTERED')

    def export_month_format(self, sheet_choice):
        gc = GoogleApiCalls()
        gc.format_row(self.service, self.full_sheet, f'{sheet_choice}!A1:M1', "ROWS", self.HEADER_NAMES)
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_KRENT, f'{sheet_choice}!E69:E69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTSUBSIDY, f'{sheet_choice}!F69:F69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTRENT, f'{sheet_choice}!H69:H69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_PAYMENT_MADE, f'{sheet_choice}!K69:K69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_CURBAL, f'{sheet_choice}!L69:L69')

    def get_ntp_wrapper(self, date):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        return populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt)

    def write_ntp(self, date, data):
        gc = GoogleApiCalls()
        breakpoint()
        gc.update_int(self.service, self.full_sheet, data, date + self.wrange_ntp, 'USER_ENTERED')

    def write_deposit_detail(self, date):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        rec = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        dep_detail = populate.get_opcashdetail_by_stmt(stmt_key=rec[0][0])
        self.export_deposit_detail(date=date, res_rep=rec[0][2], hap=rec[0][3], dep_sum=rec[0][4], dep_detail=dep_detail)

    def export_deposit_detail(self, **kw):
        gc = GoogleApiCalls()
        date = kw['date']
        gc.update_int(self.service, self.full_sheet, [kw['hap']], f'{date}' + f'{self.wrange_hap_partial}', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, [kw['res_rep']], f'{date}' + f'{self.wrange_rr_partial}', value_input_option='USER_ENTERED')
        self.split_and_write_dep_detail(dep_detail=kw['dep_detail'], gc=gc, date=date)
        self.write_sum_forumula1(date=date)

    def split_and_write_dep_detail(self, **kw):
        value = 82
        for dep_amt in kw['dep_detail']:
            sub_str0 = '!'
            sub_str1 = 'D'
            sub_str2 = ':'
            cat_str = sub_str0 + sub_str1 + str(value) + sub_str2 + sub_str1 + str(value)
            kw['gc'].update_int(self.service, self.full_sheet, [dep_amt.amount], f'{kw["date"]}' + cat_str, value_input_option='USER_ENTERED')
            value += 1

    def write_sum_forumula1(self, date):
        gc = GoogleApiCalls()
        gc.write_formula_column(self.service, self.full_sheet, self.G_DEPDETAIL, f'{date}!D90:D90')
    
    def check_totals_reconcile(self, date):
        gc = GoogleApiCalls()
        onesite_total = gc.broad_get(self.service, self.full_sheet, f'{date}!K77:K77')
        ntp = populate.get_ntp_by_period()
        nbofi_total = gc.broad_get(self.service, self.full_sheet, f'{date}!D90:D90')
        if onesite_total == nbofi_total:
            message = [f'balances at {str(dt.today().date())}']
            gc.update(self.service, self.full_sheet, message, f'{date}' + self.wrange_reconciled)
            return True
        else:
            message = ['does not balance']
            gc.update(self.service, self.full_sheet, message, f'{date}' + self.wrange_reconciled)
            return False

    def index_np_with_df(self, np):
        unit_raw = [unit.unit_name for unit in Unit().select()]

        final_list = []
        idx_list = []
        for index, unit in enumerate(unit_raw): 
            idx_list.append(int(index))
            final_list.append(unit)

        unit_index = tuple(zip(idx_list, final_list))
    
        ui_df = pd.DataFrame(unit_index, columns=['Rank', 'unit'])
        df = pd.DataFrame(np, columns=['name', 'beg_bal_at', 'lp_endbal', 'pay_month', 'charge_month', 'dam_month', 'end_bal_m', 'st_date', 'end_date',  'unit', 'subsidy', 'contract_rent'])
        df = df.set_index('unit')

        # merge indexes to order units in way we always have
        merged_df = pd.merge(df, ui_df, on='unit', how='outer')
        final_df = merged_df.sort_values(by='Rank', axis=0)
        df = final_df.set_index('Rank')
        return df

    def capitalize_name(self, tenant_list=None):
        t_list = []
        for name in tenant_list:
            new = [item.rstrip().lstrip().capitalize() for item in name.split(',')]
            t_list.append(', '.join(new))
        return t_list





