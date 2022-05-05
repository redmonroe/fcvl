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
    ui_sheet = 'intake'
    wrange_hap_partial = '!D81:D81'
    wrange_rr_partial = '!D80:D80'
    range1 = '1'
    range2 = '100'
    wrange_unit1 = f'{ui_sheet}!A{range1}:A{range2}'
    wrange_t_name1 = f'{ui_sheet}!B{range1}:B{range2}'
    wrange_k_rent1 = f'{ui_sheet}!c{range1}:c{range2}'
    wrange_subsidy1 = f'{ui_sheet}!d{range1}:d{range2}'
    wrange_t_rent1 = f'{ui_sheet}!e{range1}:e{range2}'

    def __init__(self, full_sheet, path, sleep, mode=None, test_service=None):

        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
            self.sleep = sleep
        else:
            self.service = oauth(my_scopes, 'sheet')
        
        self.file_input_path = path
        self.wrange_unit = self.wrange_unit1
        self.wrange_t_name = self.wrange_t_name1
        self.wrange_k_rent = self.wrange_k_rent1 
        self.wrange_subsidy = self.wrange_subsidy1
        self.wrange_t_rent = self.wrange_t_rent1 
        self.wrange_reconciled = '!E90:E90'
        self.t_name = []
        self.unit = [] 
        self.k_rent = [] 
        self.subsidy = [] 
        self.t_rent = []
        self.sheet_choice = None
        self.gc = GoogleApiCalls()

    def auto_control(self, month_list=None):
        if month_list == None:
            month_list = [rec.month for rec in StatusObject().select().where(StatusObject.tenant_reconciled==1).namedtuples()]
        for date in month_list:
            # self.export_month_format(date)
            self.month_write_col(date)
       
    def month_write_col(self, date):
        '''we are using pandas here and the funcs I've already made'''
        import numpy as np
        # from peewee import fn
        gc = GoogleApiCalls()
        query = QueryHC()
        first_dt, last_dt = query.make_first_and_last_dates(date_str=date)

        # unit+, tenants & vacants+, tenant rent

        list1 = []

        np = query.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
        # rr, vacants, tenants = query.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)
        # tenants_mi_on_or_before_first = [(rec.tenant_name, rec.unit, float(rec.beg_bal_amount), rec.rent_amount, rec.rent_date) for rec in Tenant().select(Tenant, TenantRent.rent_amount, Payment.amount 
        
        # TenantRent.rent_date,
        # fn.SUM(Payment.amount).over(partition_by=[Tenant.tenant_name]).alias('total_pay')
        # ).
        
        # where(
        #     (Tenant.move_in_date<=first_dt) &
        #     ((Tenant.move_out_date=='0') | (Tenant.move_out_date>=first_dt))).
        # where(TenantRent.rent_date>=first_dt).
        # where(TenantRent.rent_date<=last_dt).
        # where(Payment.date_posted>=first_dt).
        # where(Payment.date_posted<=last_dt).
        # join(TenantRent, Payment).
        # namedtuples()]

        # return list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
        #     Tenant.tenant_name, 
        #     Tenant.beg_bal_amount, 
        #     where(Payment.date_posted >= first_dt).
        #     where(Payment.date_posted <= last_dt).
        #     join(Payment).namedtuples()]))

        # unit_raw = [unit.unit_name for unit in Unit().select()]
        # unit_index = self.make_unit_index(unit_raw)
        # df = pd.DataFrame(np, columns=['name', 'unit'])

        # for idx, unit in df.iterrows():
        #     for name, unt in rr:
        #         if str(unit.unit) == unt:
        #             tup = (unt, name)
        #             list1.append(tup)

        # df = pd.DataFrame(list1, columns=['unit', 'name'])

        # beg_bal = query.get_all_tenants_beg_bal()

        # list2 = []
        # for idx, row in df.iterrows():
        #     for item in beg_bal:
        #         if item[0] == row['name']:
        #             tup = (row['unit'], row['name'], float(item[2]))
        #             list2.append(tup)


        breakpoint()

        
        
        
        '''        
        gc.update(self.service, self.full_sheet, unit, f'{sheet_choice}!A2:A68')
        
        gc.update(self.service, self.full_sheet, tenant_names, f'{sheet_choice}!B2:B68')       

        '''
        # gc.update_int(self.service, self.full_sheet, contract_rent, f'{sheet_choice}!E2:E68', value_input_option='USER_ENTERED')
        
        # gc.update_int(self.service, self.full_sheet,subsidy, f'{sheet_choice}!F2:F68', value_input_option='USER_ENTERED')
        
        # gc.update_int(self.service, self.full_sheet, tenant_rent, f'{sheet_choice}!H2:H68', value_input_option='USER_ENTERED')

    def export_month_format(self, sheet_choice):
        gc = GoogleApiCalls()
        time.sleep(self.sleep)
        gc.format_row(self.service, self.full_sheet, f'{sheet_choice}!A1:M1', "ROWS", self.HEADER_NAMES)
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_KRENT, f'{sheet_choice}!E69:E69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTSUBSIDY, f'{sheet_choice}!F69:F69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTRENT, f'{sheet_choice}!H69:H69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_PAYMENT_MADE, f'{sheet_choice}!K69:K69')
        gc.write_formula_column(self.service, self.full_sheet, self.G_CURBAL, f'{sheet_choice}!L69:L69')
        print(f'exported month format to {sheet_choice} with wait time of {self.sleep} seconds')

    # def show_current_sheets(self, interactive=False):
    #     print('showing current sheets')
    #     titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
            
    #     path = Utils.show_files_as_choices(titles_dict, interactive=interactive)
    #     if interactive == True:
            
    #         return path
    #     return titles_dict

    # def walk_download_folder(self):
    #     print('showing ALL items in download folder')
    #     current_items = [p for p in pathlib.Path(self.file_input_path).iterdir() if p.is_file()]
    #     for item in current_items:
    #         print(item.name)

    # def read_excel_ms(self, verbose=False):
    #     df = pd.read_excel(self.file_input_path, header=16)
    #     # jan len is 68
    #     if verbose: 
    #         pd.set_option('display.max_columns', None)
    #         print(df.head(100))
    #     if len(df) > 68:
    #         df = self.check_for_mo(df)

    #     t_name = df['Name'].tolist()
    #     unit = df['Unit'].tolist()
    #     k_rent = self.str_to_float(df['Lease Rent'].tolist())
    #     t_rent = self.str_to_float(df['Actual Rent Charge'].tolist())
    #     subsidy = self.str_to_float(df['Actual Subsidy Charge'].tolist())

    #     return self.fix_data(t_name), self.fix_data(unit), self.fix_data(k_rent), self.fix_data(subsidy), self.fix_data(t_rent)

    # def str_to_float(self, list1):
    #     list1 = [item.replace(',', '') for item in list1]
    #     list1 = [float(item) for item in list1]
    #     return list1

    # def check_for_mo(self, df):
    #     list1 = df['Lease Rent'].tolist()
    #     list1 = [True for item in list1 if item is nan]

    #     if len(list1) == 0:
    #         print('No move out')
    #     else:
    #         print('found a move out')
    #         move_out_list = []
    #         for index, row in df.iterrows():
    #             row_lr = row['Lease Rent']
    #             row_mr = row['Market/\nNote Rate\nRent']
    #             if row_lr is nan and row_mr is nan:
    #                 move_out_list.append(index)

    #         df = df.drop(index=move_out_list, axis=0)
    #     return df
    
    # def fix_data(self, item):
    #     item.pop()
    #     return item

    def write_to_rs(self):
        gc = GoogleApiCalls()
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_unit, self.unit, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_t_name, self.t_name, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_k_rent, self.k_rent, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_subsidy, self.subsidy, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_t_rent, self.t_rent, 'COLUMNS')

    def export_deposit_detail(self, data):
        time.sleep(self.sleep)
        gc = GoogleApiCalls()
        sheet_choice = data['formatted_hap_date']
        print(f'export deposit detail to {sheet_choice} with wait time of {self.sleep} seconds')
        self.sheet_choice = sheet_choice
        hap = [data['hap_amount']]
        rr = [data['rr_amount']]
        deposit_list = data['deposit_list']
        gc.update_int(self.service, self.full_sheet, hap, f'{sheet_choice}' + f'{self.wrange_hap_partial}', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, rr, f'{sheet_choice}' + f'{self.wrange_rr_partial}', value_input_option='USER_ENTERED')
        value = 82
        for dep_amt in deposit_list:
            sub_str0 = '!'
            sub_str1 = 'D'
            sub_str2 = ':'
            cat_str = sub_str0 + sub_str1 + str(value) + sub_str2 + sub_str1 + str(value)
            gc.update_int(self.service, self.full_sheet, [dep_amt[1]], f'{sheet_choice}' + cat_str, value_input_option='USER_ENTERED')
            value += 1

    def write_sum_forumula1(self):
        gc = GoogleApiCalls()
        gc.write_formula_column(self.service, self.full_sheet, self.G_DEPDETAIL, f'{self.sheet_choice}!D90:D90')
    
    def check_totals_reconcile(self):
        gc = GoogleApiCalls()
        onesite_total = gc.broad_get(self.service, self.full_sheet, f'{self.sheet_choice}!K77:K77')
        nbofi_total = gc.broad_get(self.service, self.full_sheet, f'{self.sheet_choice}!D90:D90')
        if onesite_total == nbofi_total:
            message = [f'balances at {str(dt.today().date())}']
            gc.update(self.service, self.full_sheet, message, f'{self.sheet_choice}' + '!E90:E90')
            return True
        else:
            message = ['does not balance']
            gc.update(self.service, self.full_sheet, message, f'{self.sheet_choice}' + '!E90:E90')
            return False

    def make_unit_index(self, units):
        '''this func is moved to setup_month.py and should be deprecated and culled'''
        final_list = []
        idx_list = []
        for index, unit in enumerate(units): # indexes units from sheet
            idx_list.append(int(index))
            final_list.append(unit)

        unit_index = tuple(zip(idx_list, final_list))
        return unit_index
    
    def get_tables(self):
        DBUtils.get_tables(self, self.db)

    def delete_table(self):
        db = Config
        DBUtils.delete_table(self, self.db)




