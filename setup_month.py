import time
from datetime import datetime as dt
from pathlib import Path

import pandas as pd
import xlsxwriter
from numpy import nan
from peewee import JOIN, fn

from auth_work import oauth
from backend import (Damages, Findexer, NTPayment, OpCash, OpCashDetail,
                     Payment, PopulateTable, QueryHC, StatusObject, StatusRS,
                     Tenant, TenantRent, Unit, db)
from config import Config
from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
from reconciler import Reconciler
from setup_year import YearSheet
from utils import Utils


class MonthSheet(YearSheet):

    wrange_corr_partial = '!D79:D79'
    wrange_rr_partial = '!D80:D80'
    wrange_hap_partial = '!D81:D81'
    wrange_reconciled = '!E90:E90'
    wrange_ntp = '!K71:K71'
    wrange_sum_mi_payments = '!K76:K76'

    def __init__(self, full_sheet, path, mode=None, test_service=None):
        self.full_sheet = full_sheet
        self.file_input_path = path
        if mode == 'testing':
            self.service = test_service
        else:
            self.service = oauth(Config.my_scopes, 'sheet')

        self.gc = GoogleApiCalls()
        self.query = QueryHC()
        self.contract_rent = '!E2:E68'
        self.subsidy = '!F2:F68'
        self.unit = '!A2:A68'
        self.tenant_names = '!B2:B68'
        self.beg_bal = '!D2:D68'
        self.end_bal = '!L2:L68'
        self.charge_month = '!H2:H68'
        self.pay_month = '!K2:K68'
        self.dam_month = '!J2:J68'
    
    def auto_control(self, source=None, mode='clean_build', month_list=None):

        month_list, wrange = self.what_is_month_list(source=source, month_list=month_list)
            
        if mode == 'clean_build':
            self.reset_spreadsheet()
            titles_dict = self.make_base_sheet()
            self.formatting_runner(title_dict=titles_dict) 
            self.duplicate_formatted_sheets(month_list=month_list)
            self.remove_base_sheet()
            status_list = self.to_google_sheets(month_list=month_list)
        elif mode == 'iter_build':
            titles_dict = self.make_base_sheet()
            self.formatting_runner(title_dict=titles_dict) 
            self.duplicate_formatted_sheets(month_list=month_list)
            self.remove_base_sheet()
            status_list = self.to_google_sheets(month_list=month_list)
        elif mode == 'single_sheet':
            month_list = Utils.months_in_ytd(Config.dynamic_current_year, show_choices=True)
            print(f'MAKE SINGLE RENT SHEET FOR {month_list} | DO NOT RESET SHEET.')
            titles_dict = self.make_single_sheet(single_month_list=month_list)
            self.formatting_runner(title_dict=titles_dict) 
        elif mode == 'to_excel':
            status_list = self.to_excel(month_list=month_list)

        self.report_status(month_list=month_list, status=status_list, wrange=wrange)
   
    def to_excel(self, month_list=None):
        import os
        status_list = []
        try:
            writer = pd.ExcelWriter(Config.TEST_EXCEL, engine='xlsxwriter')
        except PermissionError as e:
            print(e)
            breakpoint()
            raise
            # os.system('TASKKILL /F /IM excel.exe')
        df_list = []


        for date in month_list:
            reconciliation_type = self.scrape_or_opcash(date=date)

            df, contract_rent, subsidy, unit, tenant_names, beg_bal, endbal, charge_month, pay_month, dam_month = self.get_rs_col(date)

            df = df[['name', 'unit','beg_bal_at', 'contract_rent', 'subsidy', 'charge_month', 'pay_month', 'dam_month', 'end_bal_m']]

            hap, corr_sum, rr_sum, dep_detail = self.write_deposit_detail_to_excel(date, genus=reconciliation_type)
            ntp = self.get_ntp_wrapper(date)

            new_row1 = pd.Series(['hap', 'corrections', 'rr', 'laundry', 'other'])
            try:
                laundry = ntp[0][0]
            except IndexError as e:
                print(e)
                laundry = 0
           
            try:
                other = ntp[1][0]
            except IndexError as e:
                print(e)
                other = 0
            new_row = pd.Series([hap, corr_sum, rr_sum, laundry, other])
            
            df = df.append(new_row1, ignore_index=True)
            df = df.append(new_row, ignore_index=True)
            


            df_list.append((date, df))

            # breakpoint()

        for item in df_list:
            df = item[1]
            date = item[0]
            df.to_excel(writer, sheet_name=date, header=True)                   

        """I have to do a reconcilation"""
        """I have to mark in StatusObject"""
        writer.save()
        writer.close()
        return status_list

    def to_google_sheets(self, month_list=None):
        status_list = []
        for date in month_list:
            df, contract_rent, subsidy, unit, tenant_names, beg_bal, endbal, charge_month, pay_month, dam_month = self.get_rs_col(date)
            self.write_rs_col(date, contract_rent, subsidy, unit, tenant_names, beg_bal, endbal, charge_month, pay_month, dam_month)

            reconciliation_type = self.scrape_or_opcash(date=date)
            
            self.write_deposit_detail_to_gs(date, genus=reconciliation_type)                    
            ntp = self.get_ntp_wrapper(date)
            sum_laundry, other_list = self.split_ntp(ntp)
            sum_mi_payments = self.get_move_ins(date)
            self.write_move_in_box(date)
            self.write_ntp(date, [sum_laundry], start_row=71)
            self.write_ntp(date, other_list, start_row=72)
            self.write_sum_mi_payments(date, sum_mi_payments)
            status = self.check_totals_reconcile(date)
            status_list.append(status)        
        return status_list

    def report_status(self, month_list=None, status=None, wrange=None):
        print('\n\tcompleted writing to sheets\n')
        print(f'\n\t\t{month_list}\n')
        print(f'\n\t\t{wrange}\n')
        print(f'\n\t\t{status}')

    def scrape_or_opcash(self, date=None):
        try:
            reconciliation_type = [rec.scrape_reconciled for rec in StatusObject().select().where(StatusObject.month==date).namedtuples()][0]              
        except IndexError as e:
            print(f'for {date} you have returned an empty list indicating that your db did not reconcile for that month for File_Indexer or StatusObject')
            raise
        return reconciliation_type
    
    def what_is_month_list(self, source=None, month_list=None):
        """depending on inputs determines whether express list of month's to write is used or function will generate its own lists of months fro StatusObject"""
        if month_list != None:
            wrange = f'MonthSheet: This list has been expressly passed from {source}.'
            print(f'writing rent sheets for {month_list}. {wrange}')
        else:
            wrange = 'MonthSheet: This list has been generated from reconciled scrapes or opcash.'
            print('generating list of months where either scrape or opcash has reconciled to deposits.xls.')
            month_list = [rec.month for rec in StatusObject().select().where(       (StatusObject.tenant_reconciled==1) |
                    (StatusObject.scrape_reconciled==1)).namedtuples()]
    
            print(f'reconciled months = {month_list}')

        return month_list, wrange

    def get_rs_col(self, date):
        first_dt, last_dt = self.query.make_first_and_last_dates(date_str=date)

        np, cumsum = self.query.full_month_position_tenant_by_month(first_dt=first_dt, last_dt=last_dt)

        df = self.index_np_with_df(np)
        unit = df['unit'].tolist()
        tenant_names = Utils.capitalize_name(tenant_list=df['name'].tolist())
        beg_bal = df['lp_endbal'].tolist()
        charge_month = df['charge_month'].tolist()
        pay_month = df['pay_month'].tolist()
        dam_month = df['dam_month'].tolist()
        subsidy = df['subsidy'].tolist()
        contract_rent = df['contract_rent'].tolist()
        endbal = df['end_bal_m'].tolist()

        return df, contract_rent, subsidy, unit, tenant_names, beg_bal, endbal, charge_month, pay_month, dam_month

    def write_rs_col(self, date, *args):
        self.gc.update_int(self.service, self.full_sheet, args[0], f'{date}' + self.contract_rent, value_input_option='USER_ENTERED')        
        self.gc.update_int(self.service, self.full_sheet, args[1], f'{date}' + self.subsidy, value_input_option='USER_ENTERED')        
        self.gc.update(self.service, self.full_sheet, args[2], f'{date}' + self.unit)
        self.gc.update(self.service, self.full_sheet, args[3], f'{date}' + self.tenant_names)   
        self.gc.update_int(self.service, self.full_sheet, args[4], f'{date}' + self.beg_bal, value_input_option='USER_ENTERED')
        self.gc.update_int(self.service, self.full_sheet, args[5], f'{date}' + self.end_bal, value_input_option='USER_ENTERED')
        self.gc.update_int(self.service, self.full_sheet, args[6], f'{date}' + self.charge_month, value_input_option='USER_ENTERED')
        self.gc.update_int(self.service, self.full_sheet, args[7], f'{date}' + self.pay_month, value_input_option='USER_ENTERED')
        self.gc.update_int(self.service, self.full_sheet, args[8], f'{date}' + self.dam_month, value_input_option='USER_ENTERED')
   
    def get_ntp_wrapper(self, date):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        agg_ntp = populate.get_ntp_by_period_and_type(first_dt=first_dt, last_dt=last_dt)
        return agg_ntp

    def split_ntp(self, ntp=None):
        sum_laundry = sum([amount for amount, genus in ntp if genus == 'laundry'])
        other_list = [amount for amount, genus in ntp if genus != 'laundry']
        return sum_laundry, other_list

    def write_sum_mi_payments(self, date, data):
        gc = GoogleApiCalls()
        gc.update_int(self.service, self.full_sheet, [data], f'{date}' + f'{self.wrange_sum_mi_payments}', value_input_option='USER_ENTERED')   

    def write_ntp(self, date, data, start_row=None):
        gc = GoogleApiCalls()
        self.write_list_to_col(func=gc.update_int, start_row=start_row, list1=data, col_letter='K', gc=gc, date=date)

    def write_move_in_box(self, date):
        populate = PopulateTable()
        gc = GoogleApiCalls()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        mi_list_to_write = populate.get_move_ins_by_period(first_dt=first_dt, last_dt=last_dt)
        if mi_list_to_write == []:
            mi_write_item = ['no move ins this month']
            gc.format_row(self.service, self.full_sheet, f'{date}!B73:B73', "ROWS", mi_write_item)
        else:
            names_list = [item[1] for item in mi_list_to_write]
            dates_list = [item[0] for item in mi_list_to_write]
            self.write_list_to_col(func=gc.update, start_row=73, list1=names_list, col_letter='B', date=date)
            self.write_list_to_col(func=gc.update, start_row=73, list1=dates_list, col_letter='C', date=date)

    def scrape_dep_detail_func_list(self, date=None):
        print(f'writing deposit detail from scrape {date}')
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        hap = populate.get_scrape_detail_by_month_by_type(type1='hap', first_dt=first_dt, last_dt=last_dt)
        dep_correction_sum = populate.get_scrape_detail_by_month_by_type(type1='corr', first_dt=first_dt, last_dt=last_dt)
        dep_correction_sum = sum([float(item) for item in dep_correction_sum])
        dep_detail = populate.get_scrape_detail_by_month_deposit(first_dt=first_dt, last_dt=last_dt)

        """scrape does not pick up replacement_reserve amounts from csv"""
        """vvv"""
        res_rep = 0
        return hap[0], dep_correction_sum, dep_detail

    def opcash_dep_detail_func_list(self, date=None):
        print(f'writing deposit detail from opcash {date}')
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        rec = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        dep_detail = populate.get_opcashdetail_by_stmt(stmt_key=rec[0][0])
        hap = rec[0][3]
        corr_sum = rec[0][5]
        rr_sum = rec[0][2]

        return hap, corr_sum, rr_sum, dep_detail

    def write_deposit_detail_to_excel(self, date, genus=None):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        if genus == True:
            hap, corr_sum, rr_sum, dep_detail = self.scrape_dep_detail_func_list(date=date)
        elif genus == False:
            hap, corr_sum, rr_sum, dep_detail = self.opcash_dep_detail_func_list(date=date)

        return hap, corr_sum, rr_sum, dep_detail

    def write_deposit_detail_to_gs(self, date, genus=None):
        if genus == True:
            hap, corr_sum, rr_sum, dep_detail = self.scrape_dep_detail_func_list(date=date)
        elif genus == False:
            hap, corr_sum, rr_sum, dep_detail = self.opcash_dep_detail_func_list(date=date)

        self.export_deposit_detail(date=date, res_rep=rr_sum, hap=hap, corr_sum=corr_sum, dep_detail=dep_detail)

    def export_deposit_detail(self, **kw):
        date = kw['date']
        self.gc.update_int(self.service, self.full_sheet, [kw['hap']], f'{date}' + f'{self.wrange_hap_partial}', value_input_option='USER_ENTERED')
        self.gc.update_int(self.service, self.full_sheet, [kw['res_rep']], f'{date}' + f'{self.wrange_rr_partial}', value_input_option='USER_ENTERED')   
        self.gc.update_int(self.service, self.full_sheet, [kw['corr_sum']], f'{date}' + f'{self.wrange_corr_partial}', value_input_option='USER_ENTERED')  
        print(date, 'writing deposit corrections to gsheet:', kw['corr_sum'] ) 
        dep_detail_amounts = [item.amount for item in kw['dep_detail']]
        self.write_list_to_col(func=self.gc.update_int, start_row=82, list1=dep_detail_amounts, col_letter='D', date=date)

    def write_list_to_col(self, **kw):
        start_row = kw['start_row']
        for item in kw['list1']:
            sub_str0 = '!'
            sub_str1 = kw['col_letter']
            sub_str2 = ':'
            cat_str = sub_str0 + sub_str1 + str(start_row) + sub_str2 + sub_str1 + str(start_row)
            kw['func'](self.service, self.full_sheet, [item], f'{kw["date"]}' + cat_str, value_input_option='USER_ENTERED')
            start_row += 1
            
    def check_totals_reconcile(self, date=None):
        gc = GoogleApiCalls()
        onesite_total = gc.broad_get(self.service, self.full_sheet, f'{date}!K79:K79')
        nbofi_total = gc.broad_get(self.service, self.full_sheet, f'{date}!D90:D90')

        status_list = []
        if Reconciler.month_sheet_final_check(onesite_total=onesite_total, nbofi_total=nbofi_total, period=date, genus='rent sheet'):
            message = [f'balances at {str(dt.today().date())}']
            status_object_row = [(row.id, row.month) for row in StatusObject.select().where(StatusObject.month==date).namedtuples()][0]
            status_object = StatusObject.get(status_object_row[0])
            status_object.rs_reconciled = True
            status_object.save()
            gc.update(self.service, self.full_sheet, message, f'{date}' + self.wrange_reconciled)
            dict1 = {date: message}
            status_list.append(dict1)
        else:
            message = ['does not balance']
            gc.update(self.service, self.full_sheet, message, f'{date}' + self.wrange_reconciled)
            status_object_row = [(row.id, row.month) for row in StatusObject.select().where(StatusObject.month==date).namedtuples()][0]
            status_object = StatusObject.get(status_object_row[0])
            status_object.rs_reconciled = False
            status_object.save()
            dict1 = {date: message}
            status_list.append(dict1)

        return status_list

    def index_np_with_df(self, np):
        final_list = []
        idx_list = []
        for index, unit in enumerate([unit.unit_name for unit in Unit().select()]): 
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

    def get_move_ins(self, date):
        query = QueryHC()
        first_dt, last_dt = query.make_first_and_last_dates(date_str=date)
        move_ins = query.get_move_ins_by_period(first_dt=first_dt, last_dt=last_dt)

        move_in_names = [name[1] for name in move_ins]

        mi_payments = []
        for name in move_in_names:                
            mi_tp = query.get_single_ten_pay_by_period(first_dt=first_dt, last_dt=last_dt, name=name)
            mi_payments.append(mi_tp)

        return sum([float(item[1]) for item in mi_payments])

    def reset_spreadsheet(self):
        print('resetting spreadsheet')
        current_sheets = Utils.get_existing_sheets(self.service, self.full_sheet)
        gc = GoogleApiCalls()
        for name, id2, in current_sheets.items():
            if name != 'intake':
                gc.del_one_sheet(self.service, self.full_sheet, id2)
    
    def delete_one_month_sheet(self, *args, **kwargs):
        gc = GoogleApiCalls()
        titles_dict = Utils.get_existing_sheets(args[0], args[1])
        titles_dict = {name:id2 for name, id2 in titles_dict.items() if name != 'intake'}
        path = Utils.show_files_as_choices(titles_dict, interactive=True)
        gc = GoogleApiCalls()
        for name, id2 in titles_dict.items():
            if path[0] == name:
                gc.del_one_sheet(args[0], args[1], id2)


        



