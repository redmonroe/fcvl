import time
from datetime import datetime as dt
from pathlib import Path

import pandas as pd
from numpy import nan
from peewee import JOIN, fn

from auth_work import oauth
from backend import (Damages, Findexer, NTPayment, OpCash, OpCashDetail, Payment, PopulateTable, QueryHC, StatusObject, StatusRS,
                     Tenant, TenantRent, Unit, db)
from config import Config

from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
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

    def auto_control(self, source=None, mode='clean_build', month_list=None):
        if month_list != None:
            wrange = f'This list has been expressly passed from {source}.'
            print(f'writing rent sheets for {month_list}. {wrange}')
        else:
            wrange = 'This list has been generated from reconciled scrapes or opcash.'
            print('generating list of months where either scrape or opcash has reconciled to tenant.')
            month_list = [rec.month for rec in StatusObject().select().where(       (StatusObject.tenant_reconciled==1) |
                    (StatusObject.scrape_reconciled==1)).namedtuples()]
    
            print(f'reconciled months = {month_list}')

        if mode == 'clean_build':
            self.reset_spreadsheet()

        title_dict = self.make_base_sheet()
        self.formatting_runner(title_dict=title_dict) 
        self.duplicate_formatted_sheets(month_list=month_list)
        self.remove_base_sheet()

        status_list = []
        for date in month_list:
            self.write_rs_col(date)
            try:
                reconciliation_type = [rec.scrape_reconciled for rec in StatusObject().select().where(StatusObject.month==date).namedtuples()][0]              
            except IndexError as e:
                print(f'for {date} you have returned an empty list indicating that your db did not reconcile for that month for File_Indexer or StatusObject')
                raise
            
            if reconciliation_type == True:
                self.write_deposit_detail_from_scrape(date)
            elif reconciliation_type == False:
                self.write_deposit_detail_from_opcash(date)
                    
            ntp = self.get_ntp_wrapper(date)
            sum_laundry, other_list = self.split_ntp(ntp)
            sum_mi_payments = self.get_move_ins(date)
            self.write_move_in_box(date)
            self.write_ntp(date, [sum_laundry], start_row=71)
            self.write_ntp(date, other_list, start_row=72)
            self.write_sum_mi_payments(date, sum_mi_payments)
            status = self.check_totals_reconcile(date)
            status_list.append(status)

        self.report_status(month_list=month_list, status=status_list, wrange=wrange)
    
    def report_status(self, month_list=None, status=None, wrange=None):
        print('\n\tcompleted writing to sheets\n')
        print(f'\n\t\t{month_list}\n')
        print(f'\n\t\t{wrange}\n')
        print(f'\n\t\t{status}')

    def sum_move_in_payments_period(self, mi_payments=None):
        sum_mi_payments = sum([float(item[1]) for item in mi_payments])
        return sum_mi_payments

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
   
        gc.update_int(self.service, self.full_sheet, contract_rent, f'{date}!E2:E68', value_input_option='USER_ENTERED')        
        gc.update_int(self.service, self.full_sheet,subsidy, f'{date}!F2:F68', value_input_option='USER_ENTERED')        
        gc.update(self.service, self.full_sheet, unit, f'{date}!A2:A68')
        gc.update(self.service, self.full_sheet, tenant_names, f'{date}!B2:B68')   
        gc.update_int(self.service, self.full_sheet, beg_bal, f'{date}!D2:D68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, endbal, f'{date}!L2:L68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, charge_month, f'{date}!H2:H68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, pay_month, f'{date}!K2:K68', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, dam_month, f'{date}!J2:J68', value_input_option='USER_ENTERED')

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

    def write_deposit_detail_from_opcash(self, date):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        rec = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        dep_detail = populate.get_opcashdetail_by_stmt(stmt_key=rec[0][0])
        self.export_deposit_detail(date=date, res_rep=rec[0][2], hap=rec[0][3], dep_sum=rec[0][4], corr_sum=rec[0][5], dep_detail=dep_detail)

    def write_deposit_detail_from_scrape(self, date):
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
        hap = populate.get_scrape_detail_by_month_hap(first_dt=first_dt, last_dt=last_dt)
        dep_detail = populate.get_scrape_detail_by_month_deposit(first_dt=first_dt, last_dt=last_dt)
        self.export_deposit_detail(date=date, res_rep=0, hap=hap[0], dep_sum=0, dep_detail=dep_detail)

    def export_deposit_detail(self, **kw):
        gc = GoogleApiCalls()
        date = kw['date']
        gc.update_int(self.service, self.full_sheet, [kw['hap']], f'{date}' + f'{self.wrange_hap_partial}', value_input_option='USER_ENTERED')
        gc.update_int(self.service, self.full_sheet, [kw['res_rep']], f'{date}' + f'{self.wrange_rr_partial}', value_input_option='USER_ENTERED')   
        gc.update_int(self.service, self.full_sheet, [kw['corr_sum']], f'{date}' + f'{self.wrange_corr_partial}', value_input_option='USER_ENTERED')   
        dep_detail_amounts = [item.amount for item in kw['dep_detail']]
        self.write_list_to_col(func=gc.update_int, start_row=82, list1=dep_detail_amounts, col_letter='D', date=date)

    def write_list_to_col(self, **kw):
        start_row = kw['start_row']
        for item in kw['list1']:
            sub_str0 = '!'
            sub_str1 = kw['col_letter']
            sub_str2 = ':'
            cat_str = sub_str0 + sub_str1 + str(start_row) + sub_str2 + sub_str1 + str(start_row)
            kw['func'](self.service, self.full_sheet, [item], f'{kw["date"]}' + cat_str, value_input_option='USER_ENTERED')
            start_row += 1
            
    def check_totals_reconcile(self, date):
        gc = GoogleApiCalls()
        onesite_total = gc.broad_get(self.service, self.full_sheet, f'{date}!K79:K79')
        nbofi_total = gc.broad_get(self.service, self.full_sheet, f'{date}!D90:D90')

        status_list = []
        if onesite_total == nbofi_total:
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

    def get_move_ins(self, date):
        query = QueryHC()
        first_dt, last_dt = query.make_first_and_last_dates(date_str=date)
        move_ins = query.get_move_ins_by_period(first_dt=first_dt, last_dt=last_dt)

        move_in_names = [name[1] for name in move_ins]

        mi_payments = []
        for name in move_in_names:                
            mi_tp = query.get_single_ten_pay_by_period(first_dt=first_dt, last_dt=last_dt, name=name)
            mi_payments.append(mi_tp)

        sum_mi_payments = self.sum_move_in_payments_period(mi_payments=mi_payments)

        return sum_mi_payments

    def capitalize_name(self, tenant_list=None):
        t_list = []
        for name in tenant_list:
            new = [item.rstrip().lstrip().capitalize() for item in name.split(',')]
            t_list.append(', '.join(new))
        return t_list

    def reset_spreadsheet(self):
        print('resetting spreadsheet')
        current_sheets = Utils.get_existing_sheets(self.service, self.full_sheet)
        gc = GoogleApiCalls()
        for name, id2, in current_sheets.items():
            if name != 'intake':
                gc.del_one_sheet(self.service, self.full_sheet, id2)





