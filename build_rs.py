import json
import time
from calendar import month_name
from datetime import datetime

import dataset
import pandas as pd
from peewee import JOIN, fn

from auth_work import oauth
from backend import (BalanceLetter, Damages, Findexer, NTPayment, OpCash,
                     OpCashDetail, Payment, PopulateTable, StatusObject,
                     StatusRS, Tenant, TenantRent, Unit, db)
from config import Config, my_scopes
from db_utils import DBUtils
from file_indexer import FileIndexer
from google_api_calls_abstract import GoogleApiCalls
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet


class BuildRS(MonthSheet):
    def __init__(self, sleep=None, full_sheet=None, path=None, mode=None, main_db=None, rs_tablename=None, mformat_obj=None, test_service=None, mformat=None):

        self.main_db = main_db
        self.full_sheet = full_sheet
        self.path = path
        try:
            self.service = oauth(my_scopes, 'sheet')
        except FileNotFoundError as e:
            print(e, 'using testing configuration for Google Api Calls')
            self.service = oauth(Config.my_scopes, 'sheet', mode='testing')
        self.mformat = mformat
        self.calls = GoogleApiCalls() 
        self.sleep = sleep
        self.create_tables_list = [BalanceLetter, StatusRS, StatusObject, OpCash, OpCashDetail, Damages, Tenant, Unit, Payment, NTPayment, TenantRent, Findexer]

        self.target_bal_load_file = 'beginning_balance_2022.xlsx'
        self.wrange_pay = '!K2:K68'
        self.wrange_ntp = '!K71:K71'
        self.file_input_path = path
        self.df = None
        self.proc_ms_list = []
        self.proc_condition_list = None
        self.final_to_process_list = []
        self.month_complete_is_true_list = []

        self.good_opcash_list = []
        self.good_rr_list = []
        self.good_dep_list = []
        self.good_hap_list = []
        self.good_dep_detail_list = []

    def __repr__(self):
        return f'BuildRS object path: {self.file_input_path} write sheet: {self.full_sheet} service:{self.service}'
    
    @record
    def new_auto_build(self):
        print('new_auto_build')
        populate = PopulateTable()
        findex = FileIndexer(path=self.path, db=self.main_db)
        findex.drop_findex_table() 
        if self.main_db.is_closed() == True:
            self.main_db.connect()
        self.main_db.drop_tables(models=self.create_tables_list)
        self.main_db.create_tables(self.create_tables_list)

        findex.build_index_runner() # this is a findex method
        self.load_initial_tenants_and_balances()
        processed_rentr_dates_and_paths = self.iterate_over_remaining_months()
        
        Damages.load_damages()

        populate.transfer_opcash_to_db() # PROCESSED OPCASHES MOVED INTO DB

        status = StatusRS()
        status.set_current_date()
        status.show()
        self.main_db.close()

    def iterate_over_remaining_months(self):       
        # load remaining months rent
        populate = PopulateTable()
        rent_roll_list = [(item.fn, item.period, item.path) for item in Findexer().select().
            where(Findexer.doc_type == 'rent').
            where(Findexer.status == 'processed').
            where(Findexer.period != '2022-01').
            namedtuples()]

        processed_rentr_dates_and_paths = [(item[1], item[2]) for item in rent_roll_list]
        processed_rentr_dates_and_paths.sort()

        # iterate over dep
        for date, filename in processed_rentr_dates_and_paths:
            cleaned_nt_list, total_tenant_charges, cleaned_mos = populate.after_jan_load(filename=filename, date=date)
            
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)
            if date == '2022-03':
                total_rent_charges = populate.get_total_rent_charges_by_month(first_dt=first_dt, last_dt=last_dt)
                assert total_rent_charges == 15972.0 

                vacant_snapshot_loop_end = Unit.find_vacants()
                assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115', 'PT-211'])

        file_list = [(item.fn, item.period, item.path) for item in Findexer().select().
            where(Findexer.doc_type == 'deposits').
            where(Findexer.status == 'processed').
            namedtuples()]
        
        processed_dates_and_paths = [(item[1], item[2]) for item in file_list]
        processed_dates_and_paths.sort()
        
        for date1, path in processed_dates_and_paths:
            grand_total, ntp, tenant_payment_df = populate.payment_load_full(filename=path)
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=date1)
        return processed_rentr_dates_and_paths

    def load_initial_tenants_and_balances(self):
         # load tenants as 01-01-2022
        populate = PopulateTable()
        records = [(item.fn, item.period, item.path) for item in Findexer().select().
            where(Findexer.doc_type == 'rent').
            where(Findexer.status == 'processed').
            where(Findexer.period == '2022-01').
            namedtuples()]

        january_rent_roll_path = records[0][2]
        jan_date = records[0][1]

        # business logic to load inital tenants; cutoff '2022-01'
        nt_list, total_tenant_charges, explicit_move_outs = populate.init_tenant_load(filename=january_rent_roll_path, date=jan_date)

        vacant_units = Unit.find_vacants()
        assert 'PT-201' and 'CD-115' and 'CD-101' in vacant_units

        # load tenant balances at 01012022
        dir_items = [item.name for item in self.path.iterdir()]
        target_balance_file = self.path.joinpath(self.target_bal_load_file)
        populate.balance_load(filename=target_balance_file)


    def iterative_build(self, checklist_mode=None):
        breakpoint()
        for item in self.good_rr_list:
            self.write_rentroll(item)

        for item in self.good_dep_list:
            self.write_payments(item)

        for item in self.good_opcash_list: 
            print('writing from deposit_detail from db')
            self.write_opcash_detail_from_db(item)
           

    def write_opcash_detail_from_db(self, item):
        dict1 = {}
        dict1['formatted_hap_date'] = self.fix_date(item['period'])
        dict1['hap_date'] = item['period']
        dict1['hap_amount'] = item['hap']
        dict1['rr_date'] = item['period']
        dict1['rr_amount'] = item['rr']
        dict1['deposit_list_date'] = item['period']
        dict1['deposit_list'] = json.loads(item['dep_list'])[0]
        
        self.export_deposit_detail(data=dict1)
        self.checklist.check_depdetail_proc(dict1['hap_date'])
        self.checklist.check_opcash(dict1['hap_date'])
        self.write_sum_forumula1()
        reconciled_bool = self.check_totals_reconcile()
        if reconciled_bool:
            self.checklist.check_grand_total_ok(dict1['hap_date'])
        else:
            month = dict1['hap_date']
            print(f'rent sheet for {month} does not balance.')

    def write_opcash_detail(self, item):
        '''does not write from memory properly; would like it to'''
        for good_date, hap in zip(self.proc_ms_list, self.findex.hap_list):
            if self.fix_date4(good_date) == next(iter(hap[0])): # right = 01 2022
                dict1 = {}
                dict1['formatted_hap_date'] = self.fix_date2(next(iter(hap[0])))
                dict1['hap_date'] = next(iter(hap[0]))
                dict1['hap_amount'] = next(iter(hap[0].values()))[0]

        for good_date, rr in zip(self.proc_ms_list, self.findex.rr_list):
            if self.fix_date4(good_date) == next(iter(rr[0])): # right = 01 2022
                dict1['rr_date'] = next(iter(rr[0]))
                dict1['rr_amount'] = next(iter(rr[0].values()))[0]

        # this is for a total deposit: do I need it? DON'T ERASE!
        for good_date, d_sum in zip(self.proc_ms_list, self.findex.dep_list):
            if self.fix_date4(good_date) == next(iter(d_sum[0])): # right = 01 2022
                dict1['deposit_date'] = next(iter(d_sum[0]))

        for good_date, d_detail in zip(self.proc_ms_list, self.findex.deposit_and_date_list):
            if self.fix_date4(good_date) == next(iter(d_detail[0])): # right = 01 2022
                dict1['deposit_list_date'] = next(iter(d_detail[0]))
                dict1['deposit_list'] = list(d_detail[0].values())[0]

            self.export_deposit_detail(data=dict1)
            self.checklist.check_depdetail_proc(dict1['hap_date'])
            self.checklist.check_opcash(dict1['hap_date'])
            self.write_sum_forumula1()
            reconciled_bool = self.check_totals_reconcile()
            if reconciled_bool:
                self.checklist.check_grand_total_ok(dict1['hap_date'])
            else:
                month = dict1['hap_date']
                print(f'rent sheet for {month} does not balance.')

    def write_rentroll(self, item):
        dt_object = self.fix_date(item['period'])

        '''trigger formatting of dt_object named sheet'''
        self.mformat.export_month_format(dt_object)
        self.mformat.push_one_to_intake(input_file_path=item['path'])
        self.checklist.check_mfor(dt_object)
        self.month_write_col(dt_object)

    def write_payments(self, item):
        '''get raw deposit items to sql'''
        dt_object = self.fix_date(item['period'])

        df = self.read_excel_rs(path=item['path'])
        df = self.remove_nan_lines(df=df)
        self.to_sql(df=df)
        dt_code = item['period'][-2:]
        '''group objects by tenant name or unit: which was it?'''
        payment_list, grand_total, ntp, df = self.push_to_sheet_by_period(dt_code=dt_code)
        self.write_payment_list(dt_object, payment_list)
        self.write_ntp(dt_object, [str(ntp)])
        self.print_summary(payment_list, grand_total, ntp, df)

    def remove_already_made_sheets_from_list(self, input_dict=None):
        for mon_year, id1 in input_dict.items():
            for month in self.final_to_process_list:
                already_month = month + ' ' + str(Config.current_year)
                if mon_year == already_month:
                    print(f'{month} already exists in Google Sheets')
                    self.final_to_process_list.remove(month)
        return self.final_to_process_list
    
    def get_name_from_record(self, record):
        name = record['fn'].split('_')[0]
        return name 

    def push_to_sheet_by_period(self, dt_code):
        print('pushing to sheet with code:', dt_code)
        db = self.db
        tablename = self.tablename
        results_list = []
        for result in db[tablename]:
            if result['dt_code'] == dt_code:
                results_list.append(result)

        df = self.lists_to_df(results_list)
        grand_total = self.grand_total(df=df)        
        df, laundry_income = self.return_and_remove_ntp(df=df, col='unit', remove_str='0')        
        df = self.group_df(df=df)
        no_unit_number = self.group_df(df=laundry_income, just_return_total=True)

        unit_index = self.get_units()
        unit_index = self.make_unit_index(unit_index)

        unit_index_df = pd.DataFrame(unit_index, columns= ['Rank', 'unit'])
        payment_list = self.merge_indexes(df, unit_index_df) 

        ntp = no_unit_number 
        return payment_list, grand_total, ntp, df

    def print_summary(self, payment_list, grand_total, ntp, df):
        print(df.head(10))
        print('grand_total:', grand_total)
        print('tenant_payments:', sum(payment_list))
        print('ntp:', ntp)
        assert sum(payment_list) + ntp == grand_total, 'the total of your tenant & non-tenant payments does not match.  you probably need to catch more types of nontenant transactions'
        print('payments balance=ok!')

    def write_ntp(self, sheet_choice, data_string):
        self.calls.update_int(self.service, self.full_sheet, data_string, sheet_choice + self.wrange_ntp, 'USER_ENTERED')
        
    def write_payment_list(self, sheet_choice, data_list):
        self.calls.simple_batch_update(self.service, self.full_sheet, sheet_choice + self.wrange_pay, data_list, "COLUMNS")

    def merge_indexes(self, df1, df2):
        merged_df = pd.merge(df1, df2, on='unit', how='outer')
        final_df = merged_df.sort_values(by='Rank', axis=0)
        final_df = final_df.fillna(0)
        payment_list = final_df['pay'].tolist()
        return payment_list
    
    def lists_to_df(self, lists):
        df = pd.DataFrame(lists)
        return df

    def return_and_remove_ntp(self, df, col=None, remove_str=None):
        ntp_item = df.loc[df[col] == remove_str]
        for item in ntp_item.index:
            df.drop(labels=item, inplace=True)
        return df, ntp_item

    '''can parameterize?'''
    def fix_date4(self, date):
        dt_object = datetime.strptime(date, '%Y-%m')
        dt_object = datetime.strftime(dt_object, '%m %Y')
        return dt_object

    def fix_date3(self, year, month):
        date = year + '-' + month
        dt_object = datetime.strptime(date, '%Y-%b')
        dt_object = datetime.strftime(dt_object, '%Y-%m')
        return dt_object

    def fix_date2(self, date):
        dt_object = datetime.strptime(date, '%m %Y')
        dt_object = datetime.strftime(dt_object, '%b %Y').lower()
        return dt_object

    def fix_date(self, date):
        dt_object = datetime.strptime(date, '%Y-%m')
        dt_object = datetime.strftime(dt_object, '%b %Y').lower()
        return dt_object

    def grand_total(self, df):
        grand_total = sum(df['pay'].tolist())
        return grand_total
    
    def group_df(self, df, just_return_total=False):
        df = df.groupby(['name', 'unit'])['pay'].sum()
        if just_return_total:
            df = df[0]
        return df

    def get_units(self):
        results_list = Config.units
        return results_list

    def make_unit_index(self, units):
        '''this func is moved to setup_month.py'''
        final_list = []
        idx_list = []
        for index, unit in enumerate(units): # indexes units from sheet
            idx_list.append(int(index))
            final_list.append(unit)

        unit_index = tuple(zip(idx_list, final_list))
        return unit_index

    def get_by_kw(self, key=None, selected=None):
        selected_items = []
        for item in selected:
            name = item['fn'].split('_')
            if key in name:
                selected_items.append(item)
        return selected_items

    def get_processed_items_list(self):
        print('\nmaking list of processed items')
        items_true = []
        for item in self.findex.db[self.findex.tablename]:
            if item['status'] == 'processed':
                items_true.append(item)
        
        return items_true 

    def read_excel_rs(self, path, verbose=False):
        df = pd.read_excel(path, header=9)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(20))

        columns = ['deposit_id', 'unit', 'name', 'date_posted', 'amount', 'date_code']
        
        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        dt_code = [datetime.strptime(item, '%m/%d/%Y') for item in date if type(item) == str]
        dt_code = [str(datetime.strftime(item, '%m')) for item in dt_code]

        zipped = zip(bde, unit, name, date, pay, dt_code)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def remove_nan_lines(self, df):
        df = df.dropna(thresh=2)
        df = df.fillna(0)
        return df

    def reset_full_sheet(self):
        titles_dict = self.show_current_sheets()
        calls = GoogleApiCalls()

        intake_ok = False
        for name, id2 in titles_dict.items():
            if name == 'intake':
                intake_ok = True
                calls.clear_sheet(self.service, self.full_sheet, f'intake!A1:ZZ100')
                break

        if intake_ok == False:
            calls.make_one_sheet(self.service, self.full_sheet, 'intake')
        
        # removal all sheets but intake
        for name, id2, in titles_dict.items():
            if name != 'intake':
                calls.del_one_sheet(self.service, self.full_sheet, id2)
       
        time.sleep(self.sleep)
        print(f'sleeping for {self.sleep} seconds')

    def summary_assertion_at_period(self, test_date):
        self.main_db.connect()
        test_date = test_date
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        active_tenant_start_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).where(Tenant.active=='True').get().sum
        assert active_tenant_start_bal_sum == 795.0

        '''charges'''
        tenant_rent_total_mar = [float(row[1]) for row in populate.get_rent_charges_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(tenant_rent_total_mar) == 15972.0

        '''payments'''
        payments_jan = [float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(payments_jan) == 16506.0

        tenant_activity_recordtype, cumsum_endbal= populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)

        cumsum_check = 2115.0
        
        assert cumsum_endbal == cumsum_check
        self.main_db.close()
    



