import json
from datetime import datetime

from auth_work import oauth
from backend import (BalanceLetter, Damages, Findexer, NTPayment, OpCash,
                     OpCashDetail, Payment, PopulateTable, StatusObject,
                     StatusRS, Subsidy, Tenant, TenantRent, Unit, db)
from config import Config
from file_indexer import FileIndexer
from records import record
from setup_month import MonthSheet


class BuildRS(MonthSheet):
    def __init__(self, sleep=None, full_sheet=None, path=None, mode=None, main_db=None, test_service=None):

        self.main_db = main_db
        self.full_sheet = full_sheet
        self.path = path
        try:
            self.service = oauth(Config.my_scopes, 'sheet')
        except FileNotFoundError as e:
            print(e, 'using testing configuration for Google Api Calls')
            self.service = oauth(Config.my_scopes, 'sheet', mode='testing')
        self.create_tables_list1 = None

        self.target_bal_load_file = Config.beg_bal_xlsx

    def __repr__(self):
        return f'BuildRS object path: {self.path} write sheet: {self.full_sheet} service:{self.service}'

    def determine_ctx(self, flag=None):
        populate = PopulateTable()
        if flag == 'reset':
            self.create_tables_list1 = populate.return_tables_list()
            self.main_db.drop_tables(models=self.create_tables_list1)
        elif flag == 'run':
            if self.main_db.get_tables() == []:
                self.new_auto_build()
            # determine if unprocessed files in folder
            else:
                self.iter_build()  # else explicitly state nothing new to add
            # do tables exist? if not run new_auto_build
            pass

    def iter_build(self):
        print('iter_build')
        findex = FileIndexer(path=self.path, db=self.main_db)
        findex.iter_build_runner() # this is not complete
    
    @record
    def new_auto_build(self):
        print('new_auto_build')
        populate = PopulateTable()
        self.create_tables_list1 = populate.return_tables_list()
        findex = FileIndexer(path=self.path, db=self.main_db)
        findex.drop_findex_table() 
        if self.main_db.is_closed() == True:
            self.main_db.connect()
        self.main_db.drop_tables(models=self.create_tables_list1)
        self.main_db.create_tables(self.create_tables_list1)
        findex.build_index_runner() # this is a findex method
        self.load_initial_tenants_and_balances()
        processed_rentr_dates_and_paths = self.iterate_over_remaining_months()
        
        Damages.load_damages()

        populate.transfer_opcash_to_db() # PROCESSED OPCASHES MOVED INTO DB

        status = StatusRS()
        status.set_current_date()
        '''from show we can do rs_write, rent_sheets, balance_letters'''
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

    



