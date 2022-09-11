import json
from datetime import datetime

from auth_work import oauth
from backend import (ProcessingLayer, BalanceLetter, Damages, Findexer, NTPayment, OpCash, OpCashDetail, Payment, PopulateTable, StatusObject,
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
        except (FileNotFoundError, NameError) as e:
            print(e, 'using testing configuration for Google Api Calls')
            self.service = oauth(Config.my_scopes, 'sheet', mode='testing')
        self.create_tables_list1 = None
        self.target_bal_load_file = Config.beg_bal_xlsx
        self.populate = PopulateTable()
        self.ms = MonthSheet(full_sheet=self.full_sheet, path=self.path)
        self.findex = FileIndexer(path=self.path, db=self.main_db)
        self.new_files = None
        self.unfinalized_months = None
        self.ctx = None

    def __repr__(self):
        return f'BuildRS object path: {self.path} write sheet: {self.full_sheet} service:{self.service}'

    def incremental_load(self):
        status = StatusRS()
        player = ProcessingLayer()
        if self.main_db.get_tables() == []:
            """this branch is used for triggering a fresh incremental load of findexer & triggering events off of the end-state of the findexer and statusobject tables"""
            print('fresh incremental load of findexer table')
            populate = self.setup_tables()
            self.findex.build_index_runner() 
            breakpoint()
    
    def build_db_from_scratch(self, **kw):
        status = StatusRS()
        player = ProcessingLayer()
        if self.main_db.get_tables() == []:
            """this branch is used for rebuilding db from scratch"""

            print('building db from scratch')
            self.ctx = 'db empty'
            print(f'{self.ctx}')
            populate = self.setup_tables(mode='drop_and_create')
            self.findex.build_index_runner() 
            self.load_initial_tenants_and_balances()
            processed_rentr_dates_and_paths = self.iterate_over_remaining_months()
            Damages.load_damages()
            # load historical scrapes into findexer
            self.populate.transfer_opcash_to_db() # PROCESSED OPCASHES MOVED INTO DB
        else:
            if kw.get('bypass_findexer') == True:
                """this branch is used to trigger iterative build by passing in list of new added files"""
                self.ctx = 'db is not empty; iter_build; bypass findexer'
                print(f'{self.ctx}')
                populate = self.setup_tables(mode='create_only')
                self.iterate_over_remaining_months_incremental(list1=kw.get('new_files_add')[0])
                player = ProcessingLayer()
                player.set_current_date()
                most_recent_status = player.get_most_recent_status()
                all_months_ytd = player.get_all_months_ytd()

                # reconcile all available
                    # if already reconciled, don't reconcile agai
            else:
                """this branch is used to trigger iterative build of findex using new file list created here"""
                self.ctx = 'db is not empty; iter_build; do NOT bypass findexer'
                print(f'{self.ctx}')
                self.new_files, self.unfinalized_months = self.findex.incremental_filer()
                populate = self.setup_tables(mode='create_only')
                self.iterate_over_remaining_months_incremental(list1=self.new_files)
                
        all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper()

        '''this is the critical control function'''
        player.assert_reconcile_payments(month_list=all_months_ytd, ref_rec=most_recent_status)

        player.write_manual_entries_from_config()
        '''
        breakpoint()



        player.display_most_recent_status(mr_status=most_recent_status, months_ytd=all_months_ytd)
        incomplete_month_bool, paperwork_complete_months = player.is_there_mid_month(all_months_ytd, report_list)

        if kw.get('write_db') == True:
            player.find_complete_pw_months_and_iter_write(paperwork_complete_months=paperwork_complete_months)
        else:
            print('you have selected to bypass writing to RS.')
            print('if you would like to write to rent spreadsheet enable "write_db" flag')
        '''

        """only show this if I have deposits and rent roll for the month, do not show for any month after first incomplete month, there are other cases"""

        '''this method of searching for incomplete months is useless if we are already automatically putting corr amounts etc into findexer; they will be always be available there if exist'''
        # if incomplete_month_bool:
        #     choice = input(f'\nWould you like to import mid-month report from bank for {incomplete_month_bool[0]} ? Y/n (NOTE: YOU MUST CHOOSE Y IF THIS EVENT FIRES ELSE, SCRAPE WILL NOT BE MARKED AS PROCESSED AND WRITING WILL NOT BE ENABLES')
        #     if choice == 'Y':
        #         mid_month_choice = True
        #     else:
        #         mid_month_choice = False
        #         print(f'you chose not to attempt to write scrape for {incomplete_month_bool[0]}')
        #         print(f'last complete reconciliation is {most_recent_status.current_date}')

        # first_incomplete_month = incomplete_month_bool[0]
        # if mid_month_choice:
            # did_ten_pay_reconcile = player.load_scrape_and_mark_as_processed(most_recent_status=most_recent_status, target_mid_month=first_incomplete_month)

        #     if did_ten_pay_reconcile == True:
        #         do_i_write_receipts = player.make_rent_receipts(first_incomplete_month=first_incomplete_month)
        #         if do_i_write_receipts == True:
        #             player.rent_receipts_wrapper()

        #         do_i_write_bal_letters = player.make_balance_letters(first_incomplete_month=first_incomplete_month)
        #         if do_i_write_bal_letters == True:
        #             player.bal_letter_wrapper()
    
        self.main_db.close()

    def setup_tables(self, mode=None):
        populate = PopulateTable()
        self.create_tables_list1 = populate.return_tables_list()
        if self.main_db.is_closed() == True:
            self.main_db.connect()
        if mode == 'create_only':
            self.main_db.create_tables(self.create_tables_list1)
        elif mode == 'drop_and_create':
            self.main_db.drop_tables(models=self.create_tables_list1)
            self.main_db.create_tables(self.create_tables_list1)
        return populate

    def iterate_over_remaining_months_incremental(self, list1=None):
        """rent has to go first; otherwise if you have a move-in during the month there is no reference for the fk for a payment"""
        populate = PopulateTable()
        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=data[0])
                if typ == 'rent':
                    cleaned_nt_list, total_tenant_charges, cleaned_mos = populate.after_jan_load(filename=data[1], date=data[0])
                    
        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=data[0])
                if typ == 'deposits':
                    grand_total, ntp, tenant_payment_df = populate.payment_load_full(filename=data[1])

        findex = FileIndexer()
        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=data[0])
                if typ == 'scrape':
                    scrape_txn_list = findex.load_directed_scrape(path_to_scrape=data[1], target_date=data[0])
                    scrape_deposit_sum = sum([float(item['amount']) for item in scrape_txn_list if item['dep_type'] == 'deposit'])

                    assert scrape_deposit_sum == grand_total
                    target_status_object = [item for item in StatusObject().select().where(StatusObject.month==data[0])][0]
                    target_status_object.scrape_reconciled = True
                    target_status_object.save() 

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


    



