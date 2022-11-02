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
    def __init__(self, sleep=None, full_sheet=None, path=None, mode=None, test_service=None, pytest=None):

        self.main_db = db # connects backend.db to Config
        if mode == 'testing':
            db_path = Config.TEST_DB.database
            self.main_db.init(db_path)
        else:
            db_path = Config.PROD_DB.database
            self.main_db.init(db_path)

        self.full_sheet = full_sheet
        self.path = path
        self.pytest = False
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
        return f'{self.__class__.__name__} object path: {self.path} write sheet: {self.full_sheet} service:{self.service}'
    
    def build_db_from_scratch(self, **kw):
        status = StatusRS()
        player = ProcessingLayer(service=self.service, full_sheet=self.full_sheet, ms=self.ms)
        print('building db from scratch')
        self.ctx = 'db empty'
        print(f'{self.ctx}')
        populate = self.setup_tables(mode='drop_and_create')
        self.findex.build_index_runner() 
        self.load_initial_tenants_and_balances()
        processed_rentr_dates_and_paths = self.iterate_over_remaining_months()
        Damages.load_damages()
        self.populate.transfer_opcash_to_db() # PROCESSED OPCASHES MOVED INTO DB

        '''BUILD ADDRESSES HERE; MOVE IT OUT OF LETTERS'''

        all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper()

        """this is the critical control function"""
        player.reconcile_and_inscribe_state(month_list=all_months_ytd, ref_rec=most_recent_status)

        player.write_manual_entries_from_config()

        player.display_most_recent_status(mr_status=most_recent_status, months_ytd=all_months_ytd)

        writeable_months = player.final_check_writeable_months(month_list=all_months_ytd)

        if kw.get('write') == True:
            player.find_complete_pw_months_and_iter_write( writeable_months=writeable_months)
        else:
            print('you have selected to bypass writing to RS(self.write=False if coming from tests).')
            print('if you would like to write to rent spreadsheet enable "write" flag')
    
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

        '''
        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=data[0])
                if typ == 'op':
                    print('process op_cash')
                    # breakpoint(c
                    # )
                    # grand_total, ntp, tenant_payment_df = populate.payment_load_full(filename=data[1])
        '''

        findex = FileIndexer()
        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=data[0])
                if typ == 'scrape':
                    scrape_txn_list = findex.load_directed_scrape(path_to_scrape=data[1], target_date=data[0])
                    scrape_deposit_sum = sum([float(item['amount']) for item in scrape_txn_list if item['dep_type'] == 'deposit'])

                    """this really needs to be combined"""
                    
                    assert scrape_deposit_sum == grand_total
                    # except AssertionError as e:
                        # print(e)
                        # breakpoint()
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
        print('loading initial tenant balances')
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


    



