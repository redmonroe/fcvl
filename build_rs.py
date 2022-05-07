import json
from datetime import datetime

from auth_work import oauth
from backend import (BalanceLetter, Damages, Findexer, NTPayment, OpCash,
                     OpCashDetail, Payment, PopulateTable, StatusObject,
                     StatusRS, Subsidy, Tenant, TenantRent, Unit, db)
from config import Config, my_scopes
from file_indexer import FileIndexer
from records import record
from setup_month import MonthSheet


class BuildRS(MonthSheet):
    def __init__(self, sleep=None, full_sheet=None, path=None, mode=None, main_db=None, test_service=None):

        self.main_db = main_db
        self.full_sheet = full_sheet
        self.path = path
        try:
            self.service = oauth(my_scopes, 'sheet')
        except FileNotFoundError as e:
            print(e, 'using testing configuration for Google Api Calls')
            self.service = oauth(Config.my_scopes, 'sheet', mode='testing')
        self.create_tables_list1 = None

        self.target_bal_load_file = 'beginning_balance_2022.xlsx'
        self.file_input_path = path

    def __repr__(self):
        return f'BuildRS object path: {self.file_input_path} write sheet: {self.full_sheet} service:{self.service}'
    
    @record
    def new_auto_build(self):
        print('new_auto_build')
        # """
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
        # """

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

    '''can parameterize?'''
    # def fix_date4(self, date):
    #     dt_object = datetime.strptime(date, '%Y-%m')
    #     dt_object = datetime.strftime(dt_object, '%m %Y')
    #     return dt_object

    # def fix_date3(self, year, month):
    #     date = year + '-' + month
    #     dt_object = datetime.strptime(date, '%Y-%b')
    #     dt_object = datetime.strftime(dt_object, '%Y-%m')
    #     return dt_object

    # def fix_date2(self, date):
    #     dt_object = datetime.strptime(date, '%m %Y')
    #     dt_object = datetime.strftime(dt_object, '%b %Y').lower()
    #     return dt_object

    # def fix_date(self, date):
    #     dt_object = datetime.strptime(date, '%Y-%m')
    #     dt_object = datetime.strftime(dt_object, '%b %Y').lower()
    #     return dt_object

    # def grand_total(self, df):
    #     grand_total = sum(df['pay'].tolist())
    #     return grand_total 

    



