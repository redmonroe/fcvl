import time

from auth_work import oauth
from backend import (Damages, Findexer, InitLoad, AfterInitLoad, PopulateTable,
                     ProcessingLayer, db)
from config import Config
from file_indexer import FileIndexer
from reconciler import Reconciler
from setup_month import MonthSheet


class BuildRS(MonthSheet):
    def __init__(self, sleep=None, full_sheet=None, path=None,
                 mode=None, test_service=None, pytest=None, **kw):

        self.main_db = db  # connects backend.db to Config
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
        self.ms = MonthSheet(full_sheet=self.full_sheet, path=self.path)
        self.findex = FileIndexer(path=self.path, db=self.main_db)
        self.populate = self._setup_tables(mode='drop_and_create')

    def __repr__(self):
        return f'{self.__class__.__name__} | {self.path} | {self.full_sheet}'

    def build_db_from_scratch(self, **kw):
        print('building db from scratch')
        player = ProcessingLayer(service=self.service,
                                 full_sheet=self.full_sheet, ms=self.ms)
        self.start = time.time()
        self.findexer_time = time.time()
        self.findex.build_index_runner()
        self.rr_list = [(item.fn, item.period, item.path)
                        for item in Findexer().select().
                        where(Findexer.doc_type == 'rent').
                        where(Findexer.status == 'processed').
                        where(Findexer.period != '2022-01').
                        namedtuples()]
        self.file_list = [(item.fn, item.period, item.path)
                          for item in Findexer().select().
                          where(Findexer.doc_type == 'deposits').
                          where(Findexer.status == 'processed').
                          namedtuples()]
        self.proc_rentrolls = [(item[1], item[2])
                               for item in self.rr_list]
        self.proc_dates_and_paths = [(item[1], item[2])
                                     for item in self.file_list]
        self.findexer_time = time.time() - self.findexer_time

        print('loading initial tenant balances')
        init_load_time = time.time()
        initial = InitLoad(
            path=self.path,
            custom_load_file=self.target_bal_load_file)

        (tenant_rows,
         tot_ten_ch,
         ex_moveouts,
         init_ten,
         units,
         rents,
         subsidies,
         contract_rents) = initial.return_init_results()
        
        init_load_time = time.time() - init_load_time

        # TODO: can't we just pass a list of months
        after_init_load_time = time.time()
        after_initial = AfterInitLoad(rentrolls=self.proc_rentrolls,
                                      deposits=self.proc_dates_and_paths)
        
        after_init_load_time = time.time() - after_init_load_time
        
        damages_txn_status_rstime1 = time.time()
        
        Damages.load_damages()

        self.populate.transfer_opcash_from_findex_to_opcash_and_detail()

        '''BUILD ADDRESSES HERE; MOVE IT OUT OF LETTERS'''

        if kw.get('last_range_month') is not None:
            all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper(last_range_month=kw['last_range_month'])            
        else:
            all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper()
        
        damages_txn_status_rstime1 = time.time() - damages_txn_status_rstime1 

        mentry_time = time.time()
        player.write_manual_entries_from_config()
        mentry_time = time.time() - mentry_time
        
        reconcile_time = time.time()
        """this is the critical control function"""
        player.reconcile_and_inscribe_state(
            month_list=all_months_ytd,
            ref_rec=most_recent_status,
            source='build')
        reconcile_time = time.time() - reconcile_time

        player.display_most_recent_status(
            mr_status=most_recent_status, months_ytd=all_months_ytd)

        writeable_months = player.final_check_writeable_months(
            month_list=all_months_ytd)

        if kw.get('write') is True:
            player.find_complete_pw_months_and_iter_write(
                writeable_months=writeable_months)
        else:
            print(
                'you have selected to bypass writing to',
                'RS(self.write=False if coming from tests).')
            print('if you would like to write to rent',
                  'spreadsheet enable "write" flag')

        self.main_db.close()
        print(f'full time: {time.time() - self.start}')
        print(f'findexer: {self.findexer_time}')
        print('init:', init_load_time)
        print('after_init:', after_init_load_time)
        print('misc:', damages_txn_status_rstime1)
        print('manual entry:', mentry_time)
        print('reconcile:', reconcile_time)

    def _setup_tables(self, mode=None):
        populate = PopulateTable()
        self.create_tables_list1 = populate.return_tables_list()
        if self.main_db.is_closed() is True:
            self.main_db.connect()
        if mode == 'create_only':
            self.main_db.create_tables(self.create_tables_list1)
        elif mode == 'drop_and_create':
            self.main_db.drop_tables(models=self.create_tables_list1)
            self.main_db.create_tables(self.create_tables_list1)
        return populate

    def iterate_over_remaining_months_incremental(self, list1=None):
        """rent has to go first;
        otherwise if you have a move-in during t
        he month there is no reference for the fk for a payment"""
        populate = PopulateTable()

        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(
                    date_str=data[0])
                if typ == 'rent':
                    cleaned_nt_list, total_tenant_charges, cleaned_mos = populate.after_jan_load(
                        filename=data[1], date=data[0])

        for item in list1:
            for typ, data in item.items():
                first_dt, last_dt = populate.make_first_and_last_dates(
                    date_str=data[0])
                if typ == 'deposits':
                    grand_total, ntp, tenant_payment_df = populate.payment_load_full(
                        filename=data[1])

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
                first_dt, last_dt = populate.make_first_and_last_dates(
                    date_str=data[0])
                if typ == 'scrape':
                    scrape_txn_list = findex.load_directed_scrape(
                        path_to_scrape=data[1], target_date=data[0])
                    scrape_deposit_sum = sum(
                        [float(item['amount']) for item in scrape_txn_list if item['dep_type'] == 'deposit'])

                    """this really needs to be combined"""

                    Reconciler.iter_build_assert_scrape_total_match_deposits(
                        scrape_deposit_sum=scrape_deposit_sum, grand_total_from_deposits=grand_total, period=data[0], genus='reconcile scrape to deposits in iterative build')

                    month = data[0]

                    return True, month


