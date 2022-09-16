from auth_work import oauth
from backend import PopulateTable, ProcessingLayer, StatusRS
from build_rs import BuildRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet


class IterRS(BuildRS):

    def __init__(self, main_db=None, full_sheet=None, path=None, mode=None, test_service=None):

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

    def incremental_load(self):

        # loading beginning balance then stop

        status = StatusRS()
        player = ProcessingLayer()
        
        populate = self.setup_tables(mode='drop_and_create')
        self.findex.build_index_runner() 
        breakpoint()
        self.load_initial_tenants_and_balances()
        processed_rentr_dates_and_paths = self.iterate_over_remaining_months()
        Damages.load_damages()
        # load historical scrapes into findexer
        self.populate.transfer_opcash_to_db() # PROCESSED OPCASHES MOVED INTO DB
     
