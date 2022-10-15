from auth_work import oauth
from backend import PopulateTable, ProcessingLayer, StatusRS, Damages, StatusObject, db
from build_rs import BuildRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet


class IterRS(BuildRS):

    def __init__(self, full_sheet=None, path=None, mode=None, test_service=None):

        self.main_db = db # connects backend.db to Config
        if mode == 'testing':
            db_path = Config.TEST_DB.database
            self.main_db.init(db_path)
        else:
            db_path = Config.PROD_DB.database
            self.main_db.init(db_path)

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
        print('attempting incremental load')

        """
        NEXT UP: 
            - WHAT IF SCRAPE IS BEFORE OPCASH
            - WHAT IF DEPOSITS ONLY?
            - WHAT IF RR ONLY?
            - WHAT IF OPCASH ONLY?
            - DAMAGES, MANUAL ENTRY (but I think this is working ok?)
        
        
        """

        status = StatusRS()
        player = ProcessingLayer(service=self.service, full_sheet=self.full_sheet, ms=self.ms)

        populate = self.setup_tables(mode='create_only')
        new_files, unfinalized_months, final_not_written = self.findex.incremental_filer()

        if final_not_written != []:
            print('writing remaining months to rs & marking to statusobject table')
            player.find_complete_pw_months_and_iter_write( writeable_months=final_not_written)

        """we need both new files and SOME unfinalized months to do anything"""
        if new_files != [] and unfinalized_months != []:   
            self.iterate_over_remaining_months_incremental(list1=new_files)
            Damages.load_damages()

            self.populate.transfer_opcash_to_db()

            all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper()

            """this is the critical control function"""
            player.reconcile_and_inscribe_state(month_list=all_months_ytd, ref_rec=most_recent_status, from_iter=1)

            player.write_manual_entries_from_config()

            player.display_most_recent_status(mr_status=most_recent_status, months_ytd=all_months_ytd)

            writeable_months = player.final_check_writeable_months(month_list=all_months_ytd)
            
            player.find_complete_pw_months_and_iter_write( writeable_months=writeable_months)
    
            """need to incrementally add opcash if new
            RIGHT NOW THE OPCASH IS NOT ADDED TO OPCASH TABLE""" 
        else:
            print('there are no new files, but some months are still unfinalized')
            print('exiting iter_build')