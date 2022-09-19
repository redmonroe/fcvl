from auth_work import oauth
from backend import PopulateTable, ProcessingLayer, StatusRS, Damages
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
        print('attempting incremental load')

        status = StatusRS()
        player = ProcessingLayer()

        populate = self.setup_tables(mode='create_only')
        new_files, unfinalized_months = self.findex.incremental_filer()
        # breakpoint()
        self.iterate_over_remaining_months_incremental(list1=new_files)
        Damages.load_damages()

        all_months_ytd, report_list, most_recent_status = player.write_to_statusrs_wrapper()

        """this is the critical control function"""
        player.reconcile_and_inscribe_state(month_list=all_months_ytd, ref_rec=most_recent_status)

        player.write_manual_entries_from_config()

        player.display_most_recent_status(mr_status=most_recent_status, months_ytd=all_months_ytd)

        writeable_months = player.final_check_writeable_months(month_list=all_months_ytd)


        """need to incrementally add opcash if new
        RIGHT NOW THE OPCASH IS NOT ADDED TO OPCASH TABLE"""
        breakpoint()
        # self.populate.transfer_opcash_to_db()
        # breakpoint()
