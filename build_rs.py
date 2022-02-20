from config import Config, my_scopes
from auth_work import oauth
from file_indexer import FileIndexer
from setup_month import MonthSheet

class BuildRS(MonthSheet):
    def __init__(self, full_sheet, path, mode=None, test_service=None):
        if mode == 'testing':
            self.mode = 'testing'
            self.findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, table='findex')
            self.service = test_service
        else:
            self.service = oauth(my_scopes, 'sheet')

        self.full_sheet = full_sheet
        self.file_input_path = path
        self.user_text = f'Options:\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 TO VIEW ITEMS IN {self.file_input_path} \n PRESS 3 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet in {self.file_input_path} (xlsx) \n PRESS 4 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'

    def buildrs_control(self):
        # FIRST SELECT A MONTH:

            # return applicable docs, compare to necessary docs

            # do I have a rent sheet?
            # do I have the rent roll?
            # do I have the deposits?
            # do I have bank statements? 
            # can I forestall reconciliation issues at this stage? 






        if self.user_choice == 1:
            ms = MonthSheet(self.full_sheet, self.file_input_path)
            ms.show_current_sheets()
        elif self.user_choice == 2:
            pass
            # self.index_wrapper()

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def index_wrapper(self):
        # self.findex.do_index()
        # self.findex.normalize_dates()
        self.findex.show_table(table=self.findex.tablename)

    def build_rs_runner(self):
        self.index_wrapper()


if __name__ == '__main__':
    buildrs = BuildRS(mode='testing')
    buildrs.index_wrapper()
