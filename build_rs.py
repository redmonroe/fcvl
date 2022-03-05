from datetime import datetime
from config import Config, my_scopes
from db_utils import DBUtils
from auth_work import oauth
from file_indexer import FileIndexer
from setup_month import MonthSheet

class BuildRS(MonthSheet):
    def __init__(self, full_sheet=None, path=None, mode=None, test_service=None):
        if mode == 'testing':
            self.mode = 'testing'
            self.findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, table='findex')
            self.mformat = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH)
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
    def automatic_build(self):
        '''this is the hook into the program for the checklist routine'''
        
        '''display all'''
        # for item in self.findex.db['findex']:
        #     print(item)

        items_true = self.get_processed_items_list()
        rentrolls_true = self.get_by_kw(key='RENTROLL', selected=items_true)
        '''rentroll and monthly formatting'''
        '''target is jan 2022'''
        for item in rentrolls_true:
            dt_object = datetime.strptime(item['period'], '%Y-%m')
            dt_object = datetime.strftime(dt_object, '%b %Y').lower()
            '''trigger formatting of dt_object named sheet'''
            self.mformat.export_month_format(dt_object)
            self.mformat.push_one_to_intake(input_file_path=item['path'])
            print(dt_object, item['path'])

        # print(items_true)
        return items_true

    def get_by_kw(self, key=None, selected=None):
        selected_items = []
        for item in selected:
            name = item['fn'].split('_')
            if key in name:
                selected_items.append(item)
        return selected_items

    def get_processed_items_list(self):
        check_tables = DBUtils.get_tables(self, self.findex.db)
        items_true = []
        for item in self.findex.db['findex']:
            if item['status'] == 'processed':
                items_true.append(item)
        
        return items_true 

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
    buildrs.automatic_build()
