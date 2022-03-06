from datetime import datetime
from config import Config, my_scopes
from db_utils import DBUtils
from auth_work import oauth
from file_indexer import FileIndexer
from setup_month import MonthSheet
import dataset
import pandas as pd


class BuildRS(MonthSheet):
    def __init__(self, full_sheet=None, path=None, mode=None, test_service=None):
        if mode == 'testing':
            self.mode = 'testing'
            self.full_sheet=Config.TEST_RS
            self.findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, table='findex')
            self.service = test_service
            self.mformat = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing', test_service=self.service)
        else:
            self.service = oauth(my_scopes, 'sheet')

        self.file_input_path = path
        self.user_text = f'Options:\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 TO VIEW ITEMS IN {self.file_input_path} \n PRESS 3 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet in {self.file_input_path} (xlsx) \n PRESS 4 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'
        self.df = None

    def buildrs_control(self):
        pass
        # FIRST SELECT A MONTH:

            # return applicable docs, compare to necessary docs

            # do I have a rent sheet?
            # do I have the rent roll?
            # do I have the deposits?
            # do I have bank statements? 
            # can I forestall reconciliation issues at this stage? 

    def automatic_build(self, key=None):
        '''this is the hook into the program for the checklist routine'''
        
        '''display all'''
        # for item in self.findex.db['findex']:
        #     print(item)
        items_true = self.get_processed_items_list()
        if key == 'ALL':
            ## THIS DOES NOT WORK YET
            rentrolls_true = self.get_by_kw(key='RENTROLL', selected=items_true)
            deposits_true = self.get_by_kw(key='DEP', selected=items_true)
        else:
            list_true = self.get_by_kw(key=key, selected=items_true)

        '''rentroll and monthly formatting'''
        if key == 'RENTROLL':
            for item in list_true:
                dt_object = datetime.strptime(item['period'], '%Y-%m')
                dt_object = datetime.strftime(dt_object, '%b %Y').lower()
                '''trigger formatting of dt_object named sheet'''
                self.mformat.export_month_format(dt_object)
                self.mformat.push_one_to_intake(input_file_path=item['path'])
                self.month_write_col(dt_object)

        '''deposits push'''
        if key == 'DEP':
            for item in list_true:
                dt_object = datetime.strptime(item['period'], '%Y-%m')
                dt_object = datetime.strftime(dt_object, '%b %Y').lower()
                df = self.read_excel(path=item['path'])
                print(df.head(5))
                self.to_sql(df=df)
                # print(dt_object, item['path'])

        return items_true

    def read_excel(self, path, verbose=False):
        df = pd.read_excel(path, header=9)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(20))

        columns = ['deposit_id', 'unit', 'name', 'date_posted', 'amount', 'pay_float', 'date_code']

        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        pay_float = df['Amount'].tolist()
        dt_code = [str(item)[0:2]for item in date]

        zipped = zip(bde, unit, name, date, pay, pay_float, dt_code)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def to_sql(self, df):
        pass

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
    test_service = oauth(my_scopes, 'sheet')
    buildrs = BuildRS(mode='testing', test_service=test_service)
    buildrs.automatic_build(key='DEP')
