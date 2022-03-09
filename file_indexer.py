import os
from utils import Utils
from db_utils import DBUtils
from pdf import StructDataExtract
from config import Config
from pathlib import Path
from datetime import datetime
import dataset
import pandas as pd
import numpy as np
import shutil

class FileIndexer:

    def __init__(self, path=None, discard_pile=None, db=None, table=None):
        self.path = path
        self.discard_pile = discard_pile
        self.db = db
        self.tablename = table 
        self.directory_contents = []
        self.index_dict = {}
        self.test_list = []
        self.xls_list = []
        self.pdf_list = []
        self.check_tables = None
        self.processed_files = []
        self.pdf = StructDataExtract()

    def build_index_runner(self):
        self.articulate_directory()
        self.sort_directory_by_extension()
        try:
            self.rename_by_content_xls()
        except shutil.SameFileError as e:
            print(e, 'file_indexer: files likely renamed after generation proc in test')
        self.rename_by_content_pdf()

    def articulate_directory(self):
        for item in self.path.iterdir():
            self.directory_contents.append(item)
    
    def sort_directory_by_extension(self):

        for item in self.directory_contents:
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.index_dict[sub_item] = f_ext

    def get_file_names_kw(self, dir1):

        for item in dir1.iterdir():
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.test_list.append(filename)
    
    def df_date_wrapper(self, item, get_col=None, split_col=None, split_type=None, date_split=None):
            df_date = pd.read_excel(item)
            df_date = df_date.iloc[:, get_col].to_list()
            df_date = df_date[split_col].split(split_type)
            period = df_date[date_split]
            period = period.rstrip()
            period = period.lstrip()
        
            return period

    def find_by_content(self, style, target_string=None, ):
        if style == 'rr':
            filename_sub = 'TEST_RENTROLL_'
            filename_post = '.xls'
            get_col = 0
            split_col = 11
            split_type = ' '
            date_split = 2

        if style == 'dep':
            filename_sub = 'TEST_DEP_'
            filename_post = '.xls'
            get_col = 9
            split_col = 9
            split_type = '/'
            date_split = 0

        for item in self.xls_list:
            df = pd.read_excel(item)
            df = df.iloc[:, 0].to_list()
            if target_string in df:
                period = self.df_date_wrapper(item, get_col=get_col, split_col=split_col, split_type=split_type, date_split=date_split)

                filename = filename_sub + period + filename_post
                new_file = os.path.join(self.path, filename)
                shutil.copy2(item, new_file)
                shutil.move(str(item), Config.TEST_MOVE_PATH)
                print('ok')
                self.processed_files.append((filename, period))
                self.xls_list.remove(item)

    def rename_by_content_xls(self):
        '''find rent roll by content'''
        ## this can be moved out to own function ie make_xls_list, make_pdf_list
        for name, extension in self.index_dict.items():
            if extension == 'xls':
                self.xls_list.append(name)

        # self.find_by_content(style='rr', target_string='Affordable Rent Roll Detail/ GPR Report')

        # self.find_by_content(style='dep', target_string='BANK DEPOSIT DETAILS')

    def rename_by_content_pdf(self):
        '''index opcashes'''
        for name, extension in self.index_dict.items():
            if extension == 'pdf':
                self.pdf_list.append(name)
    
        # paths to op cash, op cash date, op cash extract deposits, op cash mark as processed, write a test
        op_cash_list = []
        for item in self.pdf_list:
            op_cash_path = self.pdf.select_stmt_by_str(path=item, target_str=' XXXXXXX1891')
            if op_cash_path != None:
                op_cash_list.append(op_cash_path)

        hap_list = self.extract_deposits_by_type(op_cash_list, style='hap', target_str='QUADEL')
        rr_list = self.extract_deposits_by_type(op_cash_list, style='rr', target_str='Incoming Wire')
        dep_list = self.extract_deposits_by_type(op_cash_list, style='dep', target_str='Deposit')
        print(hap_list)
        print(rr_list)
        print(dep_list)
        print(self.pdf.deposits_list)


    def extract_deposits_by_type(self, stmt_list, style=None, target_str=None):
        return_list = []
        for path in stmt_list:
            kdict = {}
            if style == 'rr':
                date, amount = self.pdf.nbofi_pdf_extract_rr(path, target_str=target_str)
            elif style == 'hap':
                date, amount = self.pdf.nbofi_pdf_extract_hap(path, target_str=target_str)
            elif style == 'dep':
                date, amount = self.pdf.nbofi_pdf_extract_deposit(path, target_str=target_str)
            kdict[str(date)] = [amount, path, style]
            return_list.append(kdict)
            
        return return_list 





    def get_more_metadata(self):
        target_file = os.path.join(self.path, target)
        # SO GET MOST RECENT FILE, MAKE CHANGES BUT PRESERVE TIMING FOR EVENTUAL DISPLAY
        target_dir = os.listdir(self.path)
        date_dict = {}
        for item in target_dir:
            td = os.path.join(self.path, item)
            file_stat = os.stat(td)
            date_time = datetime. datetime.fromtimestamp(file_stat.st_ctime)
            print(item, date_time)

    def build_index(self):
        db = self.db
        tablename = self.tablename
        
        table = db[tablename]
        table.drop()
        self.directory_contents = []
        self.articulate_directory()
        for item in self.directory_contents:
            if item.name != 'desktop.ini':
                table.insert(dict(fn=item.name, path=str(item), status='raw'))

    def update_index_for_processed(self):
        for item in self.db[self.tablename]:
            for proc_file in self.processed_files:
                if item['fn'] == proc_file[0]:                 
                    proc_date = self.normalize_dates(proc_file[1])
                    data = dict(id=item['id'], status='processed', period=proc_date)
                    self.db[self.tablename].update(data, ['id'])

    def normalize_dates(self, raw_date):    
        if raw_date:
            f_date = datetime.strptime(raw_date, '%m%Y')
            f_date = f_date.strftime('%Y-%m')
            return f_date

    def do_index(self):
        processed_check_for_test = []
        print('do_index')
        for item in self.db[self.tablename]:
            if item['status'] == 'processed':
                print(item)
                processed_check_for_test.append(item['fn'])
                processed_check_for_test.append(item['period'])

        # so now we can trigger off of processed
        return processed_check_for_test

    def drop_tables(self):
        db = self.db    
        tablename = self.tablename
        
        table = db[tablename]
        table.drop()
        
    def get_tables(self):
        self.check_tables = DBUtils.get_tables(self, self.db)
        print(self.check_tables)
        print(len(self.db[self.tablename]))

    def delete_table(self):
        db = Config
        DBUtils.delete_table(self, self.db)

    def show_table(self, table=None):
        db = self.db
        for results in db[table]:
            print(results)

if __name__ == '__main__':
    findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, table='findex')
    findex.build_index_runner()
    # findex.build_index()
    # findex.update_index_for_processed()
    # findex.do_index()

    # findex.show_table(table=findex.tablename)