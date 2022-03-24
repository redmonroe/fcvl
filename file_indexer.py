import os
from pprint import pprint
from utils import Utils
from db_utils import DBUtils
from pdf import StructDataExtract
from checklist import Checklist
from config import Config
from pathlib import Path
from datetime import datetime
import dataset
import pandas as pd
import numpy as np
import shutil

class FileIndexer:

    def __init__(self, path=None, discard_pile=None, db=None, mode=None, table=None):
        
        self.mode = mode
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
        self.checklist = Checklist()
        self.hap_list = []
        self.rr_list = []
        self.dep_list = []
        self.deposit_and_date_list = []
        self.TEST_RR_FILE = 'rent_roll_01_2022.xls'
        self.TEST_DEP_FILE = 'deposits_01_2022.xls'
        self.GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'
        self.GENERATED_DEP_FILE = 'TEST_DEP_012022.xls' 

    def build_index_runner(self):
        if self.mode == 'testing':
            self.reset_files_for_testing()
        self.articulate_directory()
        self.sort_directory_by_extension(verbose=False)
        print('stage1:', len(self.processed_files))
        if self.mode != 'testing':
            try:
                self.rename_by_content_xls()
            except shutil.SameFileError as e:
                print(e, 'file_indexer: files likely renamed after generation proc in test')
        else:
            self.rename_by_content_xls()
        
        print('stage2:', len(self.processed_files))
        self.rename_by_content_pdf()
        print('stage3:', len(self.processed_files))
        self.build_raw_index(autodrop=True, verbose=False)
        print('stage4:', len(self.processed_files))
        self.update_index_for_processed(verbose=True)
        print('stage5:', len(self.processed_files))
        self.get_list_of_processed()
        print('stage6:', len(self.processed_files))

    def articulate_directory(self):
        for item in self.path.iterdir():
            self.directory_contents.append(item)
    
    def sort_directory_by_extension(self, verbose=None):
        for item in self.directory_contents:
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.index_dict[sub_item] = f_ext
        
        if verbose:
            pprint(self.index_dict)
        return self.index_dict

    def find_by_content(self, style, target_string=None, ):
        if style == 'rent':
            get_col = 0
            split_col = 11
            split_type = ' '
            date_split = 2

        if style == 'deposits':
            get_col = 9
            split_col = 9
            split_type = '/'
            date_split = 0

        for item in self.xls_list:
            part_list = (item.stem).split('_')
            if style in part_list:
                df = pd.read_excel(item)
                df = df.iloc[:, 0].to_list()
                if target_string in df:
                    period = self.df_date_wrapper(item, get_col=get_col, split_col=split_col, split_type=split_type, date_split=date_split)
                    self.processed_files.append((item.name, period))     

    def rename_by_content_xls(self):
        '''find rent roll by content'''
        for name, extension in self.index_dict.items():
            if extension == 'xls' or extension == 'xlsx':
                self.xls_list.append(name)

        self.find_by_content(style='rent', target_string='Affordable Rent Roll Detail/ GPR Report')

        self.find_by_content(style='deposits', target_string='BANK DEPOSIT DETAILS')

        return self.processed_files

    def rename_by_content_pdf(self):
        '''index opcashes'''
        for name, extension in self.index_dict.items():
            if extension == 'pdf':
                self.pdf_list.append(name)

        op_cash_list = []
        for item in self.pdf_list:
            op_cash_path = self.pdf.select_stmt_by_str(path=item, target_str=' XXXXXXX1891')
            if op_cash_path != None:
                op_cash_list.append(op_cash_path)

        for op_cash_stmt_path in op_cash_list:
            hap_iter_one_month, stmt_date = self.extract_deposits_by_type(op_cash_stmt_path, style='hap', target_str='QUADEL')
            rr_iter_one_month, stmt_date1 = self.extract_deposits_by_type(op_cash_stmt_path, style='rr', target_str='Incoming Wire')
            dep_iter_one_month, stmt_date2 = self.extract_deposits_by_type(op_cash_stmt_path, style='dep', target_str='Deposit')
            deposit_and_date_iter_one_month = self.extract_deposits_by_type(op_cash_stmt_path, style='dep_detail', target_str='Deposit')
            assert stmt_date == stmt_date1

            self.hap_list.append(hap_iter_one_month)
            self.rr_list.append(rr_iter_one_month)
            self.dep_list.append(dep_iter_one_month)
            self.deposit_and_date_list.append(deposit_and_date_iter_one_month)

            self.processed_files.append((op_cash_stmt_path.name, ''.join(stmt_date.split(' '))))
            self.checklist.check_opcash(date=stmt_date)

    def extract_deposits_by_type(self, path, style=None, target_str=None):
        return_list = []
        kdict = {}
        if style == 'rr':
            date, amount = self.pdf.nbofi_pdf_extract_rr(path, target_str=target_str)
        elif style == 'hap':
            date, amount = self.pdf.nbofi_pdf_extract_hap(path, target_str=target_str)
        elif style == 'dep':
            date, amount = self.pdf.nbofi_pdf_extract_deposit(path, target_str=target_str)
        elif style == 'dep_detail':
            depdet_list = self.pdf.nbofi_pdf_extract_deposit(path, style=style, target_str=target_str)
            return depdet_list
        kdict[str(date)] = [amount, path, style]
        return_list.append(kdict)
            
        return return_list, date

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

    def build_raw_index(self, autodrop=None, verbose=None):
        table = self.db[self.tablename]
        if autodrop:
            table.drop()
        self.directory_contents = []
        self.articulate_directory()
        for item in self.directory_contents:
            if item.name != 'desktop.ini':
                table.insert(dict(fn=item.name, path=str(item), status='raw'))
        if verbose:
            self.show_table()

    def update_index_for_processed(self, verbose=None):
        for item in self.db[self.tablename]:
            for proc_file in self.processed_files:
                if item['fn'] == proc_file[0]:                 
                    proc_date = self.normalize_dates(proc_file[1])
                    data = dict(id=item['id'], status='processed', period=proc_date)
                    self.db[self.tablename].update(data, ['id'])
        if verbose:
            self.show_table()

    def normalize_dates(self, raw_date=None):    
        if raw_date:
            f_date = datetime.strptime(raw_date, '%m%Y')
            f_date = f_date.strftime('%Y-%m')
            return f_date

    def get_list_of_processed(self):
        processed_check_for_test = []
        print(f'\ngetting list of processed in {self.tablename}')
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
        print(f'\n contents of {self.db}\n')
        for results in self.db[self.tablename]:
            print(results)

    def show_checklist(self, col_str=None):
        return_list = []
        check_items = [item for item in self.db[self.tablename]]
        if col_str:
            for item in check_items:
                print(item)
                return_list.append(item[col_str])
            return check_items, return_list
        return check_items
    
    def get_file_names_kw(self, dir1):

        for item in dir1.iterdir():
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.test_list.append(filename)

        return self.test_list
    
    def df_date_wrapper(self, item, get_col=None, split_col=None, split_type=None, date_split=None):
            df_date = pd.read_excel(item)
            df_date = df_date.iloc[:, get_col].to_list()
            df_date = df_date[split_col].split(split_type)
            period = df_date[date_split]
            period = period.rstrip()
            period = period.lstrip()
        
            return period

    def reset_files_for_testing(self):
        should_continue_dep = self.remove_generated_file_from_dir(path1=self.path, file1=self.GENERATED_DEP_FILE)
        should_continue_rr = self.remove_generated_file_from_dir(path1=self.path, file1=self.GENERATED_RR_FILE)
        if should_continue_dep:
            self.move_original_back_to_dir(discard_dir=self.discard_pile, target_file=self.TEST_DEP_FILE, target_dir=self.path)
        if should_continue_rr:
            self.move_original_back_to_dir(discard_dir=self.discard_pile, target_file=self.TEST_RR_FILE, target_dir=self.path)

    def remove_generated_file_from_dir(self, path1=None, file1=None):
        try:
            os.remove(os.path.join(str(path1), file1))
        except FileNotFoundError as e:
            print(e, f'{file1} NOT found in test_data_repository, make sure you are looking for the right name')
            return False
    
    def move_original_back_to_dir(self, discard_dir=None, target_file=None, target_dir=None):
        self.get_file_names_kw(discard_dir)
        for item in self.test_list:
            if item == target_file:
                try:
                    shutil.move(os.path.join(str(discard_dir), item), target_dir)
                except:
                    print('Error occurred copying file: jw')

