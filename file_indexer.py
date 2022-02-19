import os
from utils import Utils
from db_utils import DBUtils
from config import Config
from pathlib import Path
import dataset
import pandas as pd
import numpy as np
import shutil

class FileIndexer:

    def __init__(self, path=None, discard_pile=None, db=None):
        self.path = path
        self.discard_pile = discard_pile
        self.db = db
        self.table_name = 'findex'
        self.directory_contents = []
        self.index_dict = {}
        self.test_list = []
        self.xls_list = []

    def build_index_runner(self):
        self.articulate_directory()
        self.sort_directory_by_extension()
        self.rename_by_content_xls()

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

                new_file = os.path.join(self.path, filename_sub + period + filename_post)
                shutil.copy2(item, new_file)
                shutil.move(str(item), Config.TEST_MOVE_PATH)
                print('ok')

                self.xls_list.remove(item)

    def rename_by_content_xls(self):
        '''find rent roll by content'''
        ## this can be moved out to own function ie make_xls_list, make_pdf_list
        for name, extension in self.index_dict.items():
            if extension == 'xls':
                self.xls_list.append(name)

        self.find_by_content(style='rr', target_string='Affordable Rent Roll Detail/ GPR Report')

        self.find_by_content(style='dep', target_string='BANK DEPOSIT DETAILS')

    def rename_by_content_xls_deposits(self):
        '''find deposits by content'''
        pass
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
        # why 
        db = self.db
        tablename = self.table_name
        
        table = db[tablename]
        table.drop()
        self.articulate_directory()
        for item in self.directory_contents:
            print(item.name)
            table.insert(dict(fn=item.name, path=str(item), status='raw'))

        for item in db[tablename]:
            print(item)

        
    def get_tables(self):
        tables = DBUtils.get_tables(self, self.db)
        print(tables)

    def delete_table(self):
        db = Config
        DBUtils.delete_table(self, self.db)

if __name__ == '__main__':
    findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db)
    findex.build_index()
    findex.get_tables()