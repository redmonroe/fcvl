import os
from utils import Utils
from config import Config
from pathlib import Path
import dataset
import pandas as pd
import numpy as np
import shutil

# list all files
# divide by .ext (pdf, xls, xlsx)

# GET TO MOST RECENT RENT ROLL 

class FileIndexer:

    def __init__(self, path=None, discard_pile=None):
        self.path = path
        self.discard_pile = discard_pile
        self.directory_contents = []
        self.index_dict = {}
        self.test_list = []
        self.xls_list = []


    def build_index_runner(self):
        self.articulate_directory()
        self.sort_directory_by_extension()
        self.rename_by_content_xls_rroll()


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

    def rename_by_content_xls_rroll(self):
        '''find rent roll by content'''
        for name, extension in self.index_dict.items():
            if extension == 'xls':
                self.xls_list.append(name)


        # Affordable Rent Roll Detail/ GPR Report
        # 'Vistula of Indiana Inc - Falls Creek Village I'
        # 'Page 1 of 2'
        for item in self.xls_list:
            df = pd.read_excel(item)
            df = df.iloc[:, 0].to_list()
            if 'Affordable Rent Roll Detail/ GPR Report' in df:
                file_path = item
                df_date = pd.read_excel(file_path)
                df_date = df_date.iloc[11].to_list()
                df_date = df_date[0].split(' ')
                period = df_date[2]
                print(period)    


                new_file = os.path.join(Config.TEST_RS_PATH, f'TEST_RENTROLL_{period}.xls')
                shutil.copy2(item, new_file)
                shutil.move(str(item), Config.TEST_MOVE_PATH)
        '''
    
        for item in self.xls_list:
            df = pd.read_excel(item)
            df = df.iloc[11].to_list()
            df = df[0].split(' ')
            print(df[2])    
        '''




    
    
    def to_xlsx(self):
        import os
        import datetime
        target = 'TEST_deposits_01_2022.xls'
        target_file = os.path.join(self.path, target)
        # print(target_file)


        # SO GET MOST RECENT FILE, MAKE CHANGES BUT PRESERVE TIMING FOR EVENTUAL DISPLAY

        target_dir = os.listdir(self.path)
        date_dict = {}
        for item in target_dir:
            td = os.path.join(self.path, item)
            file_stat = os.stat(td)
            date_time = datetime. datetime.fromtimestamp(file_stat.st_ctime)
            print(item, date_time)

if __name__ == '__main__':
    findex = FileIndexer(path=Config.TEST_RS_PATH)
    findex.build_index_runner()