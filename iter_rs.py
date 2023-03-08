import glob
import os
from pathlib import Path

from auth_work import oauth
from backend import (Damages, PopulateTable, ProcessingLayer, StatusObject,
                     StatusRS, db)
from build_rs import BuildRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet


class IterRS(BuildRS):

    def __init__(self, staging_layer=None, path=None, mode=None, test_service=None, pytest=None):

        self.main_db = db # connects backend.db to Config
        if mode == 'testing':
            db_path = Config.TEST_DB.database
            self.main_db.init(db_path)
        else:
            db_path = Config.PROD_DB.database
            self.main_db.init(db_path)

        self.full_sheet = staging_layer
        self.path = path
        try:
            self.service = oauth(Config.my_scopes, 'sheet')
        except (FileNotFoundError, NameError) as e:
            print(e, 'using testing configuration for Google Api Calls')
            self.service = oauth(Config.my_scopes, 'sheet', mode='testing')
        self.create_tables_list1 = None
        self.target_bal_load_file = Config.beg_bal_xlsx
        self.populate = PopulateTable()
        self.ms = MonthSheet(staging_layer=self.full_sheet, path=self.path)
        self.findex = FileIndexer(path=self.path, db=self.main_db)
        self.player = ProcessingLayer(service=self.service, full_sheet=self.full_sheet, ms=self.ms)
        self.pytest = pytest

    def __repr__(self):
        return f'{self.__class__.__name__} object path: {self.path} write sheet: {self.full_sheet} service:{self.service}'

    def is_new_file_available(self, genus=None, filename=None):
        if genus == 'scrape':            
            path = os.path.join(self.path, f'{filename[0]}*{filename[1]}')
            dir_cont = max([(item, os.path.getctime(item)) for item in glob.glob(path)])
            dir_cont = [dir_cont[0]]
            dir_cont = [(Path(item)) for item in dir_cont]
            dir_cont = [(item, item.name) for item in dir_cont]   
            filename = dir_cont[0][1]                       
        else:
            dir_cont = [(item, item.name) for item in self.path.iterdir() 
                        if item.name not in self.findex.excluded_file_names]
        
        try:
            record = [{genus: (True, entry[0])} for entry in dir_cont if entry[1] in filename][0]
            return record
        except IndexError:
            return {genus: (False, 'is_file_available(): file not scraped and not found in path')}
   
    def dry_run(self, *args, **kwargs):
        return self.findex.incremental_filer_sub_1_for_dry_run(currently_availables=kwargs['currently_availables'], 
                                                               target_month=kwargs['target_month'])

       
