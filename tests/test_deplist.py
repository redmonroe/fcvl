import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
import inspect
from pathlib import Path, PosixPath

from auth_work import oauth
from backend import Findexer, PopulateTable, StatusObject, StatusRS, QueryHC
from config import Config
from figuration import Figuration
from file_indexer import FileIndexer
from iter_rs import IterRS
from setup_month import MonthSheet

'''
print(f'ERROR DETECTED in {inspect.currentframe().f_code.co_name} for {month}')
print('IT IS VERY LIKELY THAT A STATUSOBJECT HAS FAILED TO RECONCILE')
# breakpoint()
'''

class TestDeplist:

    base_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/first_three_months_op_only'

    scrape_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/april_scrape_only'

    @pytest.fixture
    def set_write_mode(self, write):
        if write == 'False':
            yield False
        elif write == 'True':
            yield True
        else:            
            breakpoint()

    @pytest.fixture
    def populate(self):
        populate = PopulateTable()
        return populate

    @pytest.fixture
    def query(self):
        query = QueryHC()
        return query 

    def return_op_config(self, type1=None, configured_path=None):
        '''this is just to set up first three months for testing deplist writing from scrape'''
        if type1 == 'iter':
            figure = Figuration(path=Path(configured_path), pytest=True)
        else: 
            figure = Figuration(method='build', path=Path(configured_path), pytest=True)
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        return path, full_sheet, build, service, ms, findexer

    def return_scrape_config(self, type1=None, configured_path=None):
        if type1 == 'iter':
            figure = Figuration(path=Path(configured_path), pytest=True)
        else: 
            figure = Figuration(method='build', path=Path(configured_path), pytest=True)
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        return path, full_sheet, build, service, ms, findexer

    def test_db_reset1(self, populate):
        path, full_sheet, build, service, ms, findexer = self.return_op_config(configured_path=self.base_path)  
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset1(self):
        path, full_sheet, build, service, ms, findexer = self.return_op_config(configured_path=self.base_path)  
        ms.reset_spreadsheet()

    def test_load_init_db_state1_VIA_BUILD(self, set_write_mode):
        print('\n START OP ONLY JAN FEB MAR\n')
        path, full_sheet, build, service, ms, findexer = self.return_op_config(configured_path=self.base_path)   # uses BuildRS not IterRS
        build.build_db_from_scratch(write=set_write_mode)  

    def test_on_opcash_only_findexer(self, query):
        print(f'\n running {inspect.currentframe().f_code.co_name.capitalize()}\n')        
        indexed_files = query.get_all_by_rows_by_argument(model1=Findexer)
        assert len(indexed_files) == 9
        # breakpoint()

    def test_load_april_from_scrape(self, set_write_mode):
        print('\n START SCRAPE ONLY for April\n')
        path, full_sheet, iterb, service, ms, findexer = self.return_op_config(type1='iter', configured_path=self.scrape_path)   # return IterRS
        # breakpoint()
        iterb.incremental_load(write=set_write_mode)


        

    '''

    def test_db_reset1(self, populate):
        path, full_sheet, build, service, ms, findexer = self.return_op_config(configured_path=self.base_path)  
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset1(self):
        path, full_sheet, build, service, ms, findexer = self.return_op_config(configured_path=self.base_path)  
        ms.reset_spreadsheet()


    """CLEAN UP AFTER JAN SCRAPE ONLY BUILD"""
    
    def test_db_reset1(self, populate):
        path, full_sheet, build, service, ms, findexer = self.return_scrape_config(configured_path=self.base_path2)  
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset1(self):
        path, full_sheet, build, service, ms, findexer = self.return_scrape_config(configured_path=self.base_path2)  
        ms.reset_spreadsheet()
    
    def test_load_init_db_state1_VIA_SCRAPE_AND_OP(self, set_write_mode):
        print('\n START SCRAPE and OP BUILD\n')
        path, full_sheet, build, service, ms, findexer = self.return_scrape_config(configured_path=self.base_path3)   # uses BuildRS not IterRS
        build.build_db_from_scratch(write=set_write_mode)  # this should write to rs

    def test_x(self):
        pass

    '''