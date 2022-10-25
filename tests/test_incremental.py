import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path, PosixPath

from auth_work import oauth
from backend import PopulateTable, Findexer, StatusObject, StatusRS
from iter_rs import IterRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet
from cli import Figuration


"""what is different about these tests?"""
"""testing a db in motion, so we cannot simply rely on resetting"""
"""set write to rs with 'write' option"""

class TestFileIndexerIncr:

    base_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_march_2022'
    iter1_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_end_june_2022'
    iter2_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_mid_oct_2022'
    no_scrape_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/no_scrape_thru_sept'
    write = True

    @pytest.fixture
    def populate(self):
        populate = PopulateTable()
        return populate

    def query_for_tst1(self, rs_reconciled=None):
        jan_state = [row for row in StatusObject.select().
            where(
                (StatusObject.opcash_processed==1) &
                (StatusObject.tenant_reconciled==1) &
                (StatusObject.rs_reconciled==rs_reconciled)
            ).
            namedtuples()]
        return jan_state
        

    def return_generic_config(self, type1=None, configured_path=None):
        if type1 == 'iter':
            figure = Figuration(path=Path(configured_path), pytest=True)
        else: 
            figure = Figuration(method='build', path=Path(configured_path), pytest=True)
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        return path, full_sheet, build, service, ms, findexer

    def test_db_reset1(self, populate):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)  
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset1(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)  
        """test for ANY sheets before reset"""
        ms.reset_spreadsheet()

    def test_load_init_db_state1(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)   # uses BuildRS not IterRS
        build.build_db_from_scratch(write=self.write)  # this should write to rs

    def test_after_jan_load(self, populate):
        print('test_after_jan_load() hiiii')
        """
        doesn't need to be high engineering here: just
        compare the number of files to number of entries
        
        ******focus on statusobject  
        """
        d_rows = populate.get_all_findexer_by_type(type1='deposits')
        assert len(d_rows) == 3

        o_rows = populate.get_all_findexer_by_type(type1='opcash')
        assert len(o_rows) == 3

        r_rows = populate.get_all_findexer_by_type(type1='rent')
        assert len(r_rows) == 3
        
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)
        files = [fn for fn in path.iterdir()]
        assert len(files) == 11 # 3 files + beg balances + desktop.ini

        """test for statusobject state"""
        if self.write == True:
            jan_so_state = self.query_for_tst1(rs_reconciled=1)
        else:
            jan_so_state = self.query_for_tst1(rs_reconciled=0)
          
        assert jan_so_state[0].month == '2022-01'
        assert jan_so_state[0].scrape_reconciled == False 
        remainder_so_state = [row for row in StatusObject.select().
            where(
                (StatusObject.opcash_processed==0) &
                (StatusObject.tenant_reconciled==0) &
                (StatusObject.scrape_reconciled==0)
                )
            .
            namedtuples()]

        assert len(remainder_so_state) > 6
    
    def test_jan_through_june_iter_load_and_write(self):
        print('3, 4, 5 already exist, should just write 4, 5, 6')
        path, full_sheet, iterb, service, ms, findexer = self.return_generic_config(type1='iter', configured_path=self.iter1_path)
        iterb.incremental_load(write=self.write)

    def test_jan_through_101822_iter_load_and_write(self):
        print('should be doing 7, 8, 9, and 10')
        path, full_sheet, iterb, service, ms, findexer = self.return_generic_config(type1='iter', configured_path=self.iter2_path)
        iterb.incremental_load(write=self.write)
        
    """
    DO TEST STUFF HERE    
    
    """
    def test_db_reset2(self, populate):
        path, full_sheet, iterb, service, ms, findexer = self.return_generic_config(type1='iter', configured_path=self.iter2_path)
        create_tables_list1 = populate.return_tables_list()
        iterb.main_db.drop_tables(models=create_tables_list1)
        assert iterb.main_db.get_tables() == []

    def test_rs_reset2(self):
        path, full_sheet, iterb, service, ms, findexer = self.return_generic_config(type1='iter', configured_path=self.iter2_path)
        ms.reset_spreadsheet()

    def test_load_init_db_state2(self):
        print('SETUP ROUND 2 BUILDING DB FROM SCRATCH(write=True')
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)   
        build.build_db_from_scratch(write=self.write)  # this should write to rs

    def test_no_scrape_load_and_write(self):
        print('ROUND 2: no scrape load and write')
        path, full_sheet, iterb, service, ms, findexer = self.return_generic_config(type1='iter', configured_path=self.no_scrape_path)
        iterb.incremental_load()
    
    """NO SCRAPE TESTING HERE"""

    def test_db_reset3(self, populate):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)
        # path, full_sheet, build, service, ms, findexer = return_base_config
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset3(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)
        # path, full_sheet, build, service, ms, findexer = return_base_config
        ms.reset_spreadsheet()
    '''
    def test_process_files_step_one(self):
        """what do we want to do here??"""
        """this is where we would be triggering events
            - payments, ntp, etc
            - reconciliations
            - updating any 
        
        """
        pass


    def test_load_incr_state(self, return_test_config_incr1):
        path, full_sheet, build, service, ms, findexer = return_test_config_incr1

        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_second')

    def test_incr_state(self):
        """WE NEED TO BE TESTING UNFINALIZED MONTHS AND HOW SYSTEM REACTS"""

    def test_close(self, return_test_config_init):
        path, full_sheet, build, service, ms, findexer = return_test_config_init
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        # assert Config.TEST_DB.is_closed() == True

    '''