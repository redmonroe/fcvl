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

class TestFileIndexerIncr:

    @pytest.fixture
    def populate(self):
        populate = PopulateTable()
        return populate

    @pytest.fixture
    def return_base_config(self):
        """why? this loads db using BuildRS and writes to sheet"""
        figure = Figuration(method='build', path=Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_march_2022'))
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        yield path, full_sheet, build, service, ms, findexer

    @pytest.fixture
    def return_iter_config(self):
        """why? this loads db using BuildRS and writes to sheet"""
        figure = Figuration(path=Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_end_june_2022'), pytest=True)
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        yield path, full_sheet, build, service, ms, findexer

    @pytest.fixture
    def return_iter_config2(self):
        """why? this loads db using BuildRS and writes to sheet"""
        figure = Figuration(path=Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_mid_oct_2022'), pytest=True)
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        yield path, full_sheet, build, service, ms, findexer

    def test_db_reset(self, populate, return_base_config):
        path, full_sheet, build, service, ms, findexer = return_base_config
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_rs_reset(self, return_base_config):
        path, full_sheet, build, service, ms, findexer = return_base_config
        """test for ANY sheets before reset"""
        ms.reset_spreadsheet()

    def test_load_init_db_state(self, return_base_config):
        path, full_sheet, build, service, ms, findexer = return_base_config # uses BuildRS not IterRS
        build.build_db_from_scratch(write=True)  # this should write to rs
    
    def test_after_jan_load(self, populate, return_base_config):
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
        
        path, full_sheet, build, service, ms, findexer = return_base_config
        files = [fn for fn in path.iterdir()]
        assert len(files) == 11 # 3 files + beg balances + desktop.ini

        """test for statusobject state"""
        jan_so_state = [row for row in StatusObject.select().
            where(
                (StatusObject.opcash_processed==1) &
                (StatusObject.tenant_reconciled==1) &
                (StatusObject.rs_reconciled==1)
            ).
            namedtuples()]

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

    def test_after_jan_write(self):
        """do I want to do any checking of actual sheet with calls??"""
    
    def test_jan_through_june_iter_load_and_write(self, return_iter_config):
        path, full_sheet, iterb, service, ms, findexer = return_iter_config
        iterb.incremental_load()

    def test_jan_through_101822_iter_load_and_write(self, return_iter_config2):
        path, full_sheet, iterb, service, ms, findexer = return_iter_config2
        iterb.incremental_load()
        

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