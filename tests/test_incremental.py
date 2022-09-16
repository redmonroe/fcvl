import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path, PosixPath

from auth_work import oauth
from backend import PopulateTable, Findexer
from iter_rs import IterRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet


"""what is different about these tests?"""
"""testing a db in motion, so we cannot simply rely on resetting"""

class TestFileIndexerIncr:

    @pytest.fixture
    def populate(self):
        populate = PopulateTable()
        return populate

    @pytest.fixture
    def return_test_config_init(self):
        path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_first')
        full_sheet = Config.TEST_RS
        build = IterRS(path=path, full_sheet=full_sheet, main_db=Config.TEST_DB)
        breakpoint()
        service = oauth(Config.my_scopes, 'sheet', mode='testing')
        ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
        findexer = FileIndexer(path=path, db=build.main_db)

        return path, full_sheet, build, service, ms, findexer

    @pytest.fixture
    def return_test_config_incr1(self):
        path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_second')
        full_sheet = Config.TEST_RS
        build = IterRS(path=path, full_sheet=full_sheet, main_db=Config.TEST_DB)
        service = oauth(Config.my_scopes, 'sheet', mode='testing')
        ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
        findexer = FileIndexer(path=path, db=build.main_db)

        return path, full_sheet, build, service, ms, findexer

    def test_db_reset(self, return_test_config_init):
        path, full_sheet, build, service, ms, findexer = return_test_config_init
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        build.main_db.drop_tables(models=create_tables_list1)
        assert build.main_db.get_tables() == []

    def test_load_init_db_state(self, return_test_config_init):
        path, full_sheet, build, service, ms, findexer = return_test_config_init
        build.incremental_load()  
        """focus on statusobject: why are so many months being procesed and marked as reconciled"""      

    def test_init_state(self, populate, return_test_config_init):
        """
        doesn't need to be high engineering here: just
        compare the number of files to number of entries
        """
        d_rows = populate.get_all_findexer_by_type(type1='deposits')
        assert len(d_rows) == 3

        o_rows = populate.get_all_findexer_by_type(type1='opcash')
        assert len(o_rows) == 3

        r_rows = populate.get_all_findexer_by_type(type1='rent')
        assert len(r_rows) == 3
        
        path, full_sheet, build, service, ms, findexer = return_test_config_init
        files = [fn for fn in path.iterdir()]
        assert len(files) == 11 # 9 files + beg balances + desktop.ini

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
