import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path, PosixPath

from auth_work import oauth
from build_rs import BuildRS
from config import Config
from file_indexer import FileIndexer
from setup_month import MonthSheet

"""what is different about these tests?"""
"""testing a db in motion, so we cannot simply rely on resetting"""

class TestFileIndexerIncr:
    
    @pytest.fixture
    def return_test_config(self):
        path = Config.TEST_PATH
        full_sheet = Config.TEST_RS
        build = BuildRS(path=path, full_sheet=full_sheet, main_db=Config.TEST_DB)
        service = oauth(Config.my_scopes, 'sheet', mode='testing')
        ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
        findexer = FileIndexer(path=path, db=build.main_db)

        return path, full_sheet, build, service, ms, findexer

    @pytest.fixture
    def init_path(self):
        init_path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_first')
        yield init_path

    @pytest.fixture
    def incr_path1(self):
        incr_path1 = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_second') 
        yield incr_path1

    def test_db_reset(self, return_test_config):
        findexer = return_test_config[-1]
        findexer.drop_findex_table()
        findexer.close_findex_table()
        assert Config.TEST_DB.is_closed() == True

    def test_paths(self, init_path, incr_path1):
        assert init_path == PosixPath('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_first')
        assert incr_path1 == PosixPath('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/iter_build_second')

    def test_load_init_db_state(self, init_path):
        pass

    def test_close(self, return_test_config):
        findexer = return_test_config[-1]
        findexer.drop_findex_table()
        findexer.close_findex_table()
        assert Config.TEST_DB.is_closed() == True
