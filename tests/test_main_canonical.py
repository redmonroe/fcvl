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

class TestMainCanonical:

    base_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/canonical_docs'

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
        ms.reset_spreadsheet()

    def test_load_init_db_state1(self, set_write_mode):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)   # uses BuildRS not IterRS
        build.build_db_from_scratch(write=set_write_mode)  # this should write to rs