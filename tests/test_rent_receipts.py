import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path, PosixPath
from letters import DocxWriter
from cli import Figuration
from file_indexer import FileIndexer

class TestDocxRentReceipts:

    no_scrape_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/no_scrape_thru_sept'
    base_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_march_2022'

    def return_generic_config(self, type1=None, configured_path=None):
        if type1 == 'iter':
            figure = Figuration(path=Path(configured_path), pytest=True)
        else: 
            figure = Figuration(method='build', path=Path(configured_path), pytest=True)
        
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        return path, full_sheet, build, service, ms, findexer

    def test_rs_status(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)  
        writer = DocxWriter(db=build.main_db, service=service)
        sheet_idx = writer.check_rs_status()

        if sheet_idx == []:
            build.build_db_from_scratch(write=True)
        else:
            writer.docx_rent_receipts_from_rent_sheet(mode='testing')


        """no we need to read from outputted file"""
        # ms.reset_spreadsheet()