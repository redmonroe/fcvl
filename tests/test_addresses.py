import os
import sys

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path, PosixPath

from figuration import Figuration
from config import Config
from file_indexer import FileIndexer


class TestAddresses:

    base_path = '/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/thru_march_2022'

    def return_generic_config(self, type1=None, configured_path=None):
        if type1 == 'iter':
            figure = Figuration(path=Path(configured_path), pytest=True)
        else: 
            figure = Figuration(method='build', path=Path(configured_path), pytest=True)
        
        path, full_sheet, build, service, ms = figure.return_configuration()
        findexer = FileIndexer(path=path, db=build.main_db.database)
        return path, full_sheet, build, service, ms, findexer

    def test_write_address_excel(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)  


    def test_get_rs_status(self):
        path, full_sheet, build, service, ms, findexer = self.return_generic_config(configured_path=self.base_path)  
        # writer = DocxWriter(db=build.main_db, service=service)
        # sheet_idx = writer.check_rs_status()

        """TESTS WOULD BE FROM PERSPECTIVE OF MARCH END"""


        if sheet_idx == []:
            build.build_db_from_scratch(write=True)
            breakpoint()
            # money_lines, sum_for_test, document = self.test_write_docx()
        else:
            pass
            # money_lines, sum_for_test, document= self.test_write_docx()
        
        # assert sum([float(line) for line in sum_for_test]) == sum([float(line) for line in money_lines])
        # assert document.core_properties.title == 'docx_rent_receipts'
        # writer = DocxWriter(db=build.main_db, service=service)
        # document, save_path, sum_for_test = writer.docx_rent_receipts_from_rent_sheet(mode='testing')
        # full_texts = []

        # for para in document.paragraphs:
            # full_texts.append(para.text)

        # pay_lines = [line for line in full_texts if line[:3] == 'Our']
        # money_lines = [line.split('$') for line in pay_lines]
        # money_lines = [line[-1].replace('.', '').rstrip().lstrip() for line in money_lines]

        # return money_lines, sum_for_test, document
