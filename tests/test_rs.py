import json
import os
import sys
import time
from datetime import datetime

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from pathlib import Path

from auth_work import oauth
from backend import (Damages, Findexer, NTPayment, OpCash, OpCashDetail,
                     Payment, PopulateTable, StatusObject, StatusRS, Subsidy,
                     Tenant, TenantRent, Unit, db)
from build_rs import BuildRS
from config import Config
from file_indexer import FileIndexer
from google_api_calls_abstract import GoogleApiCalls
from googleapiclient.errors import HttpError
from setup_month import MonthSheet
from setup_year import YearSheet

full_sheet = Config.TEST_RS
path = Config.TEST_RS_PATH
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()
ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
create_tables_list = [Subsidy, Findexer, StatusObject, StatusRS, OpCash, OpCashDetail, Damages, Tenant, Unit, Payment, NTPayment, TenantRent]
'''how can I import object names without having to import object in Config class'''

@pytest.mark.testing_rs
class TestWrite:

    def test_assert_all_db_empty_and_connections_closed(self):
        assert db.get_tables() == []

    def test_statusrs_starts_empty(self):
        status = StatusRS()
        status.set_current_date(mode='autodrop')
        status.show(mode='just_asserting_empty')
        most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]
        proc_file = json.loads(most_recent_status.proc_file)
        assert proc_file == []

    def test_generic_build(self):
        basedir = os.path.abspath(os.path.dirname(__file__))
        build = BuildRS(path=path, main_db=db)
        build.new_auto_build()
        build.summary_assertion_at_period(test_date='2022-03')

    def test_end_status(self):
        most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]
        proc_file = json.loads(most_recent_status.proc_file)
        assert proc_file[0] == {'deposits_01_2022.xls': '2022-01'}

@pytest.mark.testing_rs_sub1
class TestRSOnly:

    def test_setup_sheet_prime(self):
        title_dict = ms.show_current_sheets()
        for name, id2, in title_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, full_sheet, id2)

    def test_send_to_setup_month(self):
        ms.auto_control()

    def test_select_from_sheets_after_writing(self):
        result = calls.broad_get(service, full_sheet, '2022-01!k68:k68')
        result2 = calls.broad_get(service, full_sheet, '2022-02!f68:fq68')
        grand_total = calls.broad_get(service, full_sheet, '2022-01!k77:k77')
        assert result[0][0] == '153'
        assert result2[0][0] == '588'
        assert grand_total[0][0] == '15491.71'
       
    def test_teardown_sheets(self):
        title_dict = ms.show_current_sheets()
        for name, id2, in title_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, full_sheet, id2)
                  
    # def test_teardown(self):
    #     db.drop_tables(models=create_tables_list)
    #     db.close()    

    # def test_close_db(self):
    #     if db.is_closed() == False:
    #         db.close()


