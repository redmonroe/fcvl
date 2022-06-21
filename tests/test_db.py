import json
import os
import sys
import time
from datetime import datetime

import pytest

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
import calendar
import datetime
import json
import os
import sys
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
from utils import Utils

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from pprint import pprint

import pytest
from backend import (BalanceLetter, Damages, Findexer, NTPayment, OpCash,
                     OpCashDetail, Payment, PopulateTable, StatusObject,
                     StatusRS, Tenant, TenantRent, Unit, db)
from build_rs import BuildRS
from config import Config
from db_utils import DBUtils
from file_indexer import FileIndexer
from peewee import JOIN, fn
from records import record

# from rs
full_sheet = Config.TEST_RS
path = Config.TEST_RS_PATH_MAY 
target_bal_load_file = Config.beg_bal_xlsx
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()
ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
populate = PopulateTable()
tenant = Tenant()
unit = Unit()
findex = FileIndexer(path=path, db=Config.TEST_DB)

create_tables_list = populate.return_tables_list()

'''arrange, act, assert, cleanup'''
'''basically, we just arrange to end of april, then check the state of the db'''

@pytest.mark.testing_db
class TestFileIndexer:
    
    '''simple test to pin predictably end-state for file_index db as I make changes; all functionality is'''
    '''wrapped into latter calls from build_rs()'''

    def test_db_reset(self):
        findex.drop_findex_table()
        findex.close_findex_table()
        assert Config.TEST_DB.is_closed() == True

    def test_init_run_with_april(self):
        findex.build_index_runner()
        db_items = [item.fn for item in Findexer().select().namedtuples() if item.fn not in findex.excluded_file_names]
        dir_contents = [item for item in findex.path.iterdir() if item.name not in findex.excluded_file_names] 
        assert len(dir_contents) == len(db_items)
        findex.make_a_list_of_indexed(mode=findex.query_mode.xls)
        assert len(findex.unproc_file_for_testing) == 0  
        findex.make_a_list_of_indexed(mode=findex.query_mode.pdf)
        assert len(findex.unproc_file_for_testing) == 0

    def test_close(self):
        findex.drop_findex_table()
        findex.close_findex_table()
        assert Config.TEST_DB.is_closed() == True

@pytest.mark.testing_db
class TestDB:

    def test_reset_all(self):
        db.connect()
        db.drop_tables(models=create_tables_list)

    def test_set_init_state_at_end_of_april(self):
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()       

    def test_initial_tenant_load(self):
        '''JANUARY IS DIFFERENT'''
        '''processed records+''' 
        '''beginning balance for all tenants+'''
        '''current tenant = tenants who lived here during month and did not move out during month+'''
        '''vacant units+'''
        '''unit accounting equals 67+'''
        '''all charges+'''
        '''all payments+'''
        '''opcash etc+'''
        '''ntpayments'''
        '''damages+'''
        '''statusobject+'''
        '''tenant end balances+'''
        '''balance letters+'''

        populate = PopulateTable()

        '''processed records'''
        records = [(item.fn, item.period, item.path) for item in Findexer().select().
            where(Findexer.status == 'processed').
            where(Findexer.period == '2022-01').
            namedtuples()]

        assert sorted([item[0] for item in records]) == ['deposits_01_2022.xls', 'op_cash_2022_01.pdf', 'rent_roll_01_2022.xls']

        jan_date = records[0][1]
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=jan_date)

        '''all tenants beginning balance amount'''
        all_ten_beg_bal = populate.get_all_tenants_beg_bal()
        assert all_ten_beg_bal == 793

        rent_roll, vacants, tenants = populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

        assert len(rent_roll) == 67
        assert len(vacants) == 3
        assert len(tenants) == 64

        assert 'johnson, thomas' in tenants
        assert 'greiner, richard' not in tenants

        '''jan occupied: johnson in, greiner, kelly out'''
        tenants = populate.get_current_tenants_by_month(first_dt=first_dt, last_dt=last_dt)
        assert len(tenants) == 64
        assert 'johnson, thomas' in tenants
        assert 'greiner, richard' not in tenants

        '''jan vacant: '''
        """I dont have a way to access historical vacants by date at this time; I should be able to use existing get_rentroll_by_first_of_month in query"""

        '''get current charges: individual and sum'''
        current_charges = populate.get_rent_charges_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt)
        current_charges = sum([float(item[1]) for item in current_charges])
        sum_current_charges = populate.get_total_rent_charges_by_month(last_dt=last_dt, first_dt=first_dt)
        assert current_charges == sum_current_charges

        '''get current payments: individual and sum'''
        current_payments = populate.get_payments_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt, cumsum=True)
        current_payments2 = populate.get_payments_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt)
        current_payments2 = sum([float(item[2]) for item in current_payments2])
        assert current_payments == current_payments2

        '''check opcashes'''
        opcash_sum, opcash_detail = populate.consolidated_get_stmt_by_month(first_dt=first_dt, last_dt=last_dt)
        assert opcash_sum[0][0] == 'op_cash_2022_01.pdf'
        assert opcash_detail[0].amount == '4019.0'
        assert opcash_detail[0].id == 1

        '''check ntp'''
        ntp = populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt)
        assert sum(ntp) == 516.71

        '''check damages'''
        damages = populate.get_damages_by_month(first_dt=first_dt, last_dt=last_dt)        
        assert damages == []

        '''check statusobject'''
        what_is_processed = populate.get_status_object_by_month(first_dt=first_dt, last_dt=last_dt)
        assert what_is_processed == [{'opcash_processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}]

        '''tenant end bal'''
        positions, cumsum = populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
        assert cumsum == 1287.0

        '''balance letters generated'''
        bal_letters = populate.get_balance_letters_by_month(first_dt=first_dt, last_dt=last_dt)
        assert bal_letters == []

    def remaining_months_loop(self):
        '''processed records+''' 
        '''beginning balance for all tenants+'''
        '''current tenant = tenants who lived here during month and did not move out during month+'''
        '''vacant units+'''
        '''unit accounting equals 67+'''
        '''all charges+'''
        '''all payments+'''
        '''opcash etc+'''
        '''ntpayments'''
        '''damages+'''
        '''statusobject+'''
        '''tenant end balances+'''
        '''balance letters+'''

        assert_list = [
                {
                 'date': '2022-02', 
                 'processed_record1': 'deposits_02_2022.xlsx', 
                 'rr_len': 64, 
                 'current_vacants': ['CD-101', 'CD-115', 'PT-201'], 
                 'vacant_len': 3, 
                 'sum_ntp': 726.3,
                 'damages': 'morris, michael',
                 'opcash_name': 'op_cash_2022_02.pdf', 
                 'opcash_amount': '3434.0',
                 'opcash_det_id': 7, 
                 'what_processed': [{'opcash_processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 3125.0, 
                 'bal_letters': []
                }, 
                {
                 'date': '2022-03', 
                 'processed_record1': 'deposits_03_2022.xlsx', 
                 'rr_len': 65, 
                 'current_vacants': ['CD-101', 'CD-115'], 
                 'vacant_len': 2, 
                 'sum_ntp': 272.95,
                 'damages': [],  
                 'opcash_name': 'op_cash_2022_03.pdf', 
                 'opcash_amount': '3639.0',
                 'opcash_det_id': 13, 
                 'what_processed': [{'opcash_processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 2591.0, 
                 'bal_letters': []
                }, 
                  {
                 'date': '2022-04', 
                #  'processed_record1': 'deposits_04_2022.xlsx', 
                 'processed_record1': 'CHECKING_1891_Transactions_2022-04-01_2022-04-27.csv', 
                 'rr_len': 64, 
                 'current_vacants': ['CD-101', 'CD-115', 'PT-211'], 
                 'vacant_len': 3, 
                 'sum_ntp': 227.27,
                 'damages': [],  
                 'opcash_amount': '3714.0',
                 'opcash_name': 'op_cash_2022_04.pdf', 
                 'opcash_det_id': 19, 
                 'what_processed': [{'opcash_processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 3409.0, 
                 'bal_letters': []
                }, 

        ]
        return assert_list


    def test_remaining_months(self):
        assert_list = self.remaining_months_loop()
        
        for i in range(len(assert_list)):
            records = [(item.fn, item.period, item.path) for item in Findexer().select().
                where(Findexer.status == 'processed').
                where(Findexer.period == assert_list[i]['date']).
                namedtuples()]

            assert records[0][0] == assert_list[i]['processed_record1']
            break

            first_dt, last_dt = populate.make_first_and_last_dates(date_str=assert_list[i]['date'])

            '''all tenants beginning balance amount'''
            all_ten_beg_bal = populate.get_all_tenants_beg_bal()
            assert all_ten_beg_bal == 793

            '''current occupied'''
            '''current vacant: '''
            rent_roll, vacants, tenants = populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

            assert len(rent_roll) == 67
            assert len(vacants) == assert_list[i]['vacant_len']
            transformed_vacants = [item[1] for item in vacants]
            assert sorted(transformed_vacants) == sorted(assert_list[i]['current_vacants']) 

            if assert_list[i]['date'] == '2022-02':
                assert 'johnson, thomas' in tenants
                assert 'greiner, richard' not in tenants

            if assert_list[i]['date'] == '2022-03':
                assert 'johnson, thomas' in tenants
                assert 'greiner, richard' in tenants    

            if assert_list[i]['date'] == '2022-04':
                assert 'johnson, thomas' not in tenants
                assert 'greiner, richard' in tenants
                assert 'kelly, daniel' not in tenants 

            '''current_charges'''
            current_charges = populate.get_rent_charges_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt)
            current_charges = sum([float(item[1]) for item in current_charges])
            sum_current_charges = populate.get_total_rent_charges_by_month(last_dt=last_dt, first_dt=first_dt)

            assert current_charges == sum_current_charges

            '''get current payments: individual and sum'''
            current_payments = populate.get_payments_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt, cumsum=True)
            current_payments2 = populate.get_payments_by_tenant_by_period(last_dt=last_dt, first_dt=first_dt)
            current_payments2 = sum([float(item[2]) for item in current_payments2])
            assert current_payments == current_payments2

            '''check ntp'''
            ntp = populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt)
            assert sum(ntp) == assert_list[i]['sum_ntp']

            '''check damages'''
            damages = populate.get_damages_by_month(first_dt=first_dt, last_dt=last_dt)
            if assert_list[i]['date'] == '2022-02':
                assert damages[0][0] ==assert_list[i]['damages']
            else:
                print('damages test date:', assert_list[i]['date'])
                assert damages == assert_list[i]['damages']

            '''check opcashes'''
            try:
                opcash_sum, opcash_detail = populate.consolidated_get_stmt_by_month(first_dt=first_dt, last_dt=last_dt)

                assert opcash_sum[0][0] == assert_list[i]['opcash_name']
                assert opcash_detail[0].amount == assert_list[i]['opcash_amount'] 
                assert opcash_detail[0].id == assert_list[i]['opcash_det_id']
            except IndexError as e:
                print(f'No value set for opcash_amount, opcash_det_id, opcash_name: {e}') 

            '''check statusobject'''
            what_is_processed = populate.get_status_object_by_month(first_dt=first_dt, last_dt=last_dt)
            assert what_is_processed == assert_list[i]['what_processed']

            '''tenant end bal'''
            positions, cumsum = populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
            assert cumsum == assert_list[i]['endbal_cumsum']

            '''balance letters generated'''
            bal_letters = populate.get_balance_letters_by_month(first_dt=first_dt, last_dt=last_dt)
    
            assert bal_letters == assert_list[i]['bal_letters']

    def test_teardown(self):
        db.drop_tables(models=create_tables_list)
        db.close()

    def test_close_db(self):
        if db.is_closed() == False:
            db.close()    

# @pytest.mark.testing_db_iter
@pytest.mark.testing_db
class TestIterBuild:

    def test_setup(self):
        """build needs empty db and will handle initialization at run-time; does not need additional layer of dropping/creating from test"""
        db.connect()
        db.drop_tables(models=create_tables_list)
        assert db.database == '/home/joe/local_dev_projects/fcvl/sqlite/test_pw_db.db'

    def test_iter_build1(self):
        """tests include:
        - coverage of 2022-01-01 to 2022-03-30
        - number of files in file_index db
        - tenant payment sum for entire period
        - ntp sum for entire period: 2022-01-01 to
        - moveins
        - opcash
         2022-03-30
        """
        populate = PopulateTable()
        path = Config.TEST_RS_PATH_ITER_BUILD1
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()
        assert build.ctx == 'db empty'
        assert path.stem == 'iter_build_first'
        first_dt, _ = populate.make_first_and_last_dates(date_str='2022-01')
        _, last_dt = populate.make_first_and_last_dates(date_str='2022-03')

        snapshot_of_all_recs = [item for item in Findexer.select().namedtuples()]
        assert len(snapshot_of_all_recs) == 9

        ntp_1q = populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt)
        assert sum(ntp_1q) == 1515.96
        
        tp_1q = populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt, cumsum=True)
        assert tp_1q == 46210.0

        mi_1q = populate.get_move_ins_by_period(first_dt=first_dt, last_dt=last_dt)
        assert mi_1q[0] == ('2022-02-07', 'greiner, richard')

        opcash_1q = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        assert len(opcash_1q) == 3

    def test_iter_build2(self):
        """tests include:
        - coverage of 2022-01-01 to 2022-04-30
        - number of files in file_index db
        - tenant payment sum for entire period
        - ntp sum for entire period: 2022-01-01 to
        - moveins
        - opcash
        """
        path = Config.TEST_RS_PATH_ITER_BUILD2
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()
        snapshot_of_all_recs = [item for item in Findexer.select().namedtuples()]
        assert len(snapshot_of_all_recs) == 11

        first_dt, _ = populate.make_first_and_last_dates(date_str='2022-01')
        _, last_dt = populate.make_first_and_last_dates(date_str='2022-04')

        ntp_4m = populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt)
        assert sum(ntp_4m) == 1743.23
        
        tp_4m = populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt, cumsum=True)
        assert tp_4m == 61452.0

        mi_4m = populate.get_move_ins_by_period(first_dt=first_dt, last_dt=last_dt)
        assert mi_4m[1] == ('2022-04-15', 'kelly, daniel')

        opcash_4m = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
        assert len(opcash_4m) == 3
        assert opcash_4m[-1][-2] == '16778.95'

    def test_iter_cleanup(self):
        db.drop_tables(models=create_tables_list)

@pytest.mark.testing_db
class DBBackup:

    def test_db_backup(self):
        
        DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)
        match_bool = DBUtils.find_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

        assert match_bool == True

"""start here"""

@pytest.mark.testing_rs
class TestWrite:

    def test_assert_all_db_empty_and_connections_closed(self):
        if db.get_tables() != []:
            db.drop_tables(models=create_tables_list)
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
        build.build_db_from_scratch()

    def test_end_status(self):
        most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]
        proc_file = json.loads(most_recent_status.proc_file)
        assert proc_file[0] == {'deposits_01_2022.xls': '2022-01'}

@pytest.mark.testing_rs
class TestRSOnly:

    def test_setup_sheet_prime(self):
        title_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
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
        title_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        for name, id2, in title_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, full_sheet, id2)
                  
    def test_teardown(self):
        if db.get_tables() != []:
            db.drop_tables(models=create_tables_list)
        db.close()    

    def test_close_db(self):
        if db.is_closed() == False:
            db.close()








        
        



        