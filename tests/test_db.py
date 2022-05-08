import calendar
import datetime
import json
import os
import sys

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


target_bal_load_file = 'beginning_balance_2022.xlsx'
path = Config.TEST_RS_PATH_APRIL
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
        findex.make_a_list_of_raw(mode='xls')
        assert len(findex.raw_list) == 0  
        findex.make_a_list_of_raw(mode='pdf')
        assert len(findex.raw_list) == 0

    def test_iter_run_with_may_dir(self):
        may_path = Config.TEST_RS_PATH_MAY
        may_findex = FileIndexer(path=may_path, db=Config.TEST_DB)
        index_dict = may_findex.iter_build_runner()
        assert may_findex.unproc_file_for_testing == ['op_cash_2022_04.pdf']
        assert list(may_findex.index_dict_iter.values())[0][0] == '.pdf'

        '''show that db after may update includes raw opcash_04'''
        assert Path(may_findex.raw_list[0][0]).name == 'op_cash_2022_04.pdf'
        '''show that db after may update includes proc"d opcash_04'''
        proc_items = [item.fn for item in Findexer().select().where(Findexer.status=='processed').namedtuples() if item.fn not in findex.excluded_file_names]
        assert 'op_cash_2022_04.pdf' in proc_items

    def test_close(self):
        findex.drop_findex_table()
        findex.close_findex_table()
        assert Config.TEST_DB.is_closed() == True
    
@pytest.mark.testing_db
class TestDB:

    def test_reset_all(self):
        db.connect()
        # if db.get_tables() != []:
        db.drop_tables(models=create_tables_list)

    def test_db(self):
        db.create_tables(create_tables_list)
        assert db.database == '/home/joe/local_dev_projects/fcvl/sqlite/test_pw_db.db'
        # assert sorted(db.get_tables()) == sorted(['balanceletter, statusobject', 'opcash', 'opcashdetail', 'damages', 'tenantrent', 'ntpayment', 'payment', 'tenant', 'unit', 'statusrs', 'findexer'])
        assert [*db.get_columns(table='payment')[0]._asdict().keys()] == ['name', 'data_type', 'null', 'primary_key', 'table', 'default']

    def test_set_init_state_at_end_of_april(self):
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.new_auto_build()                      

    # @record
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
        all_ten_beg_bal = populate.get_all_tenants_beg_bal(cumsum=True)
        assert all_ten_beg_bal == 793

        first_dt, last_dt = populate.make_first_and_last_dates(date_str=jan_date)
        rent_roll, vacants, tenants = populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

        assert len(rent_roll) == 67
        assert len(vacants) == 3
        assert len(tenants) == 64

        assert 'johnson, thomas' in tenants
        assert 'greiner, richard' not in tenants

        # '''current occupied: johnson in, greiner, kelly out'''
        # tenants = populate.get_current_tenants_by_month(first_dt=first_dt, last_dt=last_dt)
        # assert len(tenants) == 64
        # assert 'johnson, thomas' in tenants
        # assert 'greiner, richard' not in tenants

        # '''current vacant: '''
        # vacant_units = populate.get_current_vacants_by_month(last_dt=last_dt)

        # vacant_units = [item[1] for item in vacant_units]
        # breakpoint()
        # assert vacant_units == ['CD-101', 'CD-115', 'PT-201']
        # assert len(vacant_units) == 3

        # '''sum of vacants and currents'''
        # assert len(vacant_units) + len(tenants) == 67

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
        assert what_is_processed == [{'processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}]

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
                 'what_processed': [{'processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 2649.0, 
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
                 'what_processed': [{'processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 2115.0, 
                 'bal_letters': []
                }, 
                #   {
                #  'date': '2022-04', 
                #  'processed_record1': 'deposits_04_2022.xlsx', 
                #  'rr_len': 64, 
                #  'current_vacants': ['CD-101', 'CD-115', 'PT-211'], 
                #  'vacant_len': 3, 
                #  'sum_ntp': 227.27,
                #  'damages': [],  
                #  'opcash_name': None, 
                #  'opcash_amount': None,
                #  'opcash_det_id': 13, 
                #  'what_processed': [{'processed': False, 'tenant_reconciled': False, 'scrape_reconciled': False}], 
                #  'endbal_cumsum': 2933.0, 
                #  'bal_letters': []
                # }, 

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

            first_dt, last_dt = populate.make_first_and_last_dates(date_str=assert_list[i]['date'])

            '''all tenants beginning balance amount'''
            all_ten_beg_bal = populate.get_all_tenants_beg_bal(cumsum=True)
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

@pytest.mark.testing_db
class DBBackup:

    def test_db_backup(self):
        
        DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)
        match_bool = DBUtils.find_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

        assert match_bool == True
   
 






        
        



        