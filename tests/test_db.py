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

create_tables_list = [BalanceLetter, Findexer, StatusObject, StatusRS, OpCash, OpCashDetail, Damages, Tenant, Unit, Payment, NTPayment, TenantRent]

target_bal_load_file = 'beginning_balance_2022.xlsx'
path = Config.TEST_RS_PATH_APRIL
populate = PopulateTable()
tenant = Tenant()
unit = Unit()
findex = FileIndexer(path=path, db=Config.TEST_DB)

'''arrange, act, assert, cleanup'''
'''basically, we just arrange to end of april, then check the state of the db'''

@pytest.mark.testing_fi
class TestFileIndexer:

    def test_fi_db(self):

        findex.build_index_runner()
        db_items = [item.fn for item in Findexer().select()]
        dir_contents = [item for item in findex.path.iterdir() if item.suffix != '.ini'] 
        assert len(dir_contents) == len(db_items)

    def test_dir_contents(self):
        findex.articulate_directory()
        assert len(findex.directory_contents) == 12
        
    def test_index_dict(self):
        findex.sort_directory_by_extension()
        assert list(findex.index_dict)[0].stem == 'beginning_balance_2022'

    def test_load_what_is_in_dir(self):
        findex.load_what_is_in_dir()
        db_items = [item.fn for item in Findexer().select()]
        dir_contents = [item for item in findex.path.iterdir() if item.suffix != '.ini'] 
        assert len(dir_contents) == len(db_items)

    def test_xls_list(self):
        findex.make_a_list_of_raw(mode='xls')
        assert len(findex.raw_list) == 9
        assert findex.raw_list[0][1] == 1
   
    def test_pdf_list(self):
        findex.make_a_list_of_raw(mode='pdf')
        assert len(findex.raw_list) == 3
        assert findex.raw_list[-1][-1] == 8

    def test_close(self):
        findex.drop_findex_table()
        findex.close_findex_table()
        assert Config.TEST_DB.is_closed() == True
    
    def test_fixture(self):
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

    @record
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
                 'damages': [('morris, michael', '599', '2022-02', 'exterm')],   
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
                 'opcash_amount': '3434.0',
                 'opcash_det_id': 7, 
                 'what_processed': [{'processed': True, 'tenant_reconciled': True, 'scrape_reconciled': False}], 
                 'endbal_cumsum': 2649.0, 
                 'bal_letters': []
                }

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

            breakpoint()
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
            assert damages == assert_list[i]['damages']

            '''check opcashes'''
            opcash_sum, opcash_detail = populate.consolidated_get_stmt_by_month(first_dt=first_dt, last_dt=last_dt)

            assert opcash_sum[0][0] == assert_list[i]['opcash_name']
            assert opcash_detail[0].amount == assert_list[i]['opcash_amount'] 
            assert opcash_detail[0].id == assert_list[i]['opcash_det_id'] 

            '''check statusobject'''
            what_is_processed = populate.get_status_object_by_month(first_dt=first_dt, last_dt=last_dt)
            assert what_is_processed == assert_list[i]['what_processed']

            '''tenant end bal'''
            positions, cumsum = populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)

            '''balance letters generated'''
            bal_letters = populate.get_balance_letters_by_month(first_dt=first_dt, last_dt=last_dt)

            assert bal_letters == []


        
        
        
    
    
    


    def test_load_remaining_months_rent(self):

        processed_rentr_dates_and_paths = [(item[1], item[2]) for item in rent_roll_list]
        processed_rentr_dates_and_paths.sort()

        for date, filename in processed_rentr_dates_and_paths:
            cleaned_nt_list, total_tenant_charges, cleaned_mos = populate.after_jan_load(filename=filename, date=date)
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=date)

            if date == '2022-02':
                total_rent_charges = populate.get_total_rent_charges_by_month(first_dt=first_dt, last_dt=last_dt)
                assert total_rent_charges == 15968.0 

                # get snapshot of vacants @ march end (after greiner mi)
                vacant_snapshot_loop_end = Unit.find_vacants()
                assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115'])

            if date == '2022-03':
                total_rent_charges = populate.get_total_rent_charges_by_month(first_dt=first_dt, last_dt=last_dt)
                assert total_rent_charges == 15972.0 
                vacant_snapshot_loop_end = Unit.find_vacants()
                assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115', 'PT-211'])

    def test_real_payments(self):
        file_list = [(item.fn, item.period, item.path) for item in Findexer().select().
            where(Findexer.doc_type == 'deposits').
            where(Findexer.status == 'processed').
            namedtuples()]
        
        processed_dates_and_paths = [(item[1], item[2]) for item in file_list]
        processed_dates_and_paths.sort()
        
        for date1, path in processed_dates_and_paths:
            grand_total, ntp, tenant_payment_df = populate.payment_load_full(filename=path)
            first_dt, last_dt = populate.make_first_and_last_dates(date_str=date1)

            if date1 == '2022-01':
                # check db commited ten payments and ntp against df
                all_tp, all_ntp = populate.check_db_tp_and_ntp(grand_total=grand_total, first_dt=first_dt, last_dt=last_dt) 

                # get beginning balance by tenant and check for duplicate payments
                detail_beg_bal_all = populate.get_all_tenants_beg_bal()

                different_names = populate.check_for_multiple_payments(detail_beg_bal_all=detail_beg_bal_all, first_dt=first_dt, last_dt=last_dt)

                if len(different_names) > 0:
                    detail_one = [row for row in detail_beg_bal_all if row[0] == different_names[0]]
                    assert detail_one == [('yancy, claude', '279.00', Decimal('-9')), ('yancy, claude', '18.00', Decimal('-9'))]

                # check beg_bal_amount again
                beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='initial')
                assert beg_bal_sum_by_period == 795.0
                # check total tenant payments sum db-side
                # check total tenant payments from dataframe against what I committed to db
                tp_sum_by_period_db, tp_sum_by_period_df = populate.match_tp_db_to_df(df=tenant_payment_df, first_dt=first_dt, last_dt=last_dt)

                # sum tenant payments by tenant
                sum_payment_list = populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)

                yancy_jan = [row for row in sum_payment_list if row[0] == 'yancy, claude'][0]
                assert yancy_jan[2] == float(Decimal('297.00'))

                # check jan ending balances by tenant
                end_bal_list_no_dec = populate.get_end_bal_by_tenant(first_dt=first_dt, last_dt=last_dt)

                tj_row = [row for row in end_bal_list_no_dec if row[0] == 'johnson, thomas'][0]
                yancy_row = [row for row in end_bal_list_no_dec if row[0] == 'yancy, claude'][0]
                jack_row = [row for row in end_bal_list_no_dec if row[0] == 'davis, jacki'][0]
                assert tj_row == ('johnson, thomas', -162.0)
                assert yancy_row == ('yancy, claude', -306.0)
                assert jack_row == ('davis, jacki', -211.0)

            if date1 == '2022-02':
                first_dt, last_dt = populate.make_first_and_last_dates(date_str=date1)
                all_tp, all_ntp = populate.check_db_tp_and_ntp(grand_total=grand_total, first_dt=first_dt, last_dt=last_dt)           

                beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='other', first_dt=first_dt, last_dt=last_dt)

                detail_beg_bal_all = populate.get_all_tenants_beg_bal()

                different_names = populate.check_for_multiple_payments(detail_beg_bal_all=detail_beg_bal_all, first_dt=first_dt, last_dt=last_dt)
           
                if len(different_names) > 0:
                    detail_one = [row for row in detail_beg_bal_all if row[0] == different_names[0]]
                    assert detail_one == [('coleman, william', '192.0', Decimal('-24')), ('coleman, william', '192.0', Decimal('-24'))]
      
                beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='initial')
                assert beg_bal_sum_by_period == 795.0

                tp_sum_by_period_db, tp_sum_by_period_df = populate.match_tp_db_to_df(df=tenant_payment_df, first_dt=first_dt, last_dt=last_dt)
        
                sum_payment_list = populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)

                test_feb = [row for row in sum_payment_list if row[0] == 'coleman, william'][0]
                assert test_feb[2] == float(Decimal('384.00'))

                end_bal_list_no_dec = populate.get_end_bal_by_tenant(first_dt=first_dt, last_dt=last_dt)

    def test_load_damages(self):
        Damages.load_damages()
        assert [row.tenant.tenant_name for row in Damages().select()][0] == 'morris, michael'

    def test_april_end_balances(self):
        test_date = '2022-04'
        populate = PopulateTable()
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)
        object1, cumsum = populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
        pprint([item for item in object1 if item.name == 'crombaugh, albert'])
   
    """
    def test_end_of_loop_state(self):
        '''tests after loop is completed'''

        '''former tenants'''
        '''active tenants'''
        '''vacant units'''
        '''occupied units'''
        '''unit accounting equals 67'''
        '''all charges'''
        '''all payments'''
        '''jan, feb, mar payments subtotal'''
   
        '''former tenants'''
        # is thomas johnson marked as inactive at end of loop
        former_tenants = [row.tenant_name for row in Tenant.select().where(Tenant.active=='False')]
        assert 'johnson, thomas' in [row for row in former_tenants]
        
        '''vacant units'''
        vacant_snapshot_loop_end = Unit.find_vacants()
        assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115'])

        '''occupied units'''
        all_units = [unit for unit in Unit().select()]
        occupied_num = len(all_units) - len(vacant_snapshot_loop_end) 
        assert occupied_num == 65

        '''all charges'''
        total_rent_charges_ytd = sum([float(row.rent_amount) for row in Tenant.select(Tenant.tenant_name, TenantRent.rent_amount).join(TenantRent).namedtuples()])
        # assert total_rent_charges_ytd == 47409.0  # end of march
        assert total_rent_charges_ytd == 63456.0   # end of april

        '''all payments'''
        total_payments_ytd = sum([float(row.amount) for row in Payment.select(Payment.amount).namedtuples()])
        # assert total_payments_ytd == 46686.0 # end of march
        assert total_payments_ydt == 61928.0 # end of april

        '''jan, feb, mar payments subtotal'''
        '''be aware of dates and active status'''

        '''jan: start_bal_sum = 793, tenant_rent = 15469, payments_made = 14975, end_bal_sum = 1287'''

        '''jan start bal = 793'''
        test_date = '2022-01'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        start_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).get().sum
        assert start_bal_sum == 793.0
        
        tenant_rent_total_jan = [float(row[1]) for row in populate.get_rent_charges_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(tenant_rent_total_jan) == 15469.0

        '''payments'''
        payments_jan = [float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(payments_jan) == 14975.0

        '''end jan balances'''
        computed_jan_end_bal = start_bal_sum + sum(tenant_rent_total_jan) - sum(payments_jan)

        tenant_activity_recordtype, cumsum_endbal= populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
        assert cumsum_endbal == 1287.0

        '''pick some tenants to check'''
        assert tenant_activity_recordtype[0].name == 'woods, leon'
        assert tenant_activity_recordtype[-1].end_bal == 0.0
        # assert len(tenant_activity_recordtype) == 65

        '''feb: ACTIVE_TENANT_ALL_TIME_START_BAL_SUM = 795? PERIOD_start_bal_sum = 1287, tenant_rent = 15968, payments_made = 15205 , end_bal_sum = 2050'''
        test_date = '2022-02'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        active_tenant_start_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).where(Tenant.active=='True').get().sum
        assert start_bal_sum == 793.0

        '''charges'''
        tenant_rent_total_jan = [float(row[1]) for row in populate.get_rent_charges_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(tenant_rent_total_jan) == 15968.0

        '''payments'''
        payments_jan = [float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(payments_jan) == 15205.0

        '''feb jan balances: THIS DOES NOT YET REFLECT DAMAGES: 599 FOR MIKE'''
        tenant_activity_recordtype, cumsum_endbal= populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)
        assert cumsum_endbal == 2649.0  

        '''march: relevant alltime beg bal = , tenant_rent = 15957, payments_made = 16506, end_bal_sum = 2100 - 599'''

        test_date = '2022-03'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        '''how does this work with thomas johnson??'''
        '''so on move-out we just grab what the report says the prorated balance is; we would find a discrepancy on a month-crossing retroactive move-out bc would not be picked up on the sheet'''
        active_tenant_start_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).where(Tenant.active=='True').get().sum
        assert active_tenant_start_bal_sum == 795.0

        '''charges'''
        tenant_rent_total_mar = [float(row[1]) for row in populate.get_rent_charges_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(tenant_rent_total_mar) == 15972.0

        '''payments'''
        payments_jan = [float(row[2]) for row in populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)]
        assert sum(payments_jan) == 16506.0

        tenant_activity_recordtype, cumsum_endbal= populate.net_position_by_tenant_by_month(first_dt=first_dt, last_dt=last_dt)

        cumsum_check = 2115.0
        
        assert cumsum_endbal == cumsum_check

    """
    def test_db_backup(self):
        
        DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)
        match_bool = DBUtils.find_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

        assert match_bool == True

   

    # def test_teardown(self):
    #     db.drop_tables(models=create_tables_list)
    #     db.close()    

    # def test_close_db(self):
    #     if db.is_closed() == False:
    #         db.close()

# @pytest.mark.testing_db
# class TestBuildAndStatus:

#     # def test_assert_all_db_empty_and_connections_closed(self):
#     #     assert db.get_tables() == []

#     def test_statusrs_starts_empty(self):
#         status = StatusRS()
#         status.set_current_date(mode='autodrop')
#         # breakpoint()
#         status.show(mode='just_asserting_empty')
#         most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]
#         proc_file = json.loads(most_recent_status.proc_file)
#         assert proc_file == []

#     def test_generic_build(self):
#         basedir = os.path.abspath(os.path.dirname(__file__))
#         build = BuildRS(path=path, main_db=db)
#         build.new_auto_build()
#         build.summary_assertion_at_period(test_date='2022-03')

#     def test_end_status(self):
#         most_recent_status = [item for item in StatusRS().select().order_by(-StatusRS.status_id).namedtuples()][0]
#         proc_file = json.loads(most_recent_status.proc_file)
#         assert proc_file[0] == {'deposits_01_2022.xls': '2022-01'}

#     def test_balance_letter_queries(self):
#         status = StatusRS()
#         balance_letters = status.show_balance_letter_list_mr_reconciled()
#         # assert len(balance_letters) == 9
#         # assert balance_letters[0].target_month_end == datetime.date(2022, 3, 31)
#         # breakpoint()
    
#     def test_teardown(self):
#         # breakpoint()
#         db.drop_tables(models=create_tables_list)
#         db.close()    

#     def test_close_db(self):
#         if db.is_closed() == False:
#             db.close()

# @pytest.mark.testing_dbshort
# class TestShort:

#     test_message = 'hi'
#     def test_message(self):
#         assert test_message == 'hi'


        # class SubsidyRent(BaseModel):
        #     pass

        # class ContractRent(BaseModel):
        #     pass





        
        



        