import calendar
import datetime
import os
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from pprint import pprint

import pytest
from backend import (Damages, NTPayment, OpCash, OpCashDetail, Payment,
                     PopulateTable, Tenant, TenantRent, Unit, db)
from build_rs import BuildRS
from config import Config
from db_utils import DBUtils
from file_indexer import FileIndexer
from peewee import JOIN, fn
from records import record

create_tables_list = [OpCash, OpCashDetail, Damages, Tenant, Unit, Payment, NTPayment, TenantRent]

target_bal_load_file = 'beginning_balance_2022.xlsx'
path = Config.TEST_RS_PATH
findex_db = Config.test_findex_db
findex_tablename = Config.test_findex_name
populate = PopulateTable()
tenant = Tenant()
unit = Unit()
findex = FileIndexer(path=path, db=findex_db, tablename=findex_tablename)

@pytest.mark.testing_db
class TestDB:
    '''basic idea: db connect > findex.build_index_runner > get_processed > '''

    def test_db(self):
        db.connect()
        db.drop_tables(models=create_tables_list)
        db.create_tables(create_tables_list)
        assert db.database == '/home/joe/local_dev_projects/fcvl/sqlite/test_pw_db.db'
        assert sorted(db.get_tables()) == sorted(['opcash', 'opcashdetail', 'damages', 'tenantrent', 'ntpayment', 'payment', 'tenant', 'unit'])
        assert [*db.get_columns(table='payment')[0]._asdict().keys()] == ['name', 'data_type', 'null', 'primary_key', 'table', 'default']

        findex.drop_tables()

    @record
    def test_initial_tenant_load(self):
        '''JANUARY IS DIFFERENT'''

        findex.build_index_runner()
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        january_rent_roll_path = rent_roll_list[0][3]

        assert january_rent_roll_path == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_01_2022.xls'

        '''init is almost half of business logic'''
        nt_list, total_tenant_charges, explicit_move_outs = populate.init_tenant_load(filename=january_rent_roll_path, date='2022-01')

        # sheet side checks
        assert len(nt_list) == 64
        assert total_tenant_charges == 15469.0
        assert explicit_move_outs == []

        # db side checks
        assert len(Tenant.select()) == 64
        assert len(TenantRent.select()) == 64
        unit_list = [name for name in Unit.select().order_by(Unit.unit_name).namedtuples()]
        occupied_unit_count = Unit.select().count()
        assert occupied_unit_count == 64 

        '''test vacants after init tenant load'''
        vacant_units = Unit.find_vacants()
        assert 'PT-201' and 'CD-115' and 'CD-101' in vacant_units

        '''load initial balances at 01012022'''
        dir_items = [item.name for item in path.iterdir()]
        target_balance_file = path.joinpath(target_bal_load_file)
        populate.balance_load(filename=target_balance_file)

        '''test that balances loaded okay'''
        jan_end_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).get().sum
        assert jan_end_bal_sum == 793


    def test_load_remaining_months_rent(self):
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        paths_except_jan = rent_roll_list[1:]
        processed_rentr_dates_and_paths = [(item[1], item[3]) for item in paths_except_jan]
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
                # dt_obj_first, dt_obj_last = populate.make_first_and_last_dates(date_str=date)
                total_rent_charges = populate.get_total_rent_charges_by_month(first_dt=first_dt, last_dt=last_dt)
                assert total_rent_charges == 15972.0 

                vacant_snapshot_loop_end = Unit.find_vacants()
                assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115', 'PT-211'])
    

    def test_real_payments(self):
        records = findex.ventilate_table()
        file_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'deposits' and item['status'] == 'processed']
        
        processed_dates_and_paths = [(item[1], item[3]) for item in file_list]
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

    def test_load_nt_payments_and_type(self):
        test_date = '2022-01'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        ntp = [item for item in NTPayment.select().where(NTPayment.date_posted <= last_dt).namedtuples()]
        assert ntp[0].amount == '501.71'
        assert ntp[0].payee == 'laundry cd'

        test_date = '2022-02'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        ntp = [item for item in NTPayment.select().
        where(NTPayment.date_posted >= first_dt).
        where(NTPayment.date_posted <= last_dt).namedtuples()]
        assert ntp[0].amount == '700.0'
        assert ntp[1].payee == 'laundry pt'

    def test_load_damages(self):
        Damages.load_damages()
        assert [row.tenant.tenant_name for row in Damages().select()][0] == 'morris, michael'
   
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

        '''ideas: 
        1. I can make mi list by adding mi column to Tenant
        '''
   
        '''former tenants'''
        # is thomas johnson marked as inactive at end of loop
        former_tenants = [row.tenant_name for row in Tenant.select().where(Tenant.active=='False')]
        assert 'johnson, thomas' in [row for row in former_tenants]

        '''active tenants'''
        current_tenants = [row.tenant_name for row in Tenant.select().where(Tenant.active=='True')]
        assert len(current_tenants) == 64
        assert 'greiner, richard' in [row for row in current_tenants]        
        
        '''vacant units'''
        vacant_snapshot_loop_end = Unit.find_vacants()
        assert sorted(vacant_snapshot_loop_end) == sorted(['CD-101', 'CD-115', 'PT-211'])

        '''occupied units'''
        occupied_loop_end = [unit for unit in Unit().select()]
        assert len(occupied_loop_end) == 64

        '''unit accounting equals 67'''
        assert len(occupied_loop_end) + len(vacant_snapshot_loop_end) == 67

        '''all charges'''
        total_rent_charges_ytd = sum([float(row.rent_amount) for row in Tenant.select(Tenant.tenant_name, TenantRent.rent_amount).join(TenantRent).namedtuples()])
        assert total_rent_charges_ytd == 47409.0    

        '''all payments'''
        total_payments_ytd = sum([float(row.amount) for row in Payment.select(Payment.amount).namedtuples()])
        assert total_payments_ytd == 46686.0

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
        assert len(tenant_activity_recordtype) == 65

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

    def test_db_backup(self):
        
        DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)
        match_bool = DBUtils.find_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

        assert match_bool == True
        
@pytest.mark.testing_db
class TestOpcash:

    def test_opcash_load(self):
        records = findex.ventilate_table()
        file_list = [(item['fn'], item['period'], item['status'], item['path'], item['hap'], item['rr'], item['depsum'], item['dep_list']) for item in records if item['fn'].split('_')[1] == 'cash' and item['status'] == 'processed']

        populate.transfer_opcash_to_db(file_list=file_list)

        test_date = '2022-01'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        iter1 = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)

        iter2 = populate.get_opcashdetail_by_stmt(stmt_key=iter1[0][0])

        assert iter1 == [('op_cash_2022_01.pdf', datetime.date(2022, 1, 1), '15576.54', '30990.0', '15491.71')]

        assert iter2[0].id == 1

        test_date = '2022-02'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)
        iter1 = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)

        iter2 = populate.get_opcashdetail_by_stmt(stmt_key=iter1[0][0])
        
        assert iter1 == [('op_cash_2022_02.pdf', datetime.date(2022, 2, 1), '0.0', '31739.0', '15931.3')]

        assert iter2[0].id == 7

        test_date = '2022-03'
        first_dt, last_dt = populate.make_first_and_last_dates(date_str=test_date)

        iter1 = populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)

        iter2 = populate.get_opcashdetail_by_stmt(stmt_key=iter1[0][0])
        
        assert iter1 == [('op_cash_2022_03.pdf', datetime.date(2022, 3, 1), '3950.91', '38672.0', '16778.95')]

        assert iter2[0].id == 13
    
    def test_close_db(self):
        if db.is_closed() == False:
            db.close()

@pytest.mark.testing_db
class TestBuild:
    '''what do we want this to look like that '''
    basedir = os.path.abspath(os.path.dirname(__file__))
    build = BuildRS(path=path, main_db=db, findex_db=findex_db, findex_tablename=findex_tablename)
    build.new_auto_build()
    build.summary_assertion_at_period(test_date='2022-03')
        
        # breakpoint()

        # class Operation
        # class SubsidyRent(BaseModel):
        #     pass

        # class ContractRent(BaseModel):
        #     pass





        
        



        