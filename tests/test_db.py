import calendar
import datetime
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path

import pytest
from backend import Payment, PopulateTable, Tenant, Unit, NTPayment, TenantRent, db
from checklist import Checklist
from config import Config
from file_indexer import FileIndexer
from peewee import JOIN, fn

create_tables_list = [Tenant, Unit, Payment, NTPayment, TenantRent]

# target_tenant_load_file = 'rent_roll_01_2022.xls'
target_bal_load_file = 'beginning_balance_2022.xlsx'
target_pay_load_file = 'sample_payment_2022.xlsx'
sleep1 = 0
path = Config.TEST_RS_PATH
findex_db = Config.test_findex_db
findex_tablename = Config.test_findex_name
checkl_db = Config. test_checklist_db 
checkl_tablename = Config.test_checklist_name
populate = PopulateTable()
tenant = Tenant()
unit = Unit()
checkl = Checklist(checkl_db, checkl_tablename)
findex = FileIndexer(path=path, db=findex_db, table=findex_tablename, checklist_obj=checkl)
init_cutoff_date = '2022-01'

@pytest.mark.testing_db
class TestDB:

    test_message = 'hi'

    def test_init(self):
        assert self.test_message == 'hi'

    def test_db(self):
        db.connect()
        db.drop_tables(models=create_tables_list)
        db.create_tables(create_tables_list)
        assert db.database == '/home/joe/local_dev_projects/fcvl/sqlite/test_pw_db.db'
        assert sorted(db.get_tables()) == sorted(['tenantrent', 'ntpayment', 'payment', 'tenant', 'unit'])
        assert [*db.get_columns(table='payment')[0]._asdict().keys()] == ['name', 'data_type', 'null', 'primary_key', 'table', 'default']

    def test_initial_tenant_load(self):
        '''JANUARY IS DIFFERENT'''
        '''we know this at least has to be correct'''
        '''get db state at end of jan to assert'''

        findex.build_index_runner()
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        # try load one month of rent_roll from sheets
        january_rent_roll_path = rent_roll_list[0][3]

        assert january_rent_roll_path == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_01_2022.xls'

        '''DEFINE FUNCTION INIT_LOAD: DO NOT WRAP IT IN TOO MANY FUNC LAYERS'''

        nt_list, total_tenant_charges, explicit_move_outs = populate.init_tenant_load(filename=january_rent_roll_path, date='2022-01')
        breakpoint()

        # sheet side checks
        assert len(nt_list) == 64
        assert total_tenant_charges == 15469.0
        assert explicit_move_outs == []

        # db side checks
        assert len(Tenant.select()) == 64
        assert len(TenantRent.select()) == 64
        unit_list = Unit.select().order_by(Unit.unit_name).namedtuples()
        unit_list = [name for name in unit_list]
        occupied_unit_count = Unit.select().count()
        assert occupied_unit_count == 64 
        vacant_units = Unit.find_vacants()
        assert 'PT-201' and 'CD-115' and 'CD-101' in vacant_units

        beg_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).get().sum
        assert beg_bal_sum == 0

        # now load beginning balances from sheet
        dir_items = [item.name for item in path.iterdir()]
        target_balance_file = path.joinpath(target_bal_load_file)

        populate.balance_load(filename=target_balance_file)

        # test loaded beginning balances
        jan_end_bal_sum = Tenant.select(fn.Sum(Tenant.beg_bal_amount).alias('sum')).get().sum
        assert jan_end_bal_sum == 793

    def test_load_remaining_months_rent(self):
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        paths_except_jan = rent_roll_list[1:]
        processed_rentr_dates_and_paths = [(item[1], item[3]) for item in paths_except_jan]
        processed_rentr_dates_and_paths.sort()

        for date, filename in processed_rentr_dates_and_paths:
            populate.after_jan_load(filename=filename, date=date)
            






    # def test_compare_feb_rent_roll(self):
    #     records = findex.ventilate_table()
    #     rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

    #     # try to compare jan(from db) and feb(from rent roll)
    #     feb_rent_roll_path = rent_roll_list[1][3]
    #     assert feb_rent_roll_path == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_02_2022.xlsx'

    #     jan_tenant_from_db = set([name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()])
    #     assert len(jan_tenant_from_db) == 64
     
    #     nt_list = populate.basic_load(filename=feb_rent_roll_path, mode='return_only', date='2022-02') 
    #     feb_tenant_from_sheet = set([row.name for row in nt_list])
    #     assert 65 == len(feb_tenant_from_sheet)

    #     # this should be moved to backend: last months - this month
    #     feb_move_ins = list(feb_tenant_from_sheet - jan_tenant_from_db) # catches move in
    #     feb_move_outs = list(jan_tenant_from_db - feb_tenant_from_sheet) # catches move out

    #     assert len(feb_move_ins) == 1
    #     assert len(feb_move_outs) == 0
    #     assert isinstance(feb_move_outs, list) == True
    #     assert isinstance(feb_move_ins, list) == True

    #     populate.insert_move_ins(move_ins=feb_move_ins)

    #     feb_tenant_from_db = [name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()]
    #     assert len(feb_tenant_from_db) == 65
    #     assert 'greiner, richard' in feb_tenant_from_db     

    # def test_reset_test_for_looping(self):
    #     db.drop_tables(models=create_tables_list)
    #     db.create_tables(create_tables_list)
    #     findex.drop_tables()
    #     findex.build_index_runner()
    
    # def test_compare_rent_rolls_loop(self):
    #     records = findex.ventilate_table()
    #     rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

    #     processed_rentr_dates_and_paths = [(item[1], item[3]) for item in rent_roll_list]
    #     processed_rentr_dates_and_paths.sort()

    #     for date, path in processed_rentr_dates_and_paths:

    #         nt_list, rent_roll_set, period_start_tenant_names = populate.rent_roll_load_wrapper(path=path, date=date)

    #         if date == '2022-01':
            
    #             dt_obj_first, dt_obj_last = populate.make_first_and_last_dates(date_str=date)
    #             total_collections = populate.get_total_collections_by_month(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)


    #             assert total_collections == 15469.0

    #         if date == '2022-02':
    #             # breakpoint()
    #             # breakpoint()
    #             dt_obj_first, dt_obj_last = populate.make_first_and_last_dates(date_str=date)
    #             total_collections = populate.get_total_collections_by_month(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             # assert total_collections == 15469.0
    #             assert 'johnson, thomas' in [row.name for row in nt_list]
    #             assert len(nt_list) == 65

    #         if date == '2022-03':
    #             assert 'johnson, thomas' not in [row.name for row in nt_list]
    #             assert len(nt_list) == 64
    #             all_rows = [(tow.tenant_name, tow.active, tow.beg_bal_amount, tow.unit_name) for tow in Tenant.select(Tenant.tenant_name, Tenant.active, Tenant.beg_bal_amount, Unit.unit_name).join(Unit).namedtuples()]

    #             tj_row = [row for row in all_rows if row[0] == 'johnson, thomas'][0]
    #             assert tj_row[1] == 'False'
    
    # def test_init_balance_reload(self):
    #     assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')


    #     ''' this is state of balance at start of jan 2022, so tj should be in it'''
    #     sum_beg_bal_all = [row.beg_bal_amount for row in Tenant.select(Tenant.beg_bal_amount).namedtuples()] 
    #     summary_total = float(sum(sum_beg_bal_all))
    #     assert summary_total == 793.0

    #     ''' this is state of balance at end of loop(march 2020), so tj should not be in it'''
    #     sum_beg_bal_all = [row.beg_bal_amount for row in Tenant.select(Tenant.active, Tenant.beg_bal_amount).where(Tenant.active=='True').namedtuples()] 
    #     summary_total = float(sum(sum_beg_bal_all))
    #     assert summary_total == 795.0

    # def test_real_payments(self):
    #     records = findex.ventilate_table()
    #     file_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'deposits' and item['status'] == 'processed']
        
    #     processed_dates_and_paths = [(item[1], item[3]) for item in file_list]
    #     processed_dates_and_paths.sort()
        
    #     for date1, path in processed_dates_and_paths:
    #         grand_total, ntp, tenant_payment_df = populate.payment_load_full(filename=path)
    #         if date1 == '2022-01':
    #             # get first and last dates in date
    #             dt_obj_first, dt_obj_last = populate.make_first_and_last_dates(date_str=date1)

    #             # check db commited ten payments and ntp against df
    #             all_tp, all_ntp = populate.check_db_tp_and_ntp(grand_total=grand_total, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)      

    #             # get beginning balance by tenant and check for duplicate payments
    #             detail_beg_bal_all = populate.get_all_tenants_beg_bal()

    #             different_names = populate.check_for_multiple_payments(detail_beg_bal_all=detail_beg_bal_all, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             if len(different_names) > 0:
    #                 detail_one = [row for row in detail_beg_bal_all if row[0] == different_names[0]]
    #                 assert detail_one == [('yancy, claude', '279.00', Decimal('-9')), ('yancy, claude', '18.00', Decimal('-9'))]

    #             # check beg_bal_amount again
    #             beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='initial')
    #             assert beg_bal_sum_by_period == 795.0
    #             # check total tenant payments sum db-side
    #             # check total tenant payments from dataframe against what I committed to db
    #             tp_sum_by_period_db, tp_sum_by_period_df = populate.match_tp_db_to_df(df=tenant_payment_df, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             # sum tenant payments by tenant
    #             sum_payment_list = populate.get_sum_tp_by_tenant(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             yancy_jan = [row for row in sum_payment_list if row[0] == 'yancy, claude'][0]
    #             assert yancy_jan[2] == float(Decimal('297.00'))

    #             # check jan ending balances by tenant
    #             end_bal_list_no_dec = populate.get_end_bal_by_tenant(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             tj_row = [row for row in end_bal_list_no_dec if row[0] == 'johnson, thomas'][0]
    #             yancy_row = [row for row in end_bal_list_no_dec if row[0] == 'yancy, claude'][0]
    #             jack_row = [row for row in end_bal_list_no_dec if row[0] == 'davis, jacki'][0]
    #             assert tj_row == ('johnson, thomas', -162.0)
    #             assert yancy_row == ('yancy, claude', -306.0)
    #             assert jack_row == ('davis, jacki', -211.0)

    #         if date1 == '2022-02':
    #             dt_obj_first, dt_obj_last = populate.make_first_and_last_dates(date_str=date1)
    #             all_tp, all_ntp = populate.check_db_tp_and_ntp(grand_total=grand_total, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)           

    #             beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='other', dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             detail_beg_bal_all = populate.get_all_tenants_beg_bal()

    #             different_names = populate.check_for_multiple_payments(detail_beg_bal_all=detail_beg_bal_all, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)
           
    #             if len(different_names) > 0:
    #                 detail_one = [row for row in detail_beg_bal_all if row[0] == different_names[0]]
    #                 assert detail_one == [('coleman, william', '192.0', Decimal('-24')), ('coleman, william', '192.0', Decimal('-24'))]
      
    #             '''this doesn't work; not able to get beginning balance for february; wait till I put in rent & charges'''
    #             beg_bal_sum_by_period = populate.get_beg_bal_sum_by_period(style='initial')
    #             assert beg_bal_sum_by_period == 795.0

    #             tp_sum_by_period_db, tp_sum_by_period_df = populate.match_tp_db_to_df(df=tenant_payment_df, dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)
        
    #             sum_payment_list = populate.get_sum_tp_by_tenant(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             test_feb = [row for row in sum_payment_list if row[0] == 'coleman, william'][0]
    #             assert test_feb[2] == float(Decimal('384.00'))

    #             end_bal_list_no_dec = populate.get_end_bal_by_tenant(dt_obj_first=dt_obj_first, dt_obj_last=dt_obj_last)

    #             '''asserts for end_bal_list_no_dec no necessary until I put in rent & charges'''
                
    # def test_real_charges(self):
    #     records = findex.ventilate_table()
    #     rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

    #     processed_rentr_dates_and_paths = [(item[1], item[3]) for item in rent_roll_list]
    #     processed_rentr_dates_and_paths.sort()

        # for date, path in processed_rentr_dates_and_paths:
            # rent_roll_dict = populate.basic_load
        # final_charges = [charge for charge in TenantRent()]
        # period_start_tenant_names = set([name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()])

            # are move-ins active??q
        # class TenantRent(BaseModel):
        #     tenant = ForeignKeyField(Tenant, backref='rent')
        #     unit = ForeignKeyField(Unit, backref='rent')
        #     rent_amount = DecimalField(default=0.00)
        #     rent_date = DateField(default='2022-01-01')
        #     date_code = IntegerField()

        # class SubsidyRent(BaseModel):
        #     pass

        # class ContractRent(BaseModel):
        #     pass

        # class Damages(BaseModel):
        #     pass



        
        



        