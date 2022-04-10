import pytest
import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from config import Config
from file_indexer import FileIndexer
from checklist import Checklist
from backend import db, PopulateTable, Tenant, Unit, Payment
from peewee import JOIN, fn

create_tables_list = [Tenant, Unit, Payment]

# target_tenant_load_file = 'rent_roll_01_2022.xls'
target_bal_load_file = 'beginning_balance_2022.xlsx'
target_pay_load_file = 'sample_payment_2022.xlsx'
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
        assert db.get_tables() == ['payment', 'tenant', 'unit']
        # assert db.get_columns(table='tenant')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

        # assert db.get_columns(table='unit')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

    def test_load_rent_roll_from_real_sheet(self):
        findex.build_index_runner()
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        # try load one month of rent_roll from sheets
        january_rent_roll_path = rent_roll_list[0][3]
        assert january_rent_roll_path == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_01_2022.xls'
        populate.basic_load(filename=january_rent_roll_path, mode='execute')  
   
    def test_load_tables(self):
        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

        dir_items = [item.name for item in path.iterdir()]
        assert target_bal_load_file in dir_items

        target_balance_file = path.joinpath(target_bal_load_file)
        target_payment_file = path.joinpath(target_pay_load_file)

        populate.balance_load(filename=target_balance_file)
        populate.payment_load_simple(filename=target_payment_file)

    def test_query_tables(self):
        ten_list = Tenant.select().order_by(Tenant.tenant_name).namedtuples()
        unpacked_tenants = [name for name in ten_list]
        
        assert unpacked_tenants[0].tenant_name == 'alexander, charles'

        ten_count = Tenant.select().count()
        assert ten_count == 64

        unit_list = Unit.select().order_by(Unit.unit_name).namedtuples()
        unit_list = [name for name in unit_list]
        occupied_unit_count = Unit.select().count()
        assert occupied_unit_count == 64 

    def test_charles_alexander(self):
        # get unit for single tenant
        query = Unit.select().join(Tenant).where(Tenant.tenant_name == 'alexander, charles').namedtuples()
        alexanders_row1 = [name for name in query]
        assert alexanders_row1[0].unit_name == 'PT-204'
        
        # get all TENANT cols for single tenant
        alexander = [(name.tenant_name, name.active, name.beg_bal_date, name.beg_bal_amount) for name in Tenant.select().where(Tenant.tenant_name == 'alexander, charles').namedtuples()]
        assert alexander == [('alexander, charles', 'True', datetime.date(2022, 1, 1), Decimal('-91'))]
       
        # get all cols for single tenant (except Payment)
        query = Tenant.get(Tenant.tenant_name == 'alexander, charles')
        for unit1 in query.unit:
            row = (query.tenant_name, unit1.unit_name, query.beg_bal_amount, query.beg_bal_date)
        assert row == ('alexander, charles', 'PT-204', Decimal('-91'), datetime.date(2022, 1, 1))

        # get all payments for a single tenant
        payments = Payment().select().join(Tenant).where(Tenant.tenant_name == 'alexander, charles')
        end_bal = [(name.payment_date, name.payment_amount) for name in payments]
        assert end_bal == [(datetime.date(2022, 1, 5), Decimal('200.20')), (datetime.date(2022, 1, 6), Decimal('100')), (datetime.date(2022, 2, 5), Decimal('250'))]

        # get sum of payments for a single tenant
        payments = Payment().select().join(Tenant).where(Tenant.tenant_name == 'alexander, charles')
        sum_payments = sum([name.payment_amount for name in payments])
        assert sum_payments == Decimal('550.20')

        # get lifetime balance for a single tenant
        startbal = row[2]
        current_bal = startbal + sum_payments
        assert current_bal == Decimal('459.20')

    def test_find_vacants(self):
        vacant_units = Unit.find_vacants()
        assert 'PT-201' and 'CD-115' and 'CD-101' in vacant_units

    def test_multiple_tenants(self):
        # get unit for multiple tenants
        query = Unit.select().join(Tenant).namedtuples()
        multi_row1 = [name for name in query]
        woods_row1 = multi_row1[0]
        assert woods_row1.tenant == 'woods, leon'
        assert woods_row1.status == 'occupied'
        gil_row1 = multi_row1[1]
        assert gil_row1.tenant == 'gillespie, janet'
        assert gil_row1.unit_name == 'CD-B'
        
        # get all TENANT cols for single tenant
        many_rows = [(name.tenant_name, name.beg_bal_amount) for name in Tenant.select().namedtuples()]
        assert many_rows[0][0] == 'woods, leon'
        assert many_rows[0][1] == Decimal('18')
        
        # get all cols for multipe tenants (except Payment)
        all_rows = []
        for tow in Tenant.select(Tenant.tenant_name, Tenant.beg_bal_amount, Unit.unit_name).join(Unit).namedtuples():
            row = (tow.tenant_name, tow.beg_bal_amount, tow.unit_name) 
            all_rows.append(row) 
        assert all_rows[-1] == ('graves, renee', Decimal('38'), 'PT-212')
        
        # get all payments and sums for multiple tenants
        sum_payment_list = list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
            Tenant.tenant_name, 
            Tenant.beg_bal_amount, 
            Payment.payment_amount, 
            fn.SUM(Payment.payment_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')
            ).join(Payment).namedtuples()]))

        # generate end_bals for multiple tenants
        end_bal_list = [(rec[0], rec[1] - Decimal(rec[2]).quantize(Decimal('.01'), rounding=ROUND_UP)) for rec in sum_payment_list]

        end_bal_list_no_dec = [(rec[0], float(rec[1]) - rec[2]) for rec in sum_payment_list]

        assert ('gillespie, janet', float('-152.0')) and ('alexander, charles', float('-641.2')) in end_bal_list_no_dec
       
    
    def test_ranges(self):
        # sum if in date range

        # get all payments in Jan 2022 as list
        jan_payments = [rec for rec in Payment().select().where(Payment.payment_date >= datetime.date(2022, 1, 1)).where(Payment.payment_date <= datetime.date(2022, 1, 31)).namedtuples()]

        assert len(jan_payments) == 2
        assert jan_payments[0].id == 1
        assert jan_payments[0].tenant == 'alexander, charles'

        month_list = [datetime.date(datetime.date.today().year, month, 1) for month in range(1, 13)]

        # get all payments in Jan 2022 as sum(in a list with tenant_name)
        sum_payment_list_jan = list(set([(rec.tenant_name, rec.beg_bal_amount, rec.total_payments) for rec in Tenant.select(
            Tenant.tenant_name, 
            Tenant.beg_bal_amount, 
            Payment.payment_amount, 
            fn.SUM(Payment.payment_amount).over(partition_by=[Tenant.tenant_name]).alias('total_payments')).
            where(Payment.payment_date >= datetime.date(2022, 1, 1)).
            where(Payment.payment_date <= datetime.date(2022, 1, 31)).
            join(Payment).namedtuples()]))

        assert sum_payment_list_jan == [('alexander, charles', Decimal('-91'), 300.2)]

    def test_compare_feb_rent_roll(self):
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        # try to compare jan(from db) and feb(from rent roll)
        feb_rent_roll_path = rent_roll_list[1][3]
        assert feb_rent_roll_path == '/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources/rent_roll_02_2022.xlsx'

        jan_tenant_from_db = set([name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()])
        assert len(jan_tenant_from_db) == 64
        
        rent_roll_dict_feb = populate.basic_load(filename=feb_rent_roll_path, mode='return_only') 
        feb_tenant_from_sheet = set([name for name in rent_roll_dict_feb.keys()])
        assert len(feb_tenant_from_sheet) == 65

        # this should be moved to backend: last months - this month
        feb_move_ins = list(feb_tenant_from_sheet - jan_tenant_from_db) # catches move in
        feb_move_outs = list(jan_tenant_from_db - feb_tenant_from_sheet) # catches move out

        assert len(feb_move_ins) == 1
        assert len(feb_move_outs) == 0
        assert isinstance(feb_move_outs, list) == True
        assert isinstance(feb_move_ins, list) == True

        populate.insert_move_ins(move_ins=feb_move_ins)

        feb_tenant_from_db = [name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()]
        assert len(feb_tenant_from_db) == 65
        assert 'greiner, richard' in feb_tenant_from_db

    @pytest.mark.testing_db_loop
    def test_reset_test_for_looping(self):
        db.drop_tables(models=create_tables_list)
        db.create_tables(create_tables_list)
        findex.drop_tables()
        findex.build_index_runner()
    
    @pytest.mark.testing_db_loop
    def test_compare_rent_rolls_loop(self):
        records = findex.ventilate_table()
        rent_roll_list = [(item['fn'], item['period'], item['status'], item['path']) for item in records if item['fn'].split('_')[0] == 'rent' and item['status'] == 'processed']

        processed_rentr_dates_and_paths = [(item[1], item[3]) for item in rent_roll_list]
        processed_rentr_dates_and_paths.sort()

        for date, path in processed_rentr_dates_and_paths:
            # get initial tenant list in iter period
            period_start_tenant_names = set([name.tenant_name for name in Tenant.select().where(Tenant.active==True).namedtuples()])
            # get end tenant list in iter period
            if date == '2022-01': # skip compare on init month
                rent_roll_dict = populate.basic_load(filename=path, mode='execute')
            else: 
                rent_roll_dict = populate.basic_load(filename=path, mode='return_only')
            dipstick = (date, 'start:', len(period_start_tenant_names), 'end:', len(rent_roll_dict), path)
            rent_roll_set = set([name for name in rent_roll_dict.keys()])
            if date != '2022-01': # this is the main loop
                mis, mos = populate.find_mi_and_mo(start_set=period_start_tenant_names,end_set=rent_roll_set)
                populate.insert_move_ins(move_ins=mis)
                populate.deactivate_move_outs(move_outs=mos)
                # are move-ins active??q

            if date == '2022-02':
                assert 'johnson, thomas' in rent_roll_dict.keys()
                assert len(rent_roll_dict) == 65

            if date == '2022-03':
                assert 'johnson, thomas' not in rent_roll_dict.keys()
                assert len(rent_roll_dict) == 64
                all_rows = [(tow.tenant_name, tow.active, tow.beg_bal_amount, tow.unit_name) for tow in Tenant.select(Tenant.tenant_name, Tenant.active, Tenant.beg_bal_amount, Unit.unit_name).join(Unit).namedtuples()]

                tj_row = [row for row in all_rows if row[0] == 'johnson, thomas'][0]
                assert tj_row[1] == 'False'
    
        
    @pytest.mark.testing_db_loop
    def test_init_balance_reload(self):
        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

        dir_items = [item.name for item in path.iterdir()]
        assert target_bal_load_file in dir_items

        target_balance_file = path.joinpath(target_bal_load_file)
    #     target_payment_file = path.joinpath(target_pay_load_file)

        populate.balance_load(filename=target_balance_file)
    #     populate.payment_load_simple(filename=target_payment_file)

        ''' this is state of balance at start of jan 2022, so tj should be in it'''
        sum_beg_bal_all = [row.beg_bal_amount for row in Tenant.select(Tenant.beg_bal_amount).namedtuples()] 
        summary_total = float(sum(sum_beg_bal_all))
        assert summary_total == 793.0
        
        ''' this is state of balance at start at end of loop(march 2020), so tj should not be in it'''
        sum_beg_bal_all = [row.beg_bal_amount for row in Tenant.select(Tenant.active, Tenant.beg_bal_amount).where(Tenant.active=='True').namedtuples()] 
        summary_total = float(sum(sum_beg_bal_all))
        assert summary_total == 795.0
        # all_rows = [(tow.tenant_name, tow.active, tow.beg_bal_amount, tow.unit_name) for tow in Tenant.select(Tenant.tenant_name, Tenant.active, Tenant.beg_bal_amount, Unit.unit_name).join(Unit).where(Tenant.active=='True').namedtuples()]

        breakpoint()
        # assert all_rows[-1] == ('graves, renee', Decimal('38'), 'PT-212')

        
    # @pytest.mark.testing_db_loop
    # def test_loop_deposit_intake(self):
    #     breakpoint()
        # need to reload other tables after I dropped them in above func
        # could also do a check on vacants: vacants as of when??
        # what about transfers?
        
        # do realistic month of payments load
        # load real beginning balances at 01/2022
        # do charges class
        # damages and other charges list



        
        



        