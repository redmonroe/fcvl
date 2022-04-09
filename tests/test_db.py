import pytest
from decimal import Decimal
from pathlib import Path
from config import Config
from file_indexer import FileIndexer
from backend import db, PopulateTable, Tenant, Unit, BeginningBalance
from peewee import JOIN

create_tables_list = [Tenant, Unit, BeginningBalance]

target_tenant_load_file = 'rent_roll_01_2022.xls'
target_bal_load_file = 'beginning_balance_2022.xlsx'
path = Config.TEST_RS_PATH
populate = PopulateTable()
tenant = Tenant()
unit = Unit()
findex = FileIndexer(path=path)

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
        assert db.get_tables() == ['beginningbalance', 'tenant', 'unit']
        # assert db.get_columns(table='tenant')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

        # assert db.get_columns(table='unit')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

    def test_load_tables(self):

        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

        dir_items = [item.name for item in path.iterdir()]
        assert target_tenant_load_file in dir_items
        assert target_bal_load_file in dir_items

        target_tenant_file = path.joinpath(target_tenant_load_file)
        target_balance_file = path.joinpath(target_bal_load_file)

        populate.basic_load(filename=target_tenant_file)  
        populate.balance_load(filename=target_balance_file)

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
        # from UNIT perspective
        query = Unit.select().join(Tenant).where(Tenant.tenant_name == 'alexander, charles').namedtuples()
        alexanders_row1 = [name for name in query]
        assert alexanders_row1[0].unit_name == 'PT-204'
        
        query = BeginningBalance().select().join(Tenant).where(Tenant.tenant_name == 'alexander, charles').namedtuples()
        alexanders_row2 = [name for name in query]

        query = Tenant.get(Tenant.tenant_name == 'alexander, charles')
        for unit1, bb in zip(query.unit, query.beg_bal):
            row = (query.tenant_name, unit1.unit_name, bb.beg_bal_amount)

        assert row == ('alexander, charles', 'PT-204', Decimal('-91'))


        # query = (Tenant
        #  .select(Tenant.tenant_name)
        #  .join(Unit, JOIN.LEFT_OUTER)  # Joins user -> tweet.
        #  .join(BeginningBalance(), JOIN.LEFT_OUTER)  # Joins tweet -> favorite.
        #  .group_by(Tenant.tenant_name))
        

        breakpoint()     
        # get from backref unit and beg_bal


        