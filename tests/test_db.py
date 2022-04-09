import pytest
from pathlib import Path
from config import Config
from file_indexer import FileIndexer
from backend import db, Tenant, Unit

create_tables_list = [Tenant, Unit]

target_tenant_load_file = 'rent_roll_01_2022.xls'
path = Config.TEST_RS_PATH
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
        assert db.get_tables() == ['tenant', 'unit']
        # assert db.get_columns(table='tenant')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

        # assert db.get_columns(table='unit')[0]._asdict() == {'name': 'id', 'data_type': 'INTEGER', 'null': False, 'primary_key': True, 'table': 'tenant', 'default': None}

    def test_load_tables(self):

        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

        dir_items = [item.name for item in path.iterdir()]
        assert target_tenant_load_file in dir_items

        target_tenant_file = path.joinpath(target_tenant_load_file)

        tenant_list = tenant.load_tenants(filename=target_tenant_file)
        unit_list = unit.load_units(filename=target_tenant_file, verbose=False)       

    def test_query_tables(self):
        ten_list = Tenant.select().order_by(Tenant.tenant_name).namedtuples()
        unpacked_tenants = [name for name in ten_list]
        breakpoint()
        assert unpacked_tenants[0].tenant_name == 'Alexander, charles'

        ten_count = Tenant.select().count()
        assert ten_count == 64

        unit_list = Unit.select().order_by(Unit.unit_name).namedtuples()
        unit_list = [name for name in unit_list]
        unit_count = Unit.select().count()
        assert unit_count == 67 

    def test_join(self):

        breakpoint()

        