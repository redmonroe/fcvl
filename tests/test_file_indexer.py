import os
from config import Config
from db_utils import DBUtils
from pathlib import Path
import shutil
import pytest
from file_indexer import FileIndexer
import pdb
import dataset

path = Config.TEST_RS_PATH
discard_pile = Config.TEST_MOVE_PATH
findex = FileIndexer(path=path, discard_pile=discard_pile, db=Config.test_findex_db, table='findex')
TEST_RR_FILE = 'TEST_rent_roll_01_2022.xls'
TEST_DEP_FILE = 'TEST_deposits_01_2022.xls'
GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'
GENERATED_DEP_FILE = 'TEST_DEP_012022.xls'


@pytest.mark.unit_test_findexer
class TestFileIndexer:

    test_message = 'hi'
    path_contents = []
    db = None

    def remove_generated_file_from_dir(self, path1=None, file1=None):
        # pdb.set_trace()
        try:
            os.remove(os.path.join(str(path1), file1))
        except FileNotFoundError as e:
            print(e, f'{file1} NOT found in test_data_repository, make sure you are looking for the right name')
    
    def move_original_back_to_dir(self, discard_dir=None, target_file=None, target_dir=None):
        findex.get_file_names_kw(discard_dir)
        for item in findex.test_list:
            if item == target_file:
                try:
                    shutil.move(os.path.join(str(discard_dir), item), target_dir)
                except:
                    print('Error occurred copying file: jw')

    def make_path_contents(self, path=None):
        for item in path.iterdir():
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.path_contents.append(filename) 

    @pytest.mark.findex_db
    @pytest.fixture
    def setup_test_db(self):
        db = findex.db
        tablename = findex.tablename
        table = db[tablename]
        table.drop()
        check_tables = db.tables
        assert check_tables == []
        return db
    
    @pytest.mark.findex_db
    def test_build_index_preflight(self, setup_test_db):
        db = setup_test_db
        findex.build_index()
        tables = db.tables

        assert db == 1

    #     # assert tables == ['findex']
    #     # assert len(records_list) == 5
    #     # assert 'TEST_deposits_01_2022.xls' in records_list[3].values()

    def test_setup(self):
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_RR_FILE)
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_DEP_FILE)

        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_RR_FILE, target_dir=path)
        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_DEP_FILE, target_dir=path)

        # check discard_pile is empty
        discard_contents = [count for count, file1 in enumerate(discard_pile.iterdir())]
        # check path pile is 5
        path_contents1 = [count for count, file1 in enumerate(path.iterdir())]
        assert len(discard_contents) == 0
        assert len(path_contents1) == 5

    def test_rent_roll_flow(self):
        findex.build_index_runner()
        TestFileIndexer.make_path_contents(self, path=path)

        assert GENERATED_RR_FILE in self.path_contents

    def test_deposit_flow(self):
        TestFileIndexer.make_path_contents(self, path=path)        
        assert GENERATED_DEP_FILE in self.path_contents

    # @pytest.mark.findex_db
    # def test_build_index_preflight(self, setup_test_db):
    #     db = setup_test_db
    #     tables = db.tables
    #     records_list = [table for table in findex.db[tables[0]]]

    #     assert tables == ['findex']
    #     assert len(records_list) == 5
    #     assert GENERATED_DEP_FILE in records_list[3].values()


    def test_teardown(self):
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_RR_FILE)
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_DEP_FILE)

        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_RR_FILE, target_dir=path)
        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_DEP_FILE, target_dir=path)
        
        discard_contents = [count for count, file in enumerate(discard_pile.iterdir())]
        path_contents = [count for count, file in enumerate(path.iterdir())]

        assert len(discard_contents) == 0
        assert len(path_contents) == 5  