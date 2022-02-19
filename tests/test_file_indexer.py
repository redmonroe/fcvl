import os
from config import Config
from pathlib import Path
import shutil
import pytest
from file_indexer import FileIndexer
import pdb

path = Config.TEST_RS_PATH
discard_pile = Config.TEST_MOVE_PATH
findex = FileIndexer(path=path, discard_pile=discard_pile)
TEST_RR_FILE = 'TEST_rent_roll_01_2022.xls'
TEST_DEP_FILE = 'TEST_deposits_01_2022.xls'
GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'
GENERATED_DEP_FILE = 'TEST_DEP_012022.xls'


@pytest.mark.unit_test_findexer
class TestFileIndexer:

    test_message = 'hi'
    path_contents = []

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

    def test_build_index(self):
        findex.build_index()

    def test_teardown(self):
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_RR_FILE)
        TestFileIndexer.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_DEP_FILE)

        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_RR_FILE, target_dir=path)
        TestFileIndexer.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_DEP_FILE, target_dir=path)
        
        discard_contents = [count for count, file in enumerate(discard_pile.iterdir())]
        path_contents = [count for count, file in enumerate(path.iterdir())]

        assert len(discard_contents) == 0
        assert len(path_contents) == 5  