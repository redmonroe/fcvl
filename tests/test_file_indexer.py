import os
from config import Config
from pathlib import Path
import shutil
import pytest
from file_indexer import FileIndexer

path = Config.TEST_RS_PATH
discard_pile = Config.TEST_MOVE_PATH
findex = FileIndexer(path=path, discard_pile=discard_pile)
TEST_RR_FILE = 'TEST_rent_roll_01_2022.xls'
GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'


@pytest.mark.unit_test_findexer
class TestFileIndexer:
    
    def test_setup(self):

        # remove generated rr file from dir
        try:
            os.remove(os.path.join(str(path), GENERATED_RR_FILE))
        except FileNotFoundError as e:
            print(e, 'TEST_RR NOT found in test_data_repository, make sure you are looking for the right name')

        # move original rr file back TO path
        findex.get_file_names_kw(discard_pile)
        for item in findex.test_list:
            if item == TEST_RR_FILE:
                try:
                    shutil.move(os.path.join(str(discard_pile), item), path)
                except:
                    print('Error occurred copying file: jw')

        # check discard_pile is empty
        discard_contents = [count for count, file in enumerate(discard_pile.iterdir())]
        # check path pile is 5
        path_contents = [count for count, file in enumerate(path.iterdir())]


        assert len(discard_contents) == 0
        assert len(path_contents) == 5

    def test_rent_roll_flow(self):
        findex = FileIndexer(path=Config.TEST_RS_PATH)
        findex.build_index_runner()

        path_contents = []
        for item in path.iterdir():
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            path_contents.append(filename) 

        assert GENERATED_RR_FILE in path_contents
        assert 1 in path_contents

        # assert that 

    # def test_sort_directory(self):
    #     findex.build_index_runner()
    #     assert findex.index_dict == {'op_cash_2022_01.pdf': 'pdf', 'savings_2022_01.pdf': 'pdf', 'sd_2022_01.pdf': 'pdf', 'TEST_deposits_01_2022.xls': 'xls', 'TEST_rent_roll_01_2022.xls': 'xls'}