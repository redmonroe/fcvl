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
GENERATED_RR_FILE = 'TEST_RENTROLL.xls'


@pytest.mark.unit_test_findexer
class TestFileIndexer:
    
    def test_setup(self):

        # remove generated rr file from path
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
            

        assert rr_test_val == True
            # findex.directory_contents.append(item)

    def build_index(self):
        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

    # def test_sort_directory(self):
    #     findex.build_index_runner()
    #     assert findex.index_dict == {'op_cash_2022_01.pdf': 'pdf', 'savings_2022_01.pdf': 'pdf', 'sd_2022_01.pdf': 'pdf', 'TEST_deposits_01_2022.xls': 'xls', 'TEST_rent_roll_01_2022.xls': 'xls'}