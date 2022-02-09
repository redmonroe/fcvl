from config import Config
from pathlib import Path
import pytest
from file_indexer import FileIndexer

path = Config.TEST_RS_PATH

@pytest.mark.unit_test_findexer
class TestFileIndexer:
    
    def test_setup(self):
        pass

    def build_index(self):
        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

    def test_sort_directory(self):
        findex = FileIndexer(path=path)
        findex.build_index_runner()
        assert findex.index_dict == {'op_cash_2022_01.pdf': 'pdf', 'savings_2022_01.pdf': 'pdf', 'sd_2022_01.pdf': 'pdf', 'TEST_deposits_01_2022.xls': 'xls', 'TEST_rent_roll_01_2022.xls': 'xls'}