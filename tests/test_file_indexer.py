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