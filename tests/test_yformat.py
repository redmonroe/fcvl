import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from auth_work import oauth
from setup_year import YearSheet
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch

monkeypatch = MonkeyPatch()

class TestYearSheet():

    def test_init(self):
        service = oauth(Config.my_scopes, 'sheet', mode='testing')
        ys = YearSheet(full_sheet=Config.TEST_RS, mode='testing', test_service=service)
        assert ys.test_message == 'hi_from_year_sheets!'

if __name__ == '__main__':
    test_ys = TestYearSheet()
