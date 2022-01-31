import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from auth_work import oauth
from setup_month import MonthSheet
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch

monkeypatch = MonkeyPatch()

class TestMonthSheet():

    def test_init(self):
        ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing')
        assert ms.test_message == 'hi'

    def test_push_to_intake(self, monkeypatch):
        service = oauth(Config.my_scopes, 'sheet', mode='testing')

        ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing', test_service=service)
        
        calls = GoogleApiCalls()
        calls.clear_sheet(service, Config.TEST_RS, f'{ms.ui_sheet}!A1:ZZ100')

        choice1 = 2
        choice2 = 1
        answers = iter([choice1, choice2])
         # using lambda statement for mocking
        monkeypatch.setattr('builtins.input', lambda name: next(answers))
        ms.push_to_intake()
        result = calls.broad_get(service, Config.TEST_RS, f'{ms.ui_sheet}!A1:A1')
        print(result)
        assert result[0][0] == 'CD-A'        

if __name__ == '__main__':
    test_ms = TestMonthSheet()
    test_ms.test_push_to_intake(monkeypatch)