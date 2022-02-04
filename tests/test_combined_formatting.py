import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from auth_work import oauth
from utils import Utils
from setup_year import YearSheet
from setup_month import MonthSheet
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch

test_workbook = Config.TEST_RS

class TestSheetFormat:
    
    def test_setup(self):
        service = oauth(Config.my_scopes, 'sheet', mode='testing')
        titles_dict = Utils.get_existing_sheets(service, test_workbook)
        calls = GoogleApiCalls()

        intake_ok = False
        for name, id2 in titles_dict.items():
            if name == 'intake':
                intake_ok = True
                break

        if intake_ok == False:
            calls.make_one_sheet(service, test_workbook, 'intake')
        
        # removal all sheets but intake
        for name, id2, in titles_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, test_workbook, id2)
       
        titles_dict = Utils.get_existing_sheets(service, test_workbook)

        assert len(titles_dict) == 1
        assert list(titles_dict.keys())[0] == 'intake'
        ## if workbook has entries delete them: BE CAREFUL

if __name__ == '__main__':
    test = TestSheetFormat()
    test.test_setup()