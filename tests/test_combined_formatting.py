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

        # removal all sheets but 1
        while len(titles_dict) > 1:
            print(titles_dict)
            for name, id2 in titles_dict.items():
                calls.del_one_sheet(service, test_workbook, id2)
        
        titles_dict = Utils.get_existing_sheets(service, test_workbook)

        if list(titles_dict.keys())[0] == 'intake':
            print('only remaining sheet is named intake')
        else:
            calls.make_one_sheet(service, test_workbook, 'intake')
            calls.del_one_sheet(service, test_workbook, list(titles_dict.values())[0])

        # rename existing to intake
        # print(titles_dict.values())
            

        assert len(titles_dict) == 1
        ## if workbook has entries delete them: BE CAREFUL

if __name__ == '__main__':
    test = TestSheetFormat()
    test.test_setup()