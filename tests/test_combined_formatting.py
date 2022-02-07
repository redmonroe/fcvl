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
test_path = Config.TEST_RS_PATH
monkeypatch = MonkeyPatch()

# removal all but intake
# then make month sheets from partial test list
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()

class TestSheetFormat:
    
    def test_setup_intake(self):
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

    def test_setup_make_month_sheets(self, monkeypatch):
        ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)

        choice1 = 2
        answers = iter([choice1])
        monkeypatch.setattr('builtins.input', lambda name: next(answers))
        ys.set_user_choice()
        ys.control()
        titles_dict = Utils().get_existing_sheets(service,test_workbook)
        assert len(titles_dict) == 4
    
    def test_year_format(self):
        ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)
        choice1 = 3
        answers = iter([choice1])
        monkeypatch.setattr('builtins.input', lambda name: next(answers))
        ys.set_user_choice()
        ys.control()
        assert ys.calls == 1


    def test_teardown_month_sheets(self):
        ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)
        titles_dict = Utils().get_existing_sheets(service,test_workbook)
        for name, id2, in titles_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, test_workbook, id2)

        titles_dict1 = Utils().get_existing_sheets(service,test_workbook)        
        
        assert len(titles_dict1) == 1


        # assert dir(ys) == 1

    '''MonthSheet'''

    # def test_push_to_intake(self, monkeypatch):
    #     service = oauth(Config.my_scopes, 'sheet', mode='testing')

    #     ms = MonthSheet(full_sheet=test_workbook, path=test_path, mode='testing', test_service=service)
        
    #     calls = GoogleApiCalls()
    #     calls.clear_sheet(service, test_workbook, f'{ms.ui_sheet}!A1:ZZ100')

    #     choice1 = 2
    #     choice2 = 1
    #     answers = iter([choice1, choice2])
    #     # using lambda statement for mocking
    #     monkeypatch.setattr('builtins.input', lambda name: next(answers))
    #     ms.push_to_intake()


    #     result = calls.broad_get(service, test_workbook, f'{ms.ui_sheet}!A1:A1')
    #     assert result[0][0] == 'CD-A'      

    # def test_export_month_format(self, monkeypatch):  
    #     service = oauth(Config.my_scopes, 'sheet', mode='testing')

    #     ms = MonthSheet(full_sheet=test_workbook, path=test_path, mode='testing', test_service=service)
        
    #     calls = GoogleApiCalls()
    #     calls.clear_sheet(service, test_workbook, f'testjan22 2022!A1:ZZ100')

    #     choice1 = 4
    #     choice2 = 1
    #     choice3 = 1
    #     answers = iter([choice1, choice2, choice3])
    #     # using lambda statement for mocking
    #     monkeypatch.setattr('builtins.input', lambda name: next(answers))

    #     ms.set_user_choice()
    #     ms.control()
        
    #     result = calls.broad_get(service, test_workbook, 'testjan22 2022!A1:A1')
    #     result2 = calls.broad_get(service, test_workbook, 'testjan22 2022!h68:h68') #test formatting  
    #     assert result[0][0] == 'Unit' #test 
    #     assert result2[0][0] == '160.00' #test 

if __name__ == '__main__':
    test = TestSheetFormat()
    test.test_setup()