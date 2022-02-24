import sys
import os
import time
import pytest
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from auth_work import oauth
from utils import Utils
from setup_year import YearSheet
from setup_month import MonthSheet
from checklist import Checklist
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch

test_workbook = Config.TEST_RS
test_path = Config.TEST_RS_PATH
chck_list_db = Config.test_checklist_db
monkeypatch = MonkeyPatch()
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()
ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)


@pytest.mark.setup_only
class TestChecklist:

    def test_setup_checklist(self):
        '''make checklist for current year/12 sheets'''
        chklist = Checklist(db=chck_list_db)
        chklist.make_checklist()
        records = chklist.show_checklist()

        assert len(records[0]) == 12
    
    def test_setup_intake(self):
        '''test to make sure intake is present; reset intake state'''
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
    
    @pytest.mark.setup_only
    def test_year_format_one_sheet(self):
        shnames = ys.auto_control()
        assert len(shnames) == 12

        result = calls.broad_get(service, Config.TEST_RS, 'jan 2022!A2:A2')
        result2 = calls.broad_get(service, Config.TEST_RS, 'jan 2022!G85:G85')
       
        assert result[0][0] == 'CD-A' #test 
        assert result2[0][0] == 'total' #test 

    # @pytest.mark.setup_only
    # def test_duplicate_formatted_base(self):
    #     # should be all months  + intake
    #     titles_dict = Utils.get_existing_sheets(service, test_workbook)
    #     assert len(titles_dict) == 13

    # @pytest.mark.setup_only
    # def test_prev_balance(self):
    #     ys.make_shifted_list_for_prev_bal()
    #     prev_bal = ys.prev_bal_dict

    #     result = calls.broad_get(service, Config.TEST_RS, 'feb 2022!D2:D2')
    #     assert result[0][0] == '0'

    # @pytest.mark.setup_only
    # def test_checklist_pickup_rs_exist_and_yfor(self):
    #     cl = Checklist(db=chck_list_db)
    #     check_item, yfor, rs_exist = cl.show_checklist()

    #     assert all(yfor) == True
    #     assert all(rs_exist) == True


'''deprecated but not to destroy'''

#     def test_push_to_intake(self, monkeypatch):

#         ms = MonthSheet(full_sheet=test_workbook, path=test_path, mode='testing', test_service=service)
        
#         # calls.clear_sheet(service, test_workbook, f'{ms.ui_sheet}!A1:ZZ100')

#         choice1 = 2
#         choice2 = 1
#         answers = iter([choice1, choice2])
#         # using lambda statement for mocking
#         monkeypatch.setattr('builtins.input', lambda name: next(answers))
#         ms.push_to_intake()


#         result = calls.broad_get(service, test_workbook, f'{ms.ui_sheet}!A1:A1')
#         assert result[0][0] == 'CD-A'      

    # def test_teardown_month_sheets(self):
    #     ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)
    #     titles_dict = Utils().get_existing_sheets(service,test_workbook)

    #     calls.clear_sheet(service, test_workbook, 'intake!A1:ZZ100')
    #     for name, id2, in titles_dict.items():
    #         if name != 'intake':
    #             calls.del_one_sheet(service, test_workbook, id2)

    #     titles_dict1 = Utils().get_existing_sheets(service,test_workbook)        
        
    #     assert len(titles_dict1) == 1



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
    #     result2 = calls.broad_get(service, test_workbookN, 'testjan22 2022!h68:h68') #test formatting  
    #     assert result[0][0] == 'Unit' #test 
    #     assert result2[0][0] == '160.00' #test 

if __name__ == '__main__':
    test = TestSheetFormat()
    test.test_setup()