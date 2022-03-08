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
from db_utils import DBUtils
from pathlib import Path
from setup_year import YearSheet
from setup_month import MonthSheet
from file_indexer import FileIndexer
from build_rs import BuildRS
from checklist import Checklist
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch
import shutil
import pdb
import dataset


TEST_RR_FILE = 'TEST_rent_roll_01_2022.xls'
TEST_DEP_FILE = 'TEST_deposits_01_2022.xls'
GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'
GENERATED_DEP_FILE = 'TEST_DEP_012022.xls'

test_workbook = Config.TEST_RS
path = Config.TEST_RS_PATH
test_path = Config.TEST_RS_PATH
discard_pile = Config.TEST_MOVE_PATH
chck_list_db = Config.test_checklist_db
monkeypatch = MonkeyPatch()
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()
findex = FileIndexer(path=test_path, discard_pile=discard_pile, db=Config.test_findex_db, table='findex')
ys = YearSheet(full_sheet=test_workbook, mode='testing', test_service=service)
build = BuildRS(full_sheet=test_workbook, path=test_path, mode='testing', test_service=service)
cl = Checklist(db=chck_list_db)


@pytest.mark.setup_only
class TestChecklist:

    test_message = 'hi'
    path_contents = []
    db = None

    def remove_generated_file_from_dir(self, path1=None, file1=None):
        # pdb.set_trace()
        try:
            os.remove(os.path.join(str(path1), file1))
        except FileNotFoundError as e:
            print(e, f'{file1} NOT found in test_data_repository, make sure you are looking for the right name')
    
    def move_original_back_to_dir(self, discard_dir=None, target_file=None, target_dir=None):
        findex.get_file_names_kw(discard_dir)
        for item in findex.test_list:
            if item == target_file:
                try:
                    shutil.move(os.path.join(str(discard_dir), item), target_dir)
                except:
                    print('Error occurred copying file: jw')

    def make_path_contents(self, path=None):
        for item in path.iterdir():
            sub_item = Path(item)
            filename = sub_item.parts[-1]
            f_ext = filename.split('.')
            f_ext = f_ext[-1]
            self.path_contents.append(filename) 
    
    def test_setup_findexer(self):
        TestChecklist.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_RR_FILE)
        TestChecklist.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_DEP_FILE)

        TestChecklist.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_RR_FILE, target_dir=path)
        TestChecklist.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_DEP_FILE, target_dir=path)

        # check discard_pile is empty
        discard_contents = [count for count, file1 in enumerate(discard_pile.iterdir())]
        # check path pile is 5
        path_contents1 = [count for count, file1 in enumerate(path.iterdir())]
        assert len(discard_contents) == 1
        assert len(path_contents1) == 6

    def test_setup_checklist(self):
        '''make checklist for current year/12 sheets'''
        chklist = Checklist(db=chck_list_db)
        chklist.make_checklist()
        records = chklist.show_checklist()

        assert len(records[0]) == 12
    
    @pytest.mark.skip(reason='429 from google if I do too much')
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
    
    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_year_format_one_sheet(self):
        shnames = ys.auto_control()
        assert len(shnames) == 12

        result = calls.broad_get(service, Config.TEST_RS, 'jan 2022!A2:A2')
        result2 = calls.broad_get(service, Config.TEST_RS, 'jan 2022!G85:G85')
       
        assert result[0][0] == 'CD-A' #test 
        assert result2[0][0] == 'total' #test 

    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_duplicate_formatted_base(self):
        # should be all months  + intake
        titles_dict = Utils.get_existing_sheets(service, test_workbook)
        assert len(titles_dict) == 13

    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_prev_balance(self):
        ys.make_shifted_list_for_prev_bal()
        prev_bal = ys.prev_bal_dict

        result = calls.broad_get(service, Config.TEST_RS, 'feb 2022!D2:D2')
        assert result[0][0] == '0'

    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_checklist_pickup_rs_exist_and_yfor(self):
        check_item, yfor, rs_exist = cl.show_checklist()

        assert all(yfor) == True
        assert all(rs_exist) == True


    @pytest.fixture
    def setup_test_db(self):
        db = findex.db
        tablename = findex.tablename
        table = db[tablename]
        table.drop()
        check_tables = db.tables
        assert check_tables == []
        return db
    
    def test_build_index_preflight(self, setup_test_db):
        db = setup_test_db

        findex_name_as_str = findex.tablename
        findex.build_index()

        index_cols = db[findex_name_as_str].columns

        record_1 = db[findex_name_as_str].find_one(fn=TEST_DEP_FILE)
        
        # assert index_cols == ['id', 'fn', 'path', 'status', 'period']
        assert 'TEST_deposits_01_2022.xls' in record_1['fn']
        assert len(db[findex_name_as_str]) == 5


    def test_rent_roll_flow(self):
        findex.build_index_runner()
        TestChecklist.make_path_contents(self, path=path)

        assert GENERATED_RR_FILE in self.path_contents

    def test_deposit_flow(self):
        TestChecklist.make_path_contents(self, path=path)        
        assert GENERATED_DEP_FILE in self.path_contents

    def test_check_for_processed_and_period(self, setup_test_db):
        db = setup_test_db
        findex_name_as_str = findex.tablename

        findex.build_index()
        findex.update_index_for_processed()
        index_cols = db[findex_name_as_str].columns
        record_1 = db[findex_name_as_str].find_one(fn=GENERATED_DEP_FILE)
        proc_list = findex.do_index()
        
        assert index_cols == ['id', 'fn', 'path', 'status', 'period']
        assert GENERATED_DEP_FILE in record_1['fn']
        assert GENERATED_DEP_FILE in proc_list
        assert GENERATED_RR_FILE in proc_list
        assert '2022-01' == record_1['period']


    @pytest.mark.new_one_only
    def test_find_items_processed_by_findexer(self):
        ''' if rent roll month is processed == True, then push it to sheet'''
        processed_items = build.automatic_build(key='RENTROLL')

        test_criteria_contains_rentroll = [True for filename in processed_items if 'RENTROLL' in filename['fn'].split('_')]

        assert test_criteria_contains_rentroll[0] == True

    def test_mformat_and_push_one_to_intake(self):  
        result = calls.broad_get(service, test_workbook, 'jan 2022!E69:E69')
        result2 = calls.broad_get(service, test_workbook, f'intake!A1:A1')
    
        assert result[0][0] == '51402'   
        assert result2[0][0] == 'CD-A'   

    def test_write_pay_list(self):  
        result = calls.broad_get(service, test_workbook, 'jan 2022!K69:K69')
        assert result[0][0] == '14975'   

    def test_build_rs_to_excel(self):
        processed_items = build.automatic_build(key='DEP')
        test_df = build.df
        bde = test_df['deposit_id'].tolist()

        assert bde[0] == 20979.0

    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_teardown_mformat(self):
        # clear intake
        calls.clear_sheet(service, test_workbook, f'intake!A1:ZZ100')
        calls.clear_sheet(service, test_workbook, f'jan 2022!b2:b68')
        calls.clear_sheet(service, test_workbook, f'jan 2022!e2:h68')
        calls.clear_sheet(service, test_workbook, f'jan 2022!a69:z69')

        result = calls.broad_get(service, test_workbook, 'jan 2022!E69:E69')
        result2 = calls.broad_get(service, test_workbook, f'intake!A1:A1')
        assert result == []   
        assert result2 == []   

    @pytest.mark.skip(reason='429 from google if I do too much')
    def test_build_index_postflight(self, setup_test_db):
        db = setup_test_db
        findex_name_as_str = findex.tablename
        findex.drop_tables()

        findex.build_index()
        
        assert len(db[findex_name_as_str]) == 5




    # def test_teardown(self, setup_test_db):
    #     TestChecklist.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_RR_FILE)
    #     TestChecklist.remove_generated_file_from_dir(self, path1=path, file1=GENERATED_DEP_FILE)

    #     TestChecklist.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_RR_FILE, target_dir=path)
    #     TestChecklist.move_original_back_to_dir(self, discard_dir=discard_pile, target_file=TEST_DEP_FILE, target_dir=path)
        
    #     discard_contents = [count for count, file in enumerate(discard_pile.iterdir())]
    #     path_contents = [count for count, file in enumerate(path.iterdir())]

    #     db = setup_test_db
    #     findex_name_as_str = findex.tablename
    #     db[findex_name_as_str].drop()

    #     assert len(db[findex_name_as_str]) == 0
    #     assert len(discard_contents) == 1
    #     assert len(path_contents) == 6


'''deprecated but not to destroy'''

#     def test_push_to_intake(self, monkeypatch):

#         ms = MonthSheet(full_sheet=test_workbook, path=test_path, mode='testing', test_service=service)
        

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