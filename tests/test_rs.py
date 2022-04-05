import sys
import os
import time
import pytest
from datetime import datetime
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from auth_work import oauth
# from utils import Utils
# from db_utils import DBUtils
from pathlib import Path
from setup_year import YearSheet
from setup_month import MonthSheet
from file_indexer import FileIndexer
from build_rs import BuildRS
from checklist import Checklist
from google_api_calls_abstract import GoogleApiCalls
# from _pytest.monkeypatch import MonkeyPatch
# import shutil
# import pdb
# import dataset


# TEST_RR_FILE = 'TEST_rent_roll_01_2022.xls'
# TEST_DEP_FILE = 'TEST_deposits_01_2022.xls'
# GENERATED_RR_FILE = 'TEST_RENTROLL_012022.xls'
# GENERATED_DEP_FILE = 'TEST_DEP_012022.xls'


sleep = 1
test_workbook = Config.TEST_RS
path = Config.TEST_RS_PATH
discard_pile = Config.TEST_MOVE_PATH
cl_test_db = Config.test_checklist_db
findex_test_db = Config.test_findex_db
build_test_db = Config.test_build_db
build_tablename = Config.test_build_name
# monkeypatch = MonkeyPatch()
service = oauth(Config.my_scopes, 'sheet', mode='testing')
calls = GoogleApiCalls()
checklist = Checklist()
findex = FileIndexer(path=path, discard_pile=discard_pile, db=findex_test_db, table=Config.test_findex_name)
ys = YearSheet(full_sheet=test_workbook, mode='testing', checklist=checklist, test_service=service, sleep=sleep)
ms = MonthSheet(full_sheet=test_workbook, path=path, mode='testing', sleep=sleep, test_service=service)
build = BuildRS(full_sheet=test_workbook, path=path, mode='testing', findex_obj=findex, checklist_obj=checklist, mformat_obj=ms, test_service=service, rs_tablename=build_tablename)


# invokce a test func marked @pytest.mark.production with pytest -v -m production
# invoke test class with: pytest -q -m testing
@pytest.mark.testing
class TestProduction:

    test_message = 'hi'

    def test_setup_sheet_prime(self):
        title_dict = ys.show_current_sheets()
        for name, id2, in title_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, test_workbook, id2)
        calls.clear_sheet(service, test_workbook, f'intake!A1:ZZ100')
        title_dict = ys.show_current_sheets()
        assert [*title_dict.items()] == [('intake', 1226016565)]

    def test_setup(self):
        '''basic checks for environment and configuration'''
        assert self.test_message == 'hi'
        assert test_workbook == '1Z_Qoz-4ehalutipyH2Vj5k-y2b78U69Bc7uXoBKK47Q'
        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')
        assert Config.test_findex_name == 'findex_test'
        assert Config.test_build_name == 'build_test'
        assert Config.test_checklist_name == 'checklist_test'
        assert cl_test_db.__dict__['url'] == "sqlite:////home/joe/local_dev_projects/fcvl/sqlite/checklist_test_database.db"
        assert findex_test_db.__dict__['url'] == "sqlite:////home/joe/local_dev_projects/fcvl/sqlite/findex_test_database.db"
        assert build_test_db.__dict__['url'] == "sqlite:////home/joe/local_dev_projects/fcvl/sqlite/build_test_database.db"
        assert service.__dict__['_dynamic_attrs'][1] == 'spreadsheets'
        assert calls.verify == '511' # this is the arbitrary test for the google api calls class

    def test_setup_checklist(self):
        '''PLACE YEAR IN DB TABLENAME SO THAT IT MIGHT LIVE PAST THE YEAR'''

        checklist.drop_checklist()

        assert checklist.db == cl_test_db
        assert checklist.tablename == 'checklist_test' # we have right name

        checklist.check_cl_exist()
        assert checklist.init_status == 'empty_db' # cl is empty

        cl_month_list = checklist.limit_date() # we are building cl iteratively
        current_month = datetime.now().month
    
        assert len(cl_month_list) == current_month

        table = checklist.make_checklist(month_list=cl_month_list, mode='iterative_cl')
        assert len(table) == current_month
        assert table.columns == ['id', 'year', 'month', 'base_docs', 'rs_exist', 'yfor', 'mfor', 'rr_proc', 'dep_proc', 'depdetail_proc', 'opcash_proc', 'grand_total_ok']

    def test_setup_findexer(self):    

        '''NEED TO TEST PDF SOON OR AT SOME POINT'''

        assert path == Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/audit 2022/test_rent_sheets_data_sources')

        findex.drop_tables()
        init_status = findex.check_findex_exist()
        assert init_status == 'empty'
    
        table = findex.build_raw_index(verbose=False)
        assert len(table) == 4  # we have 4 files in the directory(2 rent roll, 2 deposits)
        assert table.columns == ['id', 'fn', 'path', 'status', 'indexed']
        directory_contents = findex.articulate_directory()
        names = [x.name for x in directory_contents]
        assert names == ['deposits_01_2022.xls', 'deposits_02_2022.xlsx', 'rent_roll_01_2022.xls', 'rent_roll_02_2022.xlsx']

        index_dict = findex.sort_directory_by_extension(verbose=False) # get extensions: NO PDF YET
        assert 'xls' and 'xlsx' in [*index_dict.values()]

        findex.mark_as_checked(verbose=False) # no return: marks all files as checked
        results = findex.ventilate_table()
        assert len(results) == 4
        checked_in_true = [x['indexed'] for x in results]
        assert all(checked_in_true)

        processed_files = findex.rename_by_content_xls() # the concept of processed is getting weaker
        findex.update_index_for_processed()

        results = findex.ventilate_table()
        processed_true = [x['status'] for x in results]
        assert all(processed_true)

        # DO NOT ERASE THIS!!!
        # self.processed_files = self.rename_by_content_pdf()

    def test_buildrs_init(self):
        results = build.findex.ventilate_table()

        build.proc_condition_list = build.check_diad_processed()
        assert [*build.proc_condition_list[0].values()] == [2, 2]
        build.proc_condition_list = build.reformat_conditions_as_bool(trigger_condition=2)
        assert [*build.proc_condition_list[0].values()] == [True, True]
        build.final_to_process_list = build.make_list_of_true_dates()
        assert '2022-01' and '2022-02' in build.final_to_process_list

    def test_merely_mark_base_docs_processed(self):
        for date in build.final_to_process_list:
            build.checklist.check_basedocs_proc(date)
        check_items = checklist.show_checklist()
        assertion1 = [x['month'] for x in check_items if x['base_docs'] == True]
        assert assertion1 == ['jan', 'feb']

    def test_compare_base_docs_true_to_grand_total_true(self):
        '''on first pass this should show empty lists bc no month is complete'''
        final_to_process_set = build.compare_base_docs_true_to_grand_total_true()
        assert build.month_complete_is_true_list == []
        assert final_to_process_set == {'2022-01', '2022-02'}
        assert type(final_to_process_set) == set

        build.final_to_process_list = list(final_to_process_set.difference(set(build.month_complete_is_true_list)))
        assert'2022-01' and '2022-02' in build.final_to_process_list

    def test_sort_final_to_process_list(self):
        build.final_to_process_list = build.sort_and_adj_final_to_process_list()
        ftp = build.final_to_process_list
        assert ftp == ['jan', 'feb']  ## ORDER MATTERS HERE

    def test_init_yearsheet_and_set_month_range(self):
        ys.shmonths = build.final_to_process_list
        assert ys.shmonths == ['jan', 'feb']

    def test_remove_made_sheets_from_ftp_list(self):
        '''I really need to test this more: second pass of these tests(not of my effort)'''
        title_dict = ys.show_current_sheets()
        build.final_to_process_list = build.remove_already_made_sheets_from_list(input_dict=title_dict)
        ftp = build.final_to_process_list
        assert ftp == ['jan', 'feb']

    def test_init_yearsheet_and_set_month_range_after_removing_writes(self):
        '''make this func meaningful'''
        ys.shmonths = build.final_to_process_list
        assert ys.shmonths == ['jan', 'feb']

    def test_write_sheets(self):
        shnames = ys.full_auto()
        assert shnames == ['jan 2022', 'feb 2022']

    def test_ready_to_write_first_pass(self):
        build.proc_ms_list = build.make_is_ready_to_write_list(style='base_docs_and_sheet_ok')
        assert build.proc_ms_list == ['2022-01', '2022-02']
    
    def test_target_processed_docs_by_month(self):
        build.good_opcash_list, build.good_rr_list, build.good_dep_list = build.find_targeted_doc_in_findex_db()
        assert build.good_opcash_list == []
        rr_s = [x['fn'] for x in build.good_rr_list]
        assert rr_s == ['rent_roll_01_2022.xls', 'rent_roll_02_2022.xlsx']
        dep_s = [x['fn'] for x in build.good_dep_list]
        assert dep_s == ['deposits_01_2022.xls', 'deposits_02_2022.xlsx']

    def test_write_all_then_test(self):
        for item in build.good_rr_list:
            build.write_rentroll(item)

        for item in build.good_dep_list:
            build.write_payments(item)

    def test_select_from_sheets_after_writing(self):
        result = calls.broad_get(service, test_workbook, 'jan 2022!k68:k68')
        result2 = calls.broad_get(service, test_workbook, 'feb 2022!f68:fq68')
        assert result[0][0] == '153'
        assert result2[0][0] == '588'
        # breakpoint()


    def test_teardown_sheets(self):
        # remove existing sheets minus intake but clear intake
        title_dict = ys.show_current_sheets()
        for name, id2, in title_dict.items():
            if name != 'intake':
                calls.del_one_sheet(service, test_workbook, id2)
        calls.clear_sheet(service, test_workbook, f'intake!A1:ZZ100')
        # calls.del_one_sheet(service, spreadsheet_id, id):
        title_dict = ys.show_current_sheets()
        assert [*title_dict.items()] == [('intake', 1226016565)]
        # breakpoint()
        # calls.clear_sheet(service, test_workbook, f'jan 2022!b2:b68')
        # calls.clear_sheet(service, test_workbook, f'jan 2022!e2:h68')
        # calls.clear_sheet(service, test_workbook, f'jan 2022!k2:k68')
        # calls.clear_sheet(service, test_workbook, f'jan 2022!k71:k71')
        # calls.clear_sheet(service, test_workbook, f'jan 2022!a69:z69')

        # result = calls.broad_get(service, test_workbook, 'jan 2022!E69:E69')
        # result2 = calls.broad_get(service, test_workbook, f'intake!A1:A1')
        # assert result == []   
        # assert result2 == []   


        
    