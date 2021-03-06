import pathlib
import time

from auth_work import oauth
from config import Config

from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
from utils import Utils


class YearSheet:

    G_SHEETS_HAP_COLLECTED = ["=D81"]
    G_SHEETS_TENANT_COLLECTED = ["=K69"]
    G_SHEETS_GRAND_TOTAL = ["=sum(K69:K78)"]
    MF_SUM_FORMULA = ["=sum(K82:K85)"]
    MF_PERCENT_FORMULA = ["=product(K86, .08)"]
    MF_FORMULA = ["=H86"]

    G_SUM_STARTBAL = ["=sum(D2:D68)"]
    G_SUM_KRENT = ["=sum(E2:E68)"]
    G_SUM_ACTSUBSIDY = ["=sum(F2:F68)"]
    G_SUM_ACTRENT = ["=sum(H2:H68)"]
    G_PAYMENT_MADE = ["=sum(K2:K68)"]
    G_CURBAL = ["=sum(L2:L68)"]

    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Actual Subsidy',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    MI_HEADER = ['Move-Ins']
    DEPOSIT_BOX_VERTICAL = ['dc total','rr', 'hap', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten','ten']
    G_DEPDETAIL = ["=sum(-D79, D82, D83, D84, D85, D86, D87, D88, D89)"]
    csc = ['type:', 'csc_total', 'other', 'other', 'other', 'other', 'total tr MIs', '', '', 'GRAND TOTAL', '', '', 'hap collected', 'positive adj', 'damages', 'tenant rent collected', 'total', 'mgmt @ 8%']
    calls = GoogleApiCalls()
    base_month = 'base'
    
    def __init__(self, full_sheet=None, month_range=None, mode=None, test_service=None):
        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
        else:
            self.service = oauth(my_scopes, 'sheet')
        
        self.source_id = None

    # def show_current_sheets(self, interactive=False):
    #     print('showing current sheets')
    #     titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
    #     path = Utils.show_files_as_choices(titles_dict, interactive=interactive)
    #     if interactive == True:
    #         return path
    #     return titles_dict

    def make_base_sheet(self):  
        response = self.calls.make_one_sheet(self.service, self.full_sheet, self.base_month + ' ' + f'{Config.current_year}')
        dict1 = {}
        dict1[response['replies'][0]['addSheet']['properties']['title']] = response['replies'][0]['addSheet']['properties']['sheetId']
        return dict1

    def duplicate_formatted_sheets(self, month_list=None):       
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        for title, id1 in titles_dict.items():
            if title == 'base 2022':
                self.source_id = id1   
        # offset == len(titles_dict) 
        insert_index = len(titles_dict)
        for name in month_list:
            insert_index += 1
            self.calls.api_duplicate_sheet(self.service, self.full_sheet, source_id=self.source_id, insert_index=insert_index, title=name)

    def formatting_runner(self, title_dict=None):
        # titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        # titles_dict = {name:id2 for name, id2 in titles_dict.items() if name != 'intake'}
        
        for sheet, sheet_id in title_dict.items():
            '''writes the sum formulas in a row'''
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_KRENT, f'{sheet}!E69:E69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTSUBSIDY, f'{sheet}!F69:F69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTRENT, f'{sheet}!H69:H69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_PAYMENT_MADE, f'{sheet}!K69:K69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_CURBAL, f'{sheet}!L69:L69')
    
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_STARTBAL, f'{sheet}!D69:D69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_SUM_FORMULA, f'{sheet}!K86:K86')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_PERCENT_FORMULA, f'{sheet}!K87:K87')
            
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!K82:K82', 'ROWS', self.G_SHEETS_HAP_COLLECTED)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!K85:K85', 'ROWS', self.G_SHEETS_TENANT_COLLECTED)

            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!J70:J90', 'COLUMNS', self.csc)
            
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!A1:M1', 'ROWS', self.HEADER_NAMES)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!B72:B72', 'ROWS', self.MI_HEADER)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!E79:E89', 'COLUMNS', self.DEPOSIT_BOX_VERTICAL)
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_DEPDETAIL, f'{sheet}!D90:D90')
            
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_GRAND_TOTAL, f'{sheet}!K79')
            
            self.calls.date_stamp(self.service, self.full_sheet, f'{sheet}!A70:A70')

            self.calls.bold_freeze(self.service, self.full_sheet, sheet_id, 1)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 1, 5, 78, 90)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 0, 100, 68, 69) 

    def remove_base_sheet(self):
        self.calls.del_one_sheet(self.service, self.full_sheet, self.source_id)

 