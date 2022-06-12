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
    G_SUM_STARTBAL = ["=sum(D2:D68)"]
    G_SUM_ENDBAL = ["=sum(L2:L68)"]
    G_SHEETS_SUM_PAYMENT = ["=sum(K2:K68)"]
    G_SHEETS_GRAND_TOTAL = ["=sum(K69:K79)"]
    MF_SUM_FORMULA = ["=sum(H81:H84)"]
    MF_PERCENT_FORMULA = ["=product(H85, .08)"]
    MF_FORMULA = ["=H86"]
    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Actual Subsidy',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    MI_HEADER = ['Move-Ins']
    MF_FORMATTING_TEXT = ['managment fee>', 'hap collected>', 'positive adj', 'damages',
    'tenant rent collected', 'total', '0.08']
    DEPOSIT_BOX_VERTICAL = ['rr', 'hap', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten','ten']
    DEPOSIT_BOX_HORIZONTAL = ['month', 'date']
    G_SHEETS_SD_TOTAL = ['total by hand']

    sd_total = ['sd_total']
    csc = ['type:', 'csc_total', 'other', 'other', 'other', 'other', 'total tr MIs', '', '', '', 'GRAND TOTAL']
    calls = GoogleApiCalls()
    base_month = 'base'
    
    def __init__(self, full_sheet=None, month_range=None, mode=None, test_service=None):
        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
        else:
            self.service = oauth(my_scopes, 'sheet')
        
        self.source_id = None

    def show_current_sheets(self, interactive=False):
        print('showing current sheets')
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        path = Utils.show_files_as_choices(titles_dict, interactive=interactive)
        if interactive == True:
            return path
        return titles_dict

    def make_base_sheet(self, gc=None):  
        self.calls.make_one_sheet(self.service, self.full_sheet, self.base_month + ' ' + f'{Config.current_year}')

    def duplicate_formatted_sheets(self, month_list=None):       
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        for title, id1 in titles_dict.items():
            if title == 'base 2022':
                self.source_id = id1   
        insert_index = 2
        for name in month_list:
            insert_index += 1
            self.calls.api_duplicate_sheet(self.service, self.full_sheet, source_id=self.source_id, insert_index=insert_index, title=name)

    def formatting_runner(self):
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        titles_dict = {name:id2 for name, id2 in titles_dict.items() if name != 'intake'}
        
        for sheet, sheet_id in titles_dict.items():
            '''writes the sum formulas in a row'''
    
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_STARTBAL, f'{sheet}!D69:D69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_ENDBAL, f'{sheet}!K69:K69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_SUM_FORMULA, f'{sheet}!H85:H85')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_PERCENT_FORMULA, f'{sheet}!H86:H86')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_PERCENT_FORMULA, f'{sheet}!H80:H80')
            
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!H81:H81', 'ROWS', self.G_SHEETS_HAP_COLLECTED)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!H84:H84', 'ROWS', self.G_SHEETS_TENANT_COLLECTED)

            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!J70:J80', 'COLUMNS', self.csc)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!M73:M74', 'ROWS', self.sd_total)
            
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!A1:M1', 'ROWS', self.HEADER_NAMES)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!B72:B72', 'ROWS', self.MI_HEADER)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!G80:G86', 'COLUMNS', self.MF_FORMATTING_TEXT)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!E80:E89', 'COLUMNS', self.DEPOSIT_BOX_VERTICAL)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!B80:C80', 'ROWS', self.DEPOSIT_BOX_HORIZONTAL)
            
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_SD_TOTAL, f'{sheet}!N73:N73')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_GRAND_TOTAL, f'{sheet}!K80')
            
            self.calls.date_stamp(self.service, self.full_sheet, f'{sheet}!A70:A70')

            self.calls.bold_freeze(self.service, self.full_sheet, sheet_id, 1)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 1, 5, 79, 90)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 0, 100, 68, 69) 

    def remove_base_sheet(self):
        self.calls.del_one_sheet(self.service, self.full_sheet, self.source_id)

 