from config import Config, my_scopes
from auth_work import oauth
from utils import Utils
from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
import pathlib
import time
from time import sleep

class YearSheet:

    G_SHEETS_HAP_COLLECTED = ["=D81"]
    G_SHEETS_TENANT_COLLECTED = ["=K69"]
    G_SUM_STARTBAL = ["=sum(D2:D68)"]
    G_SUM_ENDBAL = ["=sum(L2:L68)"]
    G_SHEETS_SUM_PAYMENT = ["=sum(K2:K68)"]
    G_SHEETS_GRAND_TOTAL = ["=sum(K71:K76)"]
    MF_SUM_FORMULA = ["=sum(H81:H84)"]
    MF_PERCENT_FORMULA = ["=product(H85, .08)"]
    MF_FORMULA = ["=H86"]
    CURRENT_BALANCE = ["=sum(D2,H2,J2,-K2)"]
    # UI header names and formatting
    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Actual Subsidy',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    MF_FORMATTING_TEXT = ['managment fee>', 'hap collected>', 'positive adj', 'damages',
    'tenant rent collected', 'total', '0.08']
    DEPOSIT_BOX_VERTICAL = ['rr', 'hap', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten', 'ten','ten']
    DEPOSIT_BOX_HORIZONTAL = ['month', 'date']
    G_SHEETS_GRAND_TOTAL = ["=sum(K69:K76)"]
    G_SHEETS_LAUNDRY_STOTAL = ["=sum(K71:K72)"]
    G_SHEETS_SD_TOTAL = ['total by hand']
    sd_total = ["sd_total"]
    csc = ["type:", "csc", "csc", "other", "other","other"]
    laundry_income = ["laundry income"]
    grand_total = ["GRAND TOTAL"]
    

    def __init__(self, full_sheet=None, mode=None, test_service=None):
        self.test_message = 'hi_from_year_sheets!'
        self.full_sheet = full_sheet

        if mode == 'testing':
            self.mode = mode
            self.sleep = 0
            self.service = test_service
            self.shmonths = ['testjan22', 'feb', 'mar']
        else:
            self.sleep = 30
            self.service = oauth(my_scopes, 'sheet')
            self.shmonths = ['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']

        self.user_text = f'Options:\n PRESS 1 to show all current sheets in {self.full_sheet} \n PRESS 2 to create list of sheet NAMES \n PRESS 3 to format all months. *****YOU NEED TO MANUALLY MAKE AN INTAKE SHEET AFTER RUNNING OPTION 3(this is the full year auto option; takes 45 min) \n >>>'
        self.user_choice = None
        self.shyear = [f'{Config.current_year}']
        self.calls = GoogleApiCalls()
        self.units = Config.units
        self.wrange_unit = '!A2:A68'
        self.sheet_id_list = None

    def control(self):
        if self.user_choice == 1:
            self.show_current_sheets(interactive=False)
        elif self.user_choice == 2:
            self.make_sheets()
        elif self.user_choice == 3:
            self.formatting_runner()
        # elif self.user_choice == 4:
        #     sheet_choice, selection = self.show_current_sheets(interactive=True)
        #     self.set_user_choice_push(sheet=sheet_choice)
        #     if self.user_choice == 1:
        #         self.export_month_format(sheet_choice)
        #         self.month_write_col(sheet_choice)

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def show_current_sheets(self, interactive=False):
        print('showing current sheets')
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        path = Utils.show_files_as_choices(titles_dict, interactive=interactive)
        if interactive == True:
            return path

    def make_sheets(self):
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        sheet_names = Utils.make_sheet_names(self.shmonths, self.shyear)
        # Utils.make_sheets_from_sheet_names
        calls = GoogleApiCalls()

        for sheet_title in sheet_names:
            print(f'writing {sheet_title}')
            calls.make_one_sheet(self.service, self.full_sheet, sheet_title)
            sleep(self.sleep)
            print(f'taking {self.sleep} second nap to preserve writes. zzz...')

    def formatting_runner(self):

        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        titles_dict = {name:id2 for name, id2 in titles_dict.items() if name != 'intake'}
        titles_list = list(titles_dict)

        self.make_sheet_id(titles_dict)
        
        for sheet, sheet_id in titles_dict.items():
            self.format_units(sheet)
            self.calls.write_formula_column(self.service, self.full_sheet, self.CURRENT_BALANCE, f'{sheet}!L2:L2')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_STARTBAL, f'{sheet}!D69:D69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SUM_ENDBAL, f'{sheet}!K69:K69')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_SUM_FORMULA, f'{sheet}!H85:H85')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_PERCENT_FORMULA, f'{sheet}!H86:H86')
            self.calls.write_formula_column(self.service, self.full_sheet, self.MF_PERCENT_FORMULA, f'{sheet}!H80:H80')

            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!H81:H81', 'ROWS', self.G_SHEETS_HAP_COLLECTED)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!H84:H84', 'ROWS', self.G_SHEETS_TENANT_COLLECTED)

            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!J70:J75', 'COLUMNS', self.csc)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!M71:M71', 'ROWS', self.laundry_income)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!J77:J77', 'ROWS', self.grand_total)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!M73:M74', 'ROWS', self.sd_total)

            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!A1:M1', 'ROWS', self.HEADER_NAMES)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!G80:G86', 'COLUMNS', self.MF_FORMATTING_TEXT)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!E80:E89', 'COLUMNS', self.DEPOSIT_BOX_VERTICAL)
            self.calls.format_row(self.service, self.full_sheet, f'{sheet}!B80:C80', 'ROWS', self.DEPOSIT_BOX_HORIZONTAL)
            
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_SD_TOTAL, f'{sheet}!N73:N73')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_GRAND_TOTAL, f'{sheet}!K77')
            self.calls.write_formula_column(self.service, self.full_sheet, self.G_SHEETS_LAUNDRY_STOTAL, f'{sheet}!N71:N71')
            
            self.calls.date_stamp(self.service, self.full_sheet, f'{sheet}!A70:A70')

            self.calls.bold_freeze(self.service, self.full_sheet, sheet_id, 1)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 1, 5, 79, 90)
            self.calls.bold_range(self.service, self.full_sheet, sheet_id, 0, 100, 68, 69)
    
            time.sleep(30)
            time.sleep(self.sleep)

    def format_units(self, sheet):
        self.calls.simple_batch_update(self.service, self.full_sheet, f'{sheet}{self.wrange_unit}', self.units, 'COLUMNS')

    def make_sheet_id(self, titles_dict):
        self.sheet_id_list = list(titles_dict.values())
        print(self.sheet_id_list)
