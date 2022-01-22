from config import Config, my_scopes
from auth_work import oauth
from utils import Utils
from google_api_calls_abstract import GoogleApiCalls
import pathlib
import pandas as pd

class MonthSheet:

    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Subsity Entitlement',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    G_SUM_KRENT = ["=sum(E2:E68)"]
    G_SUM_ACTSUBSIDY = ["=sum(F2:F68)"]
    G_SUM_ACTRENT = ["=sum(H2:H68)"]
    ui_sheet = 'intake'
    range1 = '1'
    range2 = '100'
    wrange_unit1 = f'{ui_sheet}!A{range1}:A{range2}'
    wrange_t_name1 = f'{ui_sheet}!B{range1}:B{range2}'
    wrange_k_rent1 = f'{ui_sheet}!c{range1}:c{range2}'
    wrange_subsidy1 = f'{ui_sheet}!d{range1}:d{range2}'
    wrange_t_rent1 = f'{ui_sheet}!e{range1}:e{range2}'
    user_text2 = f'\n Please make sure you have run option 3 in the previous menu that formats Intake for the rent sheet. \n Please PRESS 1 when ready . . .'

    def __init__(self, full_sheet, path):
        self.test_message = 'hi'
        self.full_sheet = full_sheet
        self.service = oauth(my_scopes, 'sheet')
        self.user_choice = None
        self.text_snippet = ''
        self.file_input_path = path
        self.user_text = f'Options:\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 TO VIEW ITEMS IN {self.file_input_path} \n PRESS 3 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet in {self.file_input_path} (xlsx) \n PRESS 4 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'
        self.wrange_unit = self.wrange_unit1
        self.wrange_t_name = self.wrange_t_name1
        self.wrange_k_rent = self.wrange_k_rent1 
        self.wrange_subsidy = self.wrange_subsidy1
        self.wrange_t_rent = self.wrange_t_rent1 
        self.t_name = []
        self.unit = [] 
        self.k_rent = [] 
        self.subsidy = [] 
        self.t_rent = []

    def control(self):
        if self.user_choice == 1:
            self.show_current_sheets(interactive=False)
        elif self.user_choice == 2:
            self.walk_download_folder()
        elif self.user_choice == 3:
            self.push_to_intake()
        elif self.user_choice == 4:
            sheet_choice, selection = self.show_current_sheets(interactive=True)
            self.set_user_choice_push(sheet=sheet_choice)
            if self.user_choice == 1:
                self.month_format(sheet_choice)

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def set_user_choice_push(self, sheet):
        print(f'\n You have chosen to work with: {sheet}')
        self.user_choice = int(input(self.user_text2))

    def show_current_sheets(self, interactive=False):
        print('showing current sheets')
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        path = Utils.show_files_as_choices(titles_dict, interactive=interactive)
        if interactive == True:
            return path

    def walk_download_folder(self):
        print('showing ALL items in download folder')
        current_items = [p for p in pathlib.Path(self.file_input_path).iterdir() if p.is_file()]
        for item in current_items:
            print(item.name)

    def read_excel(self, verbose=False):
        df = pd.read_excel(self.file_input_path, header=16)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(100))
        t_name = df['Name'].tolist()
        unit = df['Unit'].tolist()
        k_rent = df['Lease Rent'].tolist()
        t_rent = df['Actual Rent Charge'].tolist()
        subsidy = df['Actual Subsidy Charge'].tolist()

        return self.fix_data(t_name), self.fix_data(unit), self.fix_data(k_rent), self.fix_data(subsidy), self.fix_data(t_rent)

    def fix_data(self, item):
        item.pop()
        return item

    def write_to_rs(self):
        gc = GoogleApiCalls()
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_unit, self.unit, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_t_name, self.t_name, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_k_rent, self.k_rent, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_subsidy, self.subsidy, 'COLUMNS')
        gc.simple_batch_update(self.service, self.full_sheet, self.wrange_t_rent, self.t_rent, 'COLUMNS')

    def push_to_intake(self):
        print('pushing selected excel to intake', '| Path:', self.file_input_path)
        self.file_input_path = Utils.sheet_finder(path=self.file_input_path, function='month setup')
        self.t_name, self.unit, self.k_rent, self.subsidy, self.t_rent = self.read_excel(verbose=False)
        self.write_to_rs()

    def month_write_col(self, sheet_choice):
        gc = GoogleApiCalls()
        col2 = gc.batch_get(1) #tenant names #
        format.update(col2, f'{sheet_choice}!B1:B69')
        col3 = gc.batch_get(2) # market rate #
        format.update_int(col3, f'{sheet_choice}!E1:E68')
        col5 = format.batch_get(3) #actual subsidy #
        format.update_int(col5, f'{sheet_choice}!F1:F68')
        col4 = format.batch_get(4) #actual rent charged #
        format.update_int(col4, f'{sheet_choice}!H1:H68')

    def month_format(self, sheet_choice):
        gc = GoogleApiCalls()
        gc.format_row(self.service, self.full_sheet, f'{sheet_choice}!A1:M1', "ROWS", self.HEADER_NAMES)
        # gc.write_formula_column(self.G_SUM_KRENT, f'{sheet_choice}!E69:E69')#ok
        # gc.write_formula_column(self.G_SUM_ACTSUBSIDY, f'{sheet_choice}!F69:F69')#
        # gc.write_formula_column(self.G_SUM_ACTRENT, f'{sheet_choice}!H69:H69')#

ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.RS_DL_FILE_PATH)
ms.set_user_choice()
ms.control()

# the PRESS 1 IS READY GUARD IS FUCKING UP
# set up testing, sheet clearing
# formatting of intake, what else can I do with this data, setup sqlite and
    # rent potential
# I HAVE A PROBLEM GETTING THE FILE INTO WSL FOLDER: i CAN DO IT WITH LINUX OR VSCODE, BUT NOT WINDOWS EXPLORER