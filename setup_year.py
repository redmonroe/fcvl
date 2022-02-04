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

    def control(self):
        if self.user_choice == 1:
            self.show_current_sheets(interactive=False)
        elif self.user_choice == 2:
            self.make_sheets()
        # elif self.user_choice == 3:
        #     self.push_to_intake()
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

    
    
    
    
    '''

    def __init__(self, full_sheet, path, mode=None, test_service=None):
        self.test_message = 'hi'
        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
        else:
            self.service = oauth(my_scopes, 'sheet')
        self.db = Config.db_rs
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
                self.export_month_format(sheet_choice)
                self.month_write_col(sheet_choice)

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def set_user_choice_push(self, sheet):
        print(f'\n You have chosen to work with: {sheet}')
        self.user_choice = int(input(self.user_text2))

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
        self.file_input_path = Utils.sheet_finder(path=self.file_input_path, function='mformat')
        self.t_name, self.unit, self.k_rent, self.subsidy, self.t_rent = self.read_excel(verbose=False)
        self.write_to_rs()

    def month_write_col(self, sheet_choice):
        gc = GoogleApiCalls()
        unit = gc.batch_get(self.service, self.full_sheet, f'{self.ui_sheet}!A1:Z100', 0)
        gc.update(self.service, self.full_sheet, unit, f'{sheet_choice}!A2:A68')
        
        tenant_names = gc.batch_get(self.service, self.full_sheet, f'{self.ui_sheet}!A1:Z100', 1) #tenant names #
        gc.update(self.service, self.full_sheet, tenant_names, f'{sheet_choice}!B2:B68')       

        contract_rent = gc.batch_get(self.service, self.full_sheet, f'{self.ui_sheet}!A1:Z100', 2)
        gc.update(self.service, self.full_sheet, contract_rent, f'{sheet_choice}!E2:E68', value_input_option='USER_ENTERED')
        
        subsidy = gc.batch_get(self.service, self.full_sheet, f'{self.ui_sheet}!A1:Z100', 3)
        gc.update(self.service, self.full_sheet,subsidy, f'{sheet_choice}!F2:F68', value_input_option='USER_ENTERED')
        
        tenant_rent = gc.batch_get(self.service, self.full_sheet, f'{self.ui_sheet}!A1:Z100', 4)
        gc.update(self.service, self.full_sheet, tenant_rent, f'{sheet_choice}!H2:H68', value_input_option='USER_ENTERED')

    def export_month_format(self, sheet_choice):
        gc = GoogleApiCalls()
        gc.format_row(self.service, self.full_sheet, f'{sheet_choice}!A1:M1', "ROWS", self.HEADER_NAMES)
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_KRENT, f'{sheet_choice}!E69:E69')#ok
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTSUBSIDY, f'{sheet_choice}!F69:F69')#
        gc.write_formula_column(self.service, self.full_sheet, self.G_SUM_ACTRENT, f'{sheet_choice}!H69:H69')#

    def export_to_sqlite(self):
        # import dataset
        print(Config.db_rs)

        db = Config.db_rs
        # table = db['user']
        # table.insert(dict(name='John Doe', age=46, country='China'))
        users = db['user'].all()
        for u in users:
            print(u)
    
    def get_tables(self):
        DBUtils.get_tables(self, self.db)

    def delete_table(self):
        db = Config
        DBUtils.delete_table(self, self.db)
    '''
