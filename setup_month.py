from config import Config, my_scopes
from auth_work import oauth
from utils import Utils
import pathlib
import pandas as pd

class MonthSheet:

    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Subsity Entitlement',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    G_SUM_KRENT = ["=sum(E2:E68)"]
    G_SUM_ACTSUBSIDY = ["=sum(F2:F68)"]
    G_SUM_ACTRENT = ["=sum(H2:H68)"]
  

    def __init__(self, full_sheet, path):
        self.test_message = 'hi'
        self.full_sheet = full_sheet
        self.service = oauth(my_scopes, 'sheet')
        self.user_choice = None
        self.file_input_path = path
        self.user_text = f'Options:\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 TO VIEW ITEMS IN {self.file_input_path} \n PRESS 3 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet in {self.file_input_path} (xlsx) \n PRESS 4 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'

    def control(self):
        if self.user_choice == 1:
            self.show_current_sheets()
        elif self.user_choice == 2:
            self.walk_download_folder()
        elif self.user_choice == 3:
            self.push_to_intake()
        elif self.user_choice == 4:
            print('set up push to sheet from intake')

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def show_current_sheets(self):
        print('showing current sheets')
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        Utils.show_files_as_choices(titles_dict, interactive=False)

    def walk_download_folder(self):
        print('showing ALL items in download folder')
        current_items = [p for p in pathlib.Path(self.file_input_path).iterdir() if p.is_file()]
        for item in current_items:
            print(item.name)

    def push_to_intake(self):
        print('pushing selected excel to intake', '| Path:', self.file_input_path)
        path = Utils.sheet_finder(path=self.file_input_path, function='month setup')
        df = pd.read_excel(path, header=16)
        print(df.head(100))

        # select xlrd(xls) or openpyxl(xlsx)

        # sheet = Utils.load_activate_workbook(path)
        # print(sheet)

    def show_utils(self):
        for k, item in Utils.__dict__.items():
            print(k, item)


ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.RS_DL_FILE_PATH)
ms.set_user_choice()
ms.control()

# I THINK PANDAS CAN JUST PICK BY FILE TYPE: SO I CHOOSE AND THEN PANDAS CAN GRAB IF XLS OR CSV OR XLSX
# I HAVE A PROBLEM GETTING THE FILE INTO WSL FOLDER: i CAN DO IT WITH LINUX OR VSCODE, BUT NOT WINDOWS EXPLORER


# def keetchen_saink(self, sheet): 
#         """This is a custom job; tailored to the specific format of sheet for realpage deposits"""

#         pprint("Using openpyxl to iterate through rows of excel sheet . . .")
#         pprint("Generating lists of transactions et al from sheet . . . ")

#         bde = []
#         unit=[] #1
#         name=[] #3
#         date=[] #7
#         pay=[] #13

#         pay_float = []

#         for value in sheet.iter_rows(min_row=11, min_col=1, max_col=14, values_only=True):

#             if value[1] != None:
#                 bde.append(value[1])
#                 unit.append(value[0])
#                 name.append(value[2])
#                 date.append(value[5])
#                 pay.append(value[13])

#             else:
#                 print(value)
#                 print("******")

#         print("******")
#         pay_float = [float(item) for item in pay]
#         dt_code = [item[0:2]for item in date] # makes date code
#         print("******")

#         return bde, unit, name, date, pay, pay_float, dt_code