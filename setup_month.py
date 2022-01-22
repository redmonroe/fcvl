from config import Config, my_scopes
from auth_work import oauth
from utils import Utils


class MonthSheet:

    HEADER_NAMES = ['Unit', 'Tenant Name', 'Notes', 'Balance Start', 'Contract Rent', 'Subsity Entitlement',
    'Hap received', 'Tenant Rent', 'Charge Type', 'Charge Amount', 'Payment Made', 'Balance Current', 'Payment Plan/Action']
    G_SUM_KRENT = ["=sum(E2:E68)"]
    G_SUM_ACTSUBSIDY = ["=sum(F2:F68)"]
    G_SUM_ACTRENT = ["=sum(H2:H68)"]
    user_text ='Options\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet from /download_here (xlsx) \n PRESS 3 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'

    def __init__(self, full_sheet):
        self.test_message = 'hi'
        self.full_sheet = full_sheet
        self.service = oauth(my_scopes, 'sheet')
        self.user_choice = None

    def control(self):
        if self.user_choice == 1:
            self.show_current_sheets()

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def show_current_sheets(self):
        print('showing current sheets')
        titles_dict = Utils.get_existing_sheets(self.service, self.full_sheet)
        Utils.show_files_as_choices(titles_dict, interactive=False)

    def show_utils(self):
        for k, item in Utils.__dict__.items():
            print(k, item)

ms = MonthSheet(full_sheet=Config.TEST_RS)
ms.set_user_choice()
ms.control()
print(ms.user_choice)

# set input