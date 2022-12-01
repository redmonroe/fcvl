
import os.path
from calendar import monthrange
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, Decimal
from pathlib import Path

import pandas as pd
import xlrd
from openpyxl import Workbook, load_workbook

from google_api_calls_abstract import GoogleApiCalls


class Utils:

    @staticmethod
    class dotdict(dict):
        """dot.notation access to dictionary attributes"""
        __getattr__ = dict.get
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__
    
    @staticmethod
    def get_next_month(target_month=None):
        from dateutil.relativedelta import relativedelta
        
        last_date = datetime.strptime(target_month, '%Y-%m')
        new_date = last_date + relativedelta(months=+1)
        return datetime.strftime(new_date, '%Y-%m')

    @staticmethod
    def is_target_month_over(target_month=None):
        dt_obj = datetime.strptime(target_month, '%Y-%m')
        last_day = dt_obj.replace(day = monthrange(dt_obj.year, dt_obj.month)[1])
        return datetime.now() > last_day  

    @staticmethod
    def months_in_ytd(current_year, style=None, show_choices=None):
        range_month = datetime.now().strftime('%m')
        str_month = datetime.now().strftime('%b').lower()
        date_info = monthrange(int(current_year), int(range_month))
        last_day = date_info[1]
        
        if style == 'three_letter_month':
            month_list = pd.date_range(f'{current_year}-01-01',f'{current_year}-{range_month}-{last_day}',freq='MS').strftime("%b").tolist()
            month_list = [item.lower() for item in month_list]
        else:
            month_list = pd.date_range(f'{current_year}-01-01',f'{current_year}-{range_month}-{last_day}',freq='MS').strftime("%Y-%m").tolist()
            month_list = [item for item in month_list]

        if show_choices:
            for count, item in enumerate(month_list, 1):
                print(count, item)
            choice = [{count: month} for count, month in (enumerate(month_list, 1))]
            selection = int(input('Please select an item to work with: '))
            month1 = [list(month.values())[0] for month in choice if list(month.keys())[0] == selection]     
            return month1
        return month_list

    @staticmethod
    def pickle_jar(self, target_data, pknm_string):
        # binary
        pickled = open(f'{pknm_string}.pk1', 'wb')
        pickle.dump(target_data, pickled, -1)
        pickled.close()
        pickle_name = f'{pknm_string}.pk1'
        return pickle_name
        
    @staticmethod
    def open_jar(self, pknm):
        # binary
        unpickled = open(f'{pknm}', 'rb')
        return_data = pickle.load(unpickled)
        unpickled.close()
        return return_data

    @staticmethod
    def autoconvert_xls_to_xlsx(path):
        """
        This little beaut takes a path, makes a list of .xls files, then converts those files to .xlsx automatically and sends them back to the same folder (it does not delete the old file)
        """
        xls_list = []
        xls_name = []

        for items in path.iterdir():
            file_extension = os.path.splitext(items)[1]
            file_name_no_ext = os.path.splitext(items)[0]
            if file_extension == '.xls':
                xls_list.append(items)
                xls_name.append(file_name_no_ext)

        print(xls_list)

    @staticmethod
    def pick_rent_sheet(service, workbook_id): # ie RENT_SHEETS2020
        titles_dict = get_existing_sheets(service, workbook_id)

        sheet_choice, selection = show_files_as_choices(titles_dict)
        print("You've chosen to work with sheet: " + sheet_choice, '\n')
        print('You\'ve chosen to work with month:',  selection)

        return service, sheet_choice, selection

    def find_last_modified(dir):
        time, file_path = max((file.stat().st_mtime, file) for file in dir.iterdir())
        # print(datetime.fromtimestamp(time), file_path.name)
        return file_path

    @staticmethod
    def sheet_finder(path, function):
        print('\nHelping you find a sheet!!!!!\n')
        print(f'for {function}.')

        time, file_path = max((file.stat().st_mtime, file) for file in path.iterdir())
        print(f'\nThis is most recent active file: {file_path.name} {time}\n\n')

        choice = int(input('If you want to use most recent active file PRESS 1 \n or PRESS another key to keep searching'))

        if choice == 1:
            return file_path
        else:
            path = Utils.walk_download_folder(path)
            return path

    @staticmethod
    def walk_download_folder(path, interactive=True):
        choice = []
        files = []
        choice_file = {}

        for count, file in enumerate(path.iterdir()):
            print('\n', count, "******", file.name)
            choice.append(count)
            files.append(file)

        print('\n')

        if interactive:
            selection = int(input("Please select an item to work with:"))

            choice_file = dict(zip(choice, files))

            for k, v in choice_file.items():
                if selection == k:
                    path = v
            return path

    @staticmethod
    def get_letter_by_choice(choice, offset):
        print(choice)
        if isinstance(choice, list):
            print('this is type list.')
            for item in choice:
                choice = item
                print(choice)

        letters = 'abcdefghijklm'
        choice = choice - offset
        output = letters[choice]

        return output

    @staticmethod
    def string_slicer(input, start_slice, end_slice):
        checking = isinstance(input, str)
        if checking == True:
            output = input[start_slice:end_slice]

        return output

    @staticmethod
    def load_activate_workbook(path):
        workbook = load_workbook(path)
        sheet = workbook.active
        print(f"Opening {sheet} from book {workbook} . . .  ")
        return sheet
    
    @staticmethod
    def make_sheet_names(months, year):
        shnames = [] # is list okay
        for i in range(len(months)):
            shname = months[i] + " " + year[0]
            shnames.append(shname)
        return shnames

    @staticmethod
    def make_sheet_names2(months, year):
        shnames = [] # is list okay
        for i in range(len(months)):
            shname = months[i]
            shnames.append(shname)
        return shnames

    @staticmethod
    def helper_fix_date(raw_date):    
        f_date = datetime.strptime(raw_date, '%m %Y')
        f_date = f_date.strftime('%Y-%m')
        return f_date

    @staticmethod
    def helper_fix_date_str(date_str):    
        f_date = datetime.strptime(date_str, '%Y-%d-%M')
        f_date = f_date.strftime('%d/%M/%Y').lstrip('0').replace(' 0', ' ')
        return f_date

    @staticmethod
    def helper_fix_date_str2(date_str):
        f_date = datetime.strptime(date_str, '%m/%d/%Y')
        f_date = f_date.strftime('%Y-%m-%d')
        return f_date
        
    def get_book_name(service, sh_id):
        response = service.spreadsheets().get(
            spreadsheetId=sh_id
            ).execute()

        book_name = response['properties']['title']
        return book_name
    
    @staticmethod
    def capitalize_name(tenant_list=None):
        t_list = []
        for name in tenant_list:
            new = [item.rstrip().lstrip().capitalize() for item in name.split(',')]
            t_list.append(', '.join(new))
        return t_list

    @staticmethod
    def get_existing_sheets(service, sh_id, verbose=None):
        response = service.spreadsheets().get(spreadsheetId=sh_id
            ).execute()

        if verbose:
            print("book name:", response['properties']['title'])
            print('\n')
            print("URL:", response['spreadsheetUrl'])
            print('\n')
            print("Sheet name ::::::: sheet id ")
            print("********** ::::::: *********")

        titles = []
        sheet_ids = []
        for i in range(len(response['sheets'])):
            title = (response['sheets'][i]['properties']['title'])
            sheet_id = (response['sheets'][i]['properties']['sheetId'])
            if verbose:
                print(title, '--->', sheet_id)
            titles.append(title)
            sheet_ids.append(sheet_id)
        titles_dict = dict(zip(titles, sheet_ids))

        return titles_dict

    def existing_ids(dict):
        # creates index of titles and ids
        idx = []
        sheet = []
        id = []
        sub = ()
        idx_dict = {}
        print("======================================")
        print("\n\n")
        for k, v in enumerate(dict):
            idx.append(k)
            sheet.append(v)
            id.append(dict[v])
            print(k, "***", v, "***", dict[v])

        print("\n")
        print("======================================")
        sub = tuple(zip(sheet, id))
        idx_list = list(zip(idx, sub))

        return idx_list

    @staticmethod
    def enumerate_choices_for_user_input(chlist=None):
        choices = []
        files = []
        
        for count, item in enumerate(chlist, 1):
            print(count, "****", item, '****')
            choices.append(count)
            files.append(item)
        
        selection = int(input('Please select an item to work with: '))
    
        choice_file = dict(zip(choices, files))

        for k, v in choice_file.items():
            if selection == k:
                return v, selection

    def show_files_as_choices(list, interactive=True):
        choice = []
        files = []
        choice_file = {}

        for count, (k, v) in enumerate(list.items(), 1):
            choice.append(count)
            files.append(k)

        if interactive:
            selection = int(input("Please select an item to work with:"))

            choice_file = dict(zip(choice, files))

            for k, v in choice_file.items():
                if selection == k:
                    return v, selection

    def get_letter_by_choice(choice, offset):
        print(choice)
        if isinstance(choice, list):
            print('this is type list.')
            for item in choice:
                choice = item
                print(choice)

        letters = 'abcdefghijklm'
        choice = choice - offset
        output = letters[choice]

        return output

    @staticmethod
    def decimalconv(num_as_str, places=Decimal('0.01'), round1=ROUND_HALF_UP):
        num_as_quantized_decimal = Decimal(num_as_str).quantize(places, round1)
        return num_as_quantized_decimal

    def string_to_decimal(input):

        input = input.replace(',', '')
        input = input.replace('$', '')

        checking = isinstance(input, str)
        if checking == True:
            print('Found a string . . . converting to decimal.')
            output = Decimal(input)
        else:
            print(type(input))

        return output

    def non_secure_random_number():
        import random
        random.seed(version=2)
        random_id = random.randrange(100000)
        return random_id

