from __future__ import print_function
# from utils import get_existing_sheets, show_files_as_choices, sheet_finder
from utils import Utils
from receipts import RentReceipts
from pprint import pprint
from datetime import datetime, timedelta, date
from config import RENT_SHEETS2022, CURRENT_YEAR_RS, READ_RANGE_HAP, READ_RANGE_PAY_PRE, R_RANGE_INTAKE_UNITS, my_scopes
from config import Config
from auth_work import oauth
from oauth2client.service_account import ServiceAccountCredentials
import sys
import os
import pickle
import os.path
from time import sleep
import copy
import pandas as pd
import numpy as np
from pathlib import Path
from openpyxl import Workbook, load_workbook
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from app.models import db, Unit, BasicBitchPayment

class UI(object):

    def __init__(self):
        pass

    def prompt(self, *args):
        for lines in args:
            pprint(lines)
        input1 = int(input('....>'))
        return input1
    # make this kwargs

class FileHandler(object):

    def __init__(self):
        self.spreadsheet_testbook = CURRENT_YEAR_RS #wb name: rent sheets 2020

class TemplateFormatSheet(object):

    def __init__(self):
        self.service = ''
        self.spreadsheet_id = ''
        self.read_range = ''

    def set_id(self, service, spreadsheet_id, read_range):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.read_range = read_range

    # alt get implementation: you can pass in service and sheet (add majDim)
    def alt_batch_get(self, service, sheet_id, range):
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=range).execute()
        values = []
        values_ = result.get('values', [0])
        print('values:', values)
        try:
            for value in values_:
                print('value:', value)
                values.append(value[0])
        except TypeError as e:
            print(e, 'You probably need to put in the unit numbers into intake sheet.')
        return values

    # an attempt to write ints for sum formulas
    def update_int(self, data, write_range):
        service = self.service
        sheet = service.spreadsheets()
        spreadsheet_id = self.spreadsheet_id

        range_ = write_range  # TODO: Update placeholder value.

        # How the input data should be interpreted.
        value_input_option = 'USER_ENTERED'  #

        value_range_body = {"range": write_range,
                            "majorDimension": "COLUMNS",
                            "values": [data]
        }

        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()

        print(response)

class BookFormat(object):
    def __init__(self):

        self.service = ''
        self.spreadsheet_id = ''
        self.read_range = '' # from intake!!!!!!1
        self.shmonth = []
        self.shnames = []
        self.shyear = []

    def set_id(self, service, spreadsheet_id, read_range):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.read_range = read_range
        self.shmonths = ['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']
        self.shyear = [f'{Config.current_year}']
        self.shnames = None
        self.book = 'rent sheets'

    def existing_ids(self, dict):
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

    def make_sheet_names(self):
        "Making sheet names from list . . . "

        shnames = [] # is list okay
        for i in range(len(self.shmonths)):
            shname = self.shmonths[i] + " " + self.shyear[0]
            shnames.append(shname)
        self.shnames = shnames # keeps updating internal to class
        return shnames

    def make_new_book(self, title):
        service = self.service
        data = {'properties': {'title': title}
                }
        request = service.spreadsheets().create(body=data)
        response = request.execute()
        print(response)

    @staticmethod
    def make_sheets_from_shnames(shnames):
        for item in shnames:
            pprint(item)
            pprint("Taking a 30 second nap to preserve writes.  zzz....")
            sleep(30)
            pprint("Waking up . . . ")
            self.make_one_sheet(item)
        return shnames

    def make_title_list(self, shnames, titles_dict):
        print("Making clean titles list . . . ")
        titles2 = []
        print(shnames)
        for title in titles_dict.keys() :
            for shname in shnames:
                if title == shname:
                    titles2.append(shname)
        print(titles2)
        return titles2

    def clean_title_dict(self, shnames, titles_dict):
        """Validates list of sheets against titles
        prevents formatting of sheets that aren't on the list"""

        print("Cleaning titles_dict . . . ")
        t = []
        id = []
        for title in titles_dict.keys() :
            for shname in shnames:
                if title == shname:
                    t.append(shname)
                    id.append(titles_dict[title])
        clean_titles_dict = dict(zip(t, id))
        return clean_titles_dict

class DBIntake(object):

    def __init__(self):
        self.path = Config.RS_DL_FILE_PATH
        # self.dir_hrig =  Path('C:/Users/V/Google Drive/pythonwork/projects/fcv_fin/download_here')
        self.dir = None
        self.ytd_dep_book = '1vBtGLAYOIIraEiWGE-RxxnarSRSpTJr7DdsmfLXTxQs'
        self.header_names = ['Unit', "Name", "Payment_String", "Payment_Float", "Date", "Date_Code", "Trxn Id(Date_Code + Count)",]
        self.wrange_unit =  f'{Config.current_year}!A2:A200'
        self.wrange_name =  f'{Config.current_year}!B2:B200'
        self.wrange_pay =   f'{Config.current_year}!C2:C200'
        self.wrange_pay_f = f'{Config.current_year}!D2:D200'
        self.date =         f'{Config.current_year}!E2:E200'
        self.datecode =     f'{Config.current_year}!F2:F200'
        self.wrange_tid =   f'{Config.current_year}!G2:G200'
        self.wrange_id =    f'{Config.current_year}!A1:Z100'

    def load_activate_workbook(self, path):
        workbook = load_workbook(path)
        sheet = workbook.active
        pprint(f"Opening {sheet} from book {workbook} . . .  ")
        return sheet

    def make_dt(self, dict):
        pprint("Converting dates to dt and inserting in dict . . . ")
        for k, v in dict.items():
            if v['date'] != None:
                v['dt_date'] = [datetime.datetime.strptime(v['date'], "%m/%d/%Y")]
            else:
                print("NoneType not converted to datetime")

        return dict

    def containment_count(self, pay_list):
        pprint("Checking pay_list count . . .")
        total_items = 0

        for i in pay_list:
            total_items += 1

        pprint(f"Found {total_items} total_items instances in payments. Writing this to sheet . . .")

    def containment_tally(self, pay_list):
        pprint("Tallying payments for containment . . .")
        total_tally = 0
        for payment in pay_list:
            total_tally += payment

        pprint(f"Tallied {total_tally} in payments list.  Writing this to sheet . . . ")

    def keetchen_saink(self, sheet): 
        """This is a custom job; tailored to the specific format of sheet for realpage deposits"""

        pprint("Using openpyxl to iterate through rows of excel sheet . . .")
        pprint("Generating lists of transactions et al from sheet . . . ")

        bde = []
        unit=[] #1
        name=[] #3
        date=[] #7
        pay=[] #13

        pay_float = []

        for value in sheet.iter_rows(min_row=11, min_col=1, max_col=14, values_only=True):

            if value[1] != None:
                bde.append(value[1])
                unit.append(value[0])
                name.append(value[2])
                date.append(value[5])
                pay.append(value[13])

            else:
                print(value)
                print("******")

        print("******")
        pay_float = [float(item) for item in pay]
        dt_code = [item[0:2]for item in date] # makes date code
        print("******")

        return bde, unit, name, date, pay, pay_float, dt_code

    def intake_saink(self, sheet):
        """a modification of keetchen_saink: iterates through excel sheet and writes output to rent sheets 2020/intake in g sheets"""
        pprint("Using openpyxl to iterate through rows of excel sheet . . .")
        pprint("Generating lists of transactions et al from sheet . . . ")

        tenantname = []
        unit=[] #1
        Krent=[] #3
        actualsub=[] #7
        Trent=[] #13

        for value in sheet.iter_rows(min_row=11, min_col=1, max_col=14, values_only=True):

            if value[1] != None:
                unit.append(value[0])
                tenantname.append(value[3])
                Krent.append(value[6])
                actualsub.append(value[11])
                Trent.append(value[10])

            else:
                print(value)
                print("******")

        return tenantname, unit, Krent, actualsub, Trent

    def date_formatcol(self, service, ss_id, sheet_id, scol, endcol): # attempt to programmatically change date col format MAY NOT NEED
        sheet = service.spreadsheets()
        spreadsheet_id = ss_id

        requests = [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 100,
                            "startColumnIndex": scol,
                            "endColumnIndex": endcol
                },
                        "cell":  {
                            "userEnteredFormat": {
                                    "numberFormat": {
                                        "type": "DATE",
                                        "pattern": "mm/d/y"
                                }
                                }
                            },
                        "fields": "userEnteredFormat.numberFormat"
                        }
                        },
                ]

        body = {"requests": requests}
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body).execute()
        pprint(response)

    @staticmethod
    def Import2YTD_Deposits():
        """This function takes excel file in temp_path and pushes it formatted
        to YTD_DEPOSIT_BOOK 2020!"""
        path2 = Config.RS_DL_FILE_PATH

        pprint("Importing payments, be sure you have target read path aligned . . .")
        intake = DBIntake()
        Liltilities.autoconvert_xls_to_xlsx(path=path2)
        
        path = sheet_finder('find deposit detail report for upload', path2)
        sheet = Liltilities.load_activate_workbook(path)

        bde, unit, name, date, pay, pay_float, dt_code = intake.keetchen_saink(sheet) # where the magic happens
        
        print("Dropping fcv_fin tables and MAKING ANEW . . . ")
        db.metadata.drop_all(db.engine, tables=[BasicBitchPayment.__table__,])
        db.create_all()
        count = 0
        for n, u, p, pf, dc in zip(name, unit, pay, pay_float, dt_code):
            date_code = dc + "-" + str(count)
            bbp = BasicBitchPayment(name=n, unit1=u, payment=p, pay_float=pf, date_code=date_code)
            db.session.add(bbp)
            db.session.commit()
            count += 1

        '''
        '''
        service = oauth(my_scopes, 'sheet')        
        units =  broad_get(service, CURRENT_YEAR_RS, range=f'jan {Config.current_year}!A2:A68') 
        
        unit_check = Unit.query.all()
        if len(unit_check) == 67:
            print(f'unit db has length {len(unit_check)}')
        else:
            try:
            # '''unit loader if db loss '''
                db.metadata.drop_all(db.engine, tables=[Unit.__table__,])
                db.create_all()
                for unit_no in units:
                    print('loading units into units db')
                    unit_no = unit_no.pop()
                    print(unit_no)
                    print(type(unit_no))
                    u = Unit(unit_no=unit_no)
                    db.session.add(u)
                    db.session.commit()
            except Error as e:
                print('issue is probably with an empty unit database', e)
       
    @staticmethod
    def DB2RS():
        # TODO
        # make a list of tenant names
        # if payee is not on list of tenant names, then we use

        service = oauth(my_scopes, 'sheet')
        format = TemplateFormatSheet()
        format.set_id(service, '1vBtGLAYOIIraEiWGE-RxxnarSRSpTJr7DdsmfLXTxQs', '2020!A2:Z100') #dbintake here
        pd.set_option('display.max_rows', None) #makes df print the entire dataframe

        """sheet select"""
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        sheet_choice, selection = show_files_as_choices(titles_dict)
        sheet_choice1 = f'{sheet_choice}!K2:K68'
        print("You've chosen to work with: " + sheet_choice1)

        """month selector"""
        ui_pick = input(f'You picked {selection} as your month choice.  If this is correct, press j, otherwise enter the 2 digit month code you would like to use. >>>')
        if ui_pick == 'j':
            month_pick = str(selection)
        else:
            month_pick = str(ui_pick)

        r = BasicBitchPayment.query.filter(BasicBitchPayment.date_code.contains(month_pick)).all()

        num_list = []
        tenant_list = [] 
        pay_list = [] 
        date_code = []
        for item in r:
            num_list.append(item.unit1)
            tenant_list.append(item.name)
            pay_list.append(item.pay_float)
            date_code.append(item.date_code)
  

        def conv(list1, type=None):
            for item in list1:
                if type == 'str':
                    yield str(item)
                else:               
                    yield float(item)
    
        # this is the contents of the database selected by date_code and arranged
        tuple_from_db = tuple(zip(tenant_list, tuple(zip(num_list, conv(pay_list), date_code))))

        # checks payment grand total: does it match sheet? 
        tally_check = 0
        new_list = []
        for count, it in enumerate(tuple_from_db, 1):
            tally_check += float(it[1][1])
            new_list.append(it)

        # pulls out non_tenant_payments: like laundry
        non_tenant_payments = 0.0
        tenant_payments_subtotal = 0.0
        for it in tuple_from_db:
            if it[0] == 'Laundry Income, Laundry Income' or it[0][0] is None:
                non_tenant_payments += float(it[1][1])
            else:
                tenant_payments_subtotal += float(it[1][1])

        df = pd.DataFrame(new_list, columns =['Name', 'Other']) 
        df[['Unit', 'Payment', 'Date Code']] = pd.DataFrame(df['Other'].tolist(), index=df.index)
        df = df.drop(labels='Other', axis=1)
        df1 = df.groupby(['Name', 'Unit'])['Payment'].sum()
    
      
        # unit_idx is just a list of unit names as strings ie CD-A
        unit_idx = list(conv(Unit.query.all(), type='str'))

        final_list = []
        idx_list = []
        for index, unit in enumerate(unit_idx): # indexes units from sheet
            idx_list.append(int(index))
            final_list.append(unit)

        unit_index = tuple(zip(idx_list, final_list))

        unit_index_df = pd.DataFrame(unit_index, columns= ['Rank', 'Unit'])
           
        merged_df = pd.merge(df1, unit_index_df, on='Unit', how='outer')
        df2 = merged_df.sort_values(by='Rank', axis=0)
        df2 = df2.fillna(0)
        payment_list = df2['Payment'].tolist()
     
        format.simple_batch_update(service, FileHandler().spreadsheet_testbook, sheet_choice1, payment_list, "COLUMNS") # this is the write range
    
        print(f'Total items packed from db: {count}')
        print(f'Total items amount: {tally_check}')
        print(f'Total ntp amount: {non_tenant_payments}')
        print(f'Total ntp amount: {tenant_payments_subtotal}')
    

def reconciliation_runtime():

    input1 = int(input('Press 1 for real page xlsx sheet to postgres_db\nPress 2 for db to rent sheets'))
    
    if input1 == 1:
        DBIntake.Import2YTD_Deposits()

    elif input1 == 2:
        DBIntake.DB2RS()

def annual_formatting():
    if input == 1:
        pprint(f"Showing all current sheets in {CURRENT_YEAR_RS}")
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS, verbose=True) # show all current sheets
    elif input == 2:
        pprint(f"These are existing sheets in {CURRENT_YEAR_RS}.  Which sheet would you like to delete?")
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        idx_list = bookformat.existing_ids(titles_dict)
        choice = ui.prompt("Please select an index number to delete:")
        picked_sheet = idx_list[choice]

        print(f"You selected {picked_sheet[1][0]} with id {picked_sheet[1][1]}\n")
        bookformat.del_one_sheet(picked_sheet[1][1])
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        annual_options(service) # show  # delete sheets by id, shows list of sheets
    elif input == 3:
        pprint(f"Making sheet names programmatically & creating sheets . . . ")
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        pprint(titles_dict)
        shnames = bookformat.make_sheet_names() # makes all sheet names for year
        bookformat.make_sheets_from_shnames()
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        pprint(titles_dict) # create sheet names list: MUST BE DONE B4 running all
    elif input == 4:
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        pprint(titles_dict)
        titles = bookformat.make_title_list(shnames, titles_dict) # keep titles as list, used quite a lot
        prev_bal_dict = {}

        def titles_shifted_back(titles):
            """Fixes issue with locating previous month to current sheet"""
            pprint("Creating list of previous month . . .")
            prev_month = copy.deepcopy(titles)

            prior_titles = prev_month.pop()
            math = prior_titles.split()
            string = math[0]
            math = int(math[1])
            math -= 1
            math = string + " " + str(math)
            prev_month.insert(0, math)
            prev_bal_dict = dict(zip(titles, prev_month))
            return prev_bal_dict

        prev_bal_dict = titles_shifted_back(titles)
        pprint(prev_bal_dict)
        pprint(titles)

        for k, v in prev_bal_dict.items():
            if item == k:
                G_SHEETS_PREVIOUS_BALANCE = [f"='{v}'!L2"]
                format.wri  te_formula_column(G_SHEETS_PREVIOUS_BALANCE, f'{k}!D2:D2')
                pprint(f"Writing to {k} the value {v} . . .  ")
            else:
                pprint("Noddamach!")

            pprint("Taking a 101 second nap to preserve writes.  zzz....")
            sleep(101) # was 101
            pprint("Waking up . . . ")



