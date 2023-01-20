import math
from datetime import datetime as dt

import numpy as np
import pandas as pd
import requests
from dataclasses import dataclass

from auth_work import oauth
from backend import Findexer, IncomeMonth, PopulateTable, StatusObject
from config import Config
from errors import Errors
from file_indexer import FileIndexer
from utils import Utils

@dataclass
class Deposit:
    month: str = 'empty'
    amount: str = 'empty'
    account: str = 'empty'

class AnnFin:

    service = oauth(Config.my_scopes, 'sheet')
    sheet_id = Config.rec_act_2021
    worksheet_name = Config.TEST_REC_ACT
    hap_range = Config.current_year_hap
    laundry_range = Config.current_year_laundry
    sec_dep_range = Config.current_year_sec_dep
    rr_range = Config.current_year_rr
    dim = 'COLUMNS'

    LAUNDRY_RANGE_FROM_RS = '!N71:N71'
    RR_RANGE_FROM_RS = '!D80:D80'
    SEC_DEP_RANGE_FROM_RS = '!N73:N73'
    CURRENT_YEAR_RS = Config.RS_2022
    month_match_dict = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'june': 6,
        'july': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12,
    }

    def __init__(self, db=None):
        self.populate = PopulateTable()
        self.tables = self.populate.return_tables_list()
        self.hap_code = '5121'
        self.rent_collected = '5120'
        self.laundry_code = '5910'
        self.db = db
        # self.trial_balance_2021_ye = 'trial_balance_ye_2021.xlsx'
        # self.trial_balance_2022_ye = 'Fall+Creek+Village+I_Trial+Balance.xls'
        # self.trial_balance_2022_ye = 'Fall+Creek+Village+I_Trial+Balance.xlsx'
        # self.output_path = Config.TEST_ANNFIN_OUTPUT / 'merged_trial_balance.xlsx'
        self.last_year = '2021'
        self.this_year = '2022'

    def connect_to_db(self, mode=None):
        if self.db.is_closed():
            self.db.connect()
        if mode == 'autodrop':
            self.db.drop_tables(models=self.tables)
        self.db.create_tables(models=self.tables)
       
    def receivables_actual(self):
        '''notes on canonical amounts:'''
        '''01/22: 501.71 laundry'''
        '''01/22: 15 nationwide: fixed'''
        '''02/22: 700 greiner sd: should be other at least'''
        '''02/22: 26.30 laundry'''
        '''03/22: 272.95 laundry'''
        '''04/22: 227.27 laundry'''
        '''04/22: 115.17 nationwide refund: should be other'''
        '''04/22: 92.96 laundry'''
        '''04/22: 322.26 laundry'''
        self.connect_to_db()
        path = Config.TEST_ANNFIN_PATH / 'fcv_pl_2022.xlsx'
        # directory_contents = [item for item in path.iterdir()]
        # path_to_pl = [
            # path for path in directory_contents if path.name != 'desktop.ini']

        closed_month_list = [dt.strptime(rec.month, '%Y-%m') for rec in StatusObject(
        ).select().where(StatusObject.tenant_reconciled == 1).namedtuples()]

        df = pd.read_excel(path)
        df = df.fillna('0')
        hap = self.qb_extract_pl_line(df=df, keyword=self.hap_code)
        rent_collected = self.qb_extract_pl_line(df=df, keyword=self.rent_collected)
        # laundry = self.qb_extract_pl_line(df=df, keyword=self.laundry_code)
        breakpoint()

        # laundry = self.qb_extract_pl_line(
        #     keyword=self.laundry_code, path=path_to_pl)

        # for month in closed_month_list:
        #     for date, hap_amount in hap.items():
        #         if month == date:
        #             hap_db = IncomeMonth(
        #                 year=Config.current_year, month=month, hap=hap_amount)

    def match_hap(self):
        print('attempt to match hap, send to db, and write')
        # findex = FileIndexer(path=self.path, db=self.main_db)

        op_cash_hap = [(row.hap, row.period) for row in Findexer.select().where(
            Findexer.hap != '0').namedtuples()]

    def qb_extract_pl_line(self, df=None, keyword=None):
        '''limits: cells with formulas will not be extracted properly; however, the workaround is to put an x in a cell and save and close.  we need a no-touch way to do this'''

        date_extract = df.iloc[3]
        extract = df.loc[df['Fall Creek Village I'].str.contains(keyword,
                                                                 na=False
                                                                 )]
        extract = [item for item in list(extract.values[0]) if type(item) != str]
        date_extract = [item for item in date_extract.to_list() if isinstance(item, str) == True]
        date_extract = [item for item in date_extract if item != '0']
        
        line_items = dict(zip(date_extract, extract))

        return line_items

    def qbo_cleanup_line(self, path=None, dirty_list=None):

        df = pd.read_excel(path, header=4)
        date = list(df.columns)
        date = date[1:]
        target_date_dict = dict(zip(date, dirty_list))

        line_items = {
            k: v for (k, v) in target_date_dict.items() if 'Total' not in k}

        line_items = {k: v for (k, v) in line_items.items()
                      if math.isnan(v) == False}

        line_items = {dt.strptime(k, '%b %Y'): v for (
            k, v) in line_items.items() if '-' not in k}

        return line_items

    def remainder_code(self):
        '''remainder code'''

        total = {k: v for (k, v) in target_date_dict.items() if 'Total' in k}

        dict_wo_total_and_mtd = {
            k: v for (k, v) in dict_wo_total.items() if '-' not in k}

        '''model date YYYY-MM'''
        fixed_target_date_dict = {k.strftime(
            '%Y-%M'): v for (k, v) in fixed_target_date_dict.items()}
        fixed_target_date_dict = {dateq: (0 if math.isnan(amount) else amount) for (
            dateq, amount) in fixed_target_date_dict.items()}

        return fixed_target_date_dict

    def qb_extract_security_deposit(self, filename, path=None):

        abs_file_path = os.path.join(path, filename)

        df = pd.read_excel(abs_file_path)

        df = df.loc[df['Unnamed: 2'].str.contains('Deposit', na=False)]
        dates_list = list(df['Unnamed: 1'])
        amount_list = list(df['Unnamed: 8'])
        tup_list = [(dt.strptime(dateq,  '%m/%d/%Y'), amount)
                    for dateq, amount in zip(dates_list, amount_list)]
        tup_list = [(item[0].strftime('%m %Y'), item[1]) for item in tup_list]
        sum_dict = defaultdict(float)
        for datet, amount in tup_list:
            sum_dict[datet] += amount

        return sum_dict

    def qb_extract_deposit_detail(self, filename, path=None):
        abs_file_path = os.path.join(path, filename)

        df = pd.read_excel(abs_file_path)
        df = df.loc[df['Unnamed: 2'].str.contains('Deposit', na=False)]
        # df.sum('Unammed: 8')

        '''
        dates_list = list(df['Unnamed: 1'])
        amount_list = list(df['Unnamed: 8'])

        target_date_dict = defaultdict(list)
        for dateq, amount in zip(dates_list, amount_list):
            target_date_dict[dateq].append(amount)
        '''
        print(df.head(10))

    def pick_bank_statements(choice=None, list_of_statements=None):

        for item in bank_statements_ytd:
            item2 = item.split('.')
            item2 = item2[0]
            item2 = item2.split(' ')
            join_item = ' '.join(item2[2:4])
            if choice == join_item:
                stmt_list.append(item)
                # print(item)
        return stmt_list

    def extraction_wrapper_for_transaction_detail(choice, func=None, path=None, keyword=None):

        path, files = path_to_statements(path=path, keyword=keyword)
        #date_dict_groupby_m = qb_extract_security_deposit(files[0], path=path)
        date_dict_groupby_m = func(files[0], path=path)
        result = {amount for (dateq, amount)
                  in date_dict_groupby_m.items() if dateq == choice}
        is_empty_set = (len(result) == 0)
        if is_empty_set:
            data = [0]
            return data
        else:
            data = [min(result)]
            return data

    def start_here(self):
        self.receivables_actual()

    def start_here2(self):

        choice = str(input('enter target month (mm/yyyy): '))
        choice = '01 2022'  # need to reup December qbo, right now still showing 1-29 of december
        print('you picked:', choice)
        year_choice = choice.split(' ')
        month_choice = year_choice[0]
        year_choice = year_choice[1]

        if year_choice == '2022':
            bank_stmts = Config.TEST_ANNFIN_PATH
            p_and_l = Config.TEST_ANNFIN_PATH
            path_security_deposit = Config.TEST_ANNFIN_PATH

        three_letter_month = [str(month_str) for month_str, month_int in self.month_match_dict.items(
        ) if int(month_choice) == month_int]

        titles_dict = Utils.get_existing_sheets(
            self.service, Config.CURRENT_YEAR_RS)
        target_sheet = {sheet_name for (sheet_name, sheet_id) in titles_dict.items(
        ) if three_letter_month[0] in sheet_name}
        target_sheet = min(target_sheet)
        print(target_sheet)

 
    '''

    def prep_trial_balance_dataframe(self, path=None, year=None):
        try:
            df = pd.read_excel(path, engine='openpyxl', header=4)
        except OSError as e:
            df = pd.read_excel(path, engine='xlrd',
                               sheet_name='Trial Balance', header=4)

        df = df.fillna(0)
        df.set_index('Unnamed: 0', inplace=True)
        # breakpoint()

        df[year] = df['Debit'] + df['Credit']
        df.drop('Debit', inplace=True, axis=1)
        df.drop('Credit', inplace=True, axis=1)
        return df

    def add_xlsxwriter_formatting(self, output_path=None):
        import xlsxwriter

        workbook = xlsxwriter.Workbook(output_path)
        worksheet = workbook.add_worksheet()

        # set column width
        # set conditional formatting
        # set out divisions
        # set number formatting

        currency_format = workbook.add_format(
            {'num_format': '[$$-409]#,##0.00'})

        worksheet.set_column('A:A', 35)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 20)
        worksheet.set_column('D:D', 20)
        # worksheet.write('A1', 1234.56, currency_format)

        workbook.close()

    def df_formatting_insert_row(self, df=None, target=None, index=None):
        i = df.index.get_loc(target)
        new_row = pd.DataFrame(index=[index])
        index_position = i + 1
        final = pd.concat(
            [df.iloc[:index_position], new_row, df.iloc[index_position:]])
        return final

    def trial_balance_portal(self):
        import io

        from xlwt import Workbook
        filename = Config.TEST_ANNFIN_PATH / self.trial_balance_2022_ye
        breakpoint()
        with open(filename, 'rb') as f:
            lines = [x.decode('utf8').strip() for x in f.readlines()]
            print(lines)

        file1 = io.open(filename, "r")

        for line in file1:
            print(line)
        # data = file1.readlines()

        # Creating a workbook object
        xldoc = Workbook()
        # Adding a sheet to the workbook object
        sheet = xldoc.add_sheet("Sheet1", cell_overwrite_ok=True)
        # Iterating and saving the data to sheet
        for i, row in enumerate(data):
            print(row)
            # Two things are done here
            # Removeing the '\n' which comes while reading the file using io.open
            # Getting the values after splitting using '\t'
            for j, val in enumerate(row.replace('\n', '').split('\t')):
                sheet.write(i, j, val)

        # Saving the file as an excel file
        output_path = Config.TEST_ANNFIN_OUTPUT / 'myexcel.xlsx'
        xldoc.save(output_path)
        breakpoint()
        path = Config.TEST_ANNFIN_PATH / self.trial_balance_2021_ye
        base = self.prep_trial_balance_dataframe(path, year=self.last_year)
        path2 = Config.TEST_ANNFIN_PATH / self.trial_balance_2022_ye
        new = self.prep_trial_balance_dataframe(path2, year=self.this_year)

        final = pd.merge(base, new, on='Unnamed: 0', how='outer')
        final.fillna(0)

        final = self.df_formatting_insert_row(
            df=final, target='3900 Retained Earnings', index='INCOME')
        final = self.df_formatting_insert_row(
            df=final, target='5940 Other Revenue:Forf Ten Security Deposits', index='ADMIN & OFFICE')
        final = self.df_formatting_insert_row(
            df=final, target='Operating & Maintainance:Other:Equipment Rental', index='JOURNAL ENTRIES & OTHER')

        final['variance'] = (
            round(final[self.this_year] / final[self.last_year] * 100)) - 100
        # print(final.loc['6700 Taxes & Insurance'])\

        # what do I want to do?
        # conditional formatting

        writer = Errors.xlsx_permission_error(
            self.output_path, pandas_object=pd)

        final.to_excel(writer, sheet_name='merged_tb')

        workbook = writer.book
        # Get Sheet1
        writer.close()
        worksheet = writer.sheets['merged_tb']

        cell_format = workbook.add_format()
        cell_format.set_bold()
        # cell_format.set_font_color('blue')

        worksheet.set_column('A:A', 35, cell_format)
        worksheet.set_column('B:B', 20, cell_format)
        worksheet.set_column('C:C', 20, cell_format)
        worksheet.set_column('D:D', 20, cell_format)

        # breakpoint()

        # self.add_xlsxwriter_formatting(output_path=self.output_path)
    '''



    '''
    sh_col = Liltilities.get_letter_by_choice(int(month_choice), 0)
    hap_wrange = f'{worksheet_name}!{sh_col}{hap_range}:{sh_col}{hap_range}'
    laundry_wrange =f'{worksheet_name}!{sh_col}{laundry_range}:{sh_col}{laundry_range}' 
    sec_dep_wrange =f'{worksheet_name}!{sh_col}{sec_dep_range}:{sh_col}{sec_dep_range}' 
    rr_wrange = f'{worksheet_name}!{sh_col}{rr_range}:{sh_col}{rr_range}' 

    stmt_list = []
    target_bank_stmt_path, bank_statements_ytd = path_to_statements(path=bank_stmts, keyword='op cash')
    target_report = pick_bank_statements(choice=choice, list_of_statements=bank_statements_ytd)
    dateq, hap_stmt = nbofi_pdf_extract_hap(target_report[0], path=target_bank_stmt_path)
    
    target_pl_path, profit_and_loss_ytd = path_to_statements(path=p_and_l, keyword='Profit')

    hap_date_dict = qb_extract_p_and_l(profit_and_loss_ytd[0], keyword='5121', path=target_pl_path)
    for dateh, amount in hap_date_dict.items():
        if dateh == choice:
            hap_qbo = amount

    if hap_stmt == hap_qbo:
        data = [hap_stmt]
        simple_batch_update(service, sheet_id, hap_wrange, data, dim)
    else:
        print('hap does not balance between rs and qbo.')
        print('hap from bank', hap_stmt, '|', type(hap_stmt), 'rr from qb=', hap_qbo, '|', type(hap_qbo))
        simple_batch_update(service, sheet_id, hap_wrange, [100000000], dim)  

    # get laundry_income_rs
    laundry_income_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + LAUNDRY_RANGE_FROM_RS)
    laundry_income_rs = float(laundry_income_rs[0][0])

    laundry_date_dict = qb_extract_p_and_l(profit_and_loss_ytd[0], keyword='5910', path=target_pl_path)
    for dateq, amount in laundry_date_dict.items():
        if dateq == choice:
            laundry_income_qbo = float(amount)

    if laundry_income_rs == laundry_income_qbo:
        simple_batch_update(service, sheet_id, laundry_wrange, [laundry_income_rs], dim)
    else:
        print('laundry does not balance between rs and qb')
        print('laundr rs=', laundry_income_rs, '|', type(laundry_income_rs, 'laundry qb=', laundry_income_qbo, '|', type(laundry_income_qbo)))

     # sec dep
    sec_dep_qb = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_security_deposit, path=path_security_deposit, keyword='Security')
    sec_dep_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + SEC_DEP_RANGE_FROM_RS)

    if float(sec_dep_rs[0][0]) == float(sec_dep_qb[0]):
        simple_batch_update(service, sheet_id, sec_dep_wrange, sec_dep_qb, dim)
    else:
        print('sec_dp does not balance between rs and qb.  Have I adjusted on rs.')
        print('sd rs=', sec_dep_rs, '|', type(sec_dep_rs), 'sd qb=', sec_dep_qb, '|', type(sec_dep_qb))

    ## rr from qbo
    rr_qbo = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_security_deposit, path=Config.rr_2021, keyword='rr')
    ## rr from rent_sheets (see above)
    rr_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + RR_RANGE_FROM_RS)
    rr_rs = float(rr_rs[0][0])

    if rr_rs == rr_qbo:
        simple_batch_update(service, sheet_id, rr_wrange, rr_rs, dim)
    else:
        print('rr does not balance between rs and qbo.')
        print('rr from rs=', rr_rs, '|', type(rr_rs), 'rr from qb=', rr_qbo, '|', type(rr_qbo))
        print('WRITING PLUG PENDING JANUARY STATEMENTS AND ABILITY TO WORK ON LIVE DATA')
        simple_batch_update(service, sheet_id, rr_wrange, [100000000], dim)
    # deposit detail from qbo: need a group by swing here

    data = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_deposit_detail, path=Config.deposit_detail_2021, keyword='deposit')
    print(data)

    print('joe')
    '''
