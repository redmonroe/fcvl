import math
from dataclasses import dataclass
from datetime import datetime as dt
from operator import attrgetter

import pandas as pd
import requests

from auth_work import oauth
from backend import (Findexer, IncomeMonth, NTPayment, PopulateTable,
                     StatusObject)
from config import Config
from errors import Errors
from file_indexer import FileIndexer
from peewee import fn
from utils import Utils
from google_api_calls_abstract import GoogleApiCalls


@dataclass(frozen=True, eq=True)
class RecordItem:
    account: str = 'empty'
    month: str = 'empty'
    amount: str = 'empty'


@dataclass(frozen=True, eq=True, order=True)
class ReconcileItem:
    account: str = 'empty'
    month: str = 'empty'
    amount: str = 'empty'
    discrepancy: str = 'empty'
    reconciled: bool = False

    def reconcile_hap(self, qb_side_iter=None, db_side_iter=None):
        qb_side_iter = [RecordItem(account='laundry', month='2022-01', amount=501.71), RecordItem(account='laundry', month='2022-02', amount='0')]
        db_side_iter = [RecordItem(account='laundry', month='2022-01', amount=501.71), RecordItem(account='laundry', month='2022-02', amount=26.3),]
        
        matches = [ReconcileItem(account=report.account,
                                 month=report.month,
                                 amount=str(report.amount),
                                 discrepancy=str(0),
                                 reconciled=True) for report in set(qb_side_iter) & set(db_side_iter)]
        
        differences = set(qb_side_iter) - set(db_side_iter)
        
        

        # differences = list(set([ReconcileItem(account=report.account,
        #                                       month=report.month,
        #                                       amount='0',
        #                                       discrepancy=str(
        #                                           round(abs(float(report.amount) - float(record.amount)), 2)),
        #                                       reconciled=False)
        #                         for report, record in zip(db_side_iter, qb_side_iter)]))
        # differences = [record for record in differences if record.discrepancy != '0.0']
        breakpoint()
        return matches + differences


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

    def __init__(self, db=None, full_sheet=None, mode=None, test_service=None):
        self.populate = PopulateTable()
        self.tables = self.populate.return_tables_list()
        self.reconciler = ReconcileItem()
        self.gc = GoogleApiCalls()
        self.hap_code = '5121'
        self.rent_collected = '5120'
        self.laundry_code = '5910'
        self.db = db
        self.full_sheet = full_sheet
        if mode == 'testing':
            self.service = test_service
        else:
            self.service = oauth(Config.my_scopes, 'sheet')
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

        '''
        download this report: https://app.qbo.intuit.com/app/reportv2?token=PANDL&show_logo=false&date_macro=lastyear&low_date=01/01/2022&high_date=12/31/2022&column=monthly&showrows=active&showcols=active&subcol_pp=&subcol_pp_chg=&subcol_pp_pct_chg=&subcol_py=&subcol_py_chg=&subcol_py_pct_chg=&subcol_py_ytd=&subcol_ytd=&subcol_pct_ytd=&subcol_pct_row=&subcol_pct_col=&subcol_pct_inc=false&subcol_pct_exp=false&cash_basis=no&collapsed_rows=&edited_sections=false&divideby1000=false&hidecents=false&exceptzeros=true&negativenums=1&negativered=false&show_header_title=true&show_header_range=true&show_footer_custom_message=true&show_footer_date=true&show_footer_time=true&show_footer_basis=true&header_alignment=Center&footer_alignment=Center&show_header_company=true&company_name=Fall%20Creek%20Village%20I&collapse_subs=false&title=Profit%20and%20Loss&footer_custom_message=
        
        vanilla profit and loss, by month
        
        save as: fcv_pl_YYYY.xlsx AND resave to .xls
        
        supported reconciliations:
            -  hap
        
        
        '''
        self.connect_to_db()
        path = Config.TEST_ANNFIN_PATH / 'fcv_pl_2022.xls'

        closed_month_list = [str(rec.month) for rec in StatusObject(
        ).select().where(StatusObject.tenant_reconciled == 1).namedtuples()]

        df = pd.read_excel(path)
        df = df.fillna('0')

        # p and l side from QUICKBOOKS
        hap_qb = self.qb_extract_pl_line(
            name='hap', df=df, keyword=self.hap_code)
        # rent_collected_qb = self.qb_extract_pl_line(name='collected_rent', df=df, keyword=self.rent_collected)
        laundry_qb = self.qb_extract_pl_line(
            name='laundry', df=df, keyword=self.laundry_code)

        # database, rs, and docs side
        supported_list = ['hap', 'laundry', 'collected_rent']
        results = []
        for name in supported_list:
            # if name == 'collected_rent':
            #     for month in closed_month_list:
            #         rent_collected = self.gc.broad_get(self.service, self.full_sheet, f'{month}!K69')
            #         rents_db.append(RecordItem(account=name, month=month, amount=rent_collected[0][0]))
            #     result = self.compare(name=name, qb_side_iter=rent_collected_qb, db_side_iter=rents_db)
            if name == 'hap':
                hap_db = [RecordItem(account=name, month=row.period, amount=float(row.hap)) for row in Findexer.select().
                          where(attrgetter(name)(Findexer) != '0')]
                results.append((name, self.reconciler.reconcile_hap(
                    qb_side_iter=hap_qb, db_side_iter=hap_db)))

            if name == 'laundry':
                laundry_db = [RecordItem(account=name, month=dt.strftime(row.date_posted, '%Y-%m'), amount=float(row.amount)) for row in NTPayment.select(
                    fn.SUM(NTPayment.amount), NTPayment.date_posted).
                    group_by(fn.strftime('%Y-%m', NTPayment.date_posted)).
                    where(attrgetter('payee')(NTPayment) == 'laundry')]
                results.append((name, self.reconciler.reconcile_hap(
                    qb_side_iter=laundry_qb, db_side_iter=laundry_db)))

        self.to_stdout(list_of_tup=results)
        # for month in closed_month_list:
        #     for date, hap_amount in hap.items():
        #         if month == date:
        #             hap_db = IncomeMonth(
        #                 year=Config.current_year, month=month, hap=hap_amount)

    def reconciler(self, name=None, qb_side_iter=None, db_side_iter=None):
        return self.reconciler.reconcile_hap(
            qb_side_iter=qb_side_iter, db_side_iter=db_side_iter)

    def to_stdout(self,  list_of_tup=None):
        print('\n')
        for item in list_of_tup:
            print(f'{item[0]}')
            print('month:     ',  f'\n'.join(
                [''.join(['{:9}'.format(str(reconciled.month)) for reconciled in sorted(item[1])])]))
            print('reconciled?:',f'\n'.join(
                [''.join(['{:9}'.format(str(reconciled.reconciled)) for reconciled in sorted(item[1])])]))
            print('discrepancy?:',f'\n'.join(
                [''.join(['{:8}'.format(str(reconciled.discrepancy)) for reconciled in sorted(item[1])])]))

    def qb_extract_pl_line(self, name=None, df=None, keyword=None):
        '''limits: cells with formulas will not be extracted properly;
        however, the workaround is to put an x in a cell and save and close.  
        we need a no-touch way to do this'''

        date_extract = [Utils.helper_fix_date_str4(
            item) for item in df.iloc[3] if item != '0']
        date_extract = [item for item in date_extract if item != 'Total']

        extract = df.loc[df['Fall Creek Village I'].str.contains(
            keyword, na=False)]
        try:
            extract = [item for item in list(
                extract.values[0]) if type(item) != str]
        except IndexError as e:
            print(f'{e}, no info found for account: {name}')
            report_items = [RecordItem(
                account=name, month=date, amount='0') for date in date_extract]
            return report_items

        extract = [item for item in extract if item != 'Total']
        group = dict(zip(date_extract, extract))
        report_items = [RecordItem(account=name, month=date, amount=float(
            amount)) for date, amount in group.items()]
        return report_items

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
