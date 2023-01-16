import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend import Findexer, PopulateTable, Reconciler, StatusObject, DryRunRentRoll
from config import Config
from pdf import StructDataExtract
from persistent import Persistent
from reconciler import Reconciler
from utils import Utils


class Scrape:

    def get_period_from_scrape_fn_and_mark_in_findexer(self):
        """this just provides row in findexer db & marks as processed; does not mark_scrape reconciled as True or file out hap, tenant, etc cols"""
        for item, doc_id in self.indexed_list:
            scrape_file = Findexer.get(Findexer.doc_id == doc_id)
            date_str = [part.split('_') for part in item.split('/')][-1][-2]
            date_str = '-'.join(date_str.split('-')[0:2])
            scrape_file.status = self.status_str.processed
            scrape_file.period = date_str
            scrape_file.doc_type = 'scrape'
            scrape_file.save()

    def load_scrape_data_historical(self):
        """this function gets scrape files and period via type=='scrape in Findexer table and updates row for various items(deposit sum, hap sum, replacement res. sum, corrections and chargebacks sum et al) and makes sure deposits assert before proceeding"""

        scrapes = [(row.path, row.period) for row in Findexer.select().where(
            Findexer.doc_type == 'scrape').namedtuples()]

        for scrape_path, scrape_period in scrapes:
            scrape_txn_list = self.load_directed_scrape(
                path_to_scrape=scrape_path, target_date=scrape_period)

            dep_sum = sum(
                [item['amount'] for item in scrape_txn_list if item['dep_type'] == 'deposit'])

            hap_sum = sum([item['amount']
                          for item in scrape_txn_list if item['dep_type'] == 'hap'])

            rr_sum = sum([item['amount']
                         for item in scrape_txn_list if item['dep_type'] == 'rr'])

            corr_sum = sum(
                [item['amount'] for item in scrape_txn_list if item['dep_type'] == 'corr'])

            dep_list = [{item['date']:item['amount']}
                        for item in scrape_txn_list if item['dep_type'] == 'deposit']

            check = float(sum([Utils.decimalconv(str(item['amount']))
                          for item in scrape_txn_list]))
            sum_of_parts = dep_sum + hap_sum + corr_sum + rr_sum

            Reconciler.findexer_assert_scrape_catches_all_target_txns(
                period=scrape_period, sum_of_parts=sum_of_parts, check=check)

            target_scr_id = [row for row in Findexer.select().where(
                (Findexer.period == scrape_period) &
                (Findexer.doc_type == 'scrape')).namedtuples()][0]

            scrape_to_update = Findexer.get(target_scr_id.doc_id)
            scrape_to_update.corr_sum = corr_sum
            scrape_to_update.hap = hap_sum
            scrape_to_update.depsum = dep_sum
            scrape_to_update.rr = rr_sum
            scrape_to_update.deplist = dep_list
            scrape_to_update.save()

    def load_directed_scrape(self, path_to_scrape=None, target_date=None):

        populate = PopulateTable()
        df = self.get_df_of_scrape(path=path_to_scrape)
        scrape_txn_list = self.get_targeted_rows_for_scrape(scrape_df=df)
        populate.load_scrape_to_db(
            deposit_list=scrape_txn_list, target_date=target_date)

        return scrape_txn_list

    def load_mm_scrape(self, list1=None):
        """
        This function runs through main file path and finds most recent scrape and select lines containing 'DEPOSIT', 'QUADEL', and 'CHARGEBACK to get tenant deposits, quadel, and deposit corrections

        returns a list of dicts
        """
        mr_dict = {}
        for fn in self.scrape_path.iterdir():
            if fn.suffix == '.csv' and fn.name not in self.excluded_file_names:
                mr_dict[fn] = fn.lstat().st_ctime

        most_recent_scrape = self.get_most_recent_scrape(
            most_recent_dict=mr_dict)

        return self.get_targeted_rows_for_scrape(scrape_df=most_recent_scrape)

    def get_most_recent_scrape(self, most_recent_dict=None):
        return pd.read_csv(max(list(most_recent_dict)))

    def get_df_of_scrape(self, path=None):
        return pd.read_csv(path)

    def adjust_deposit_date(self, date_str=None):
        if '-' in [letter for letter in date_str]:
            return date_str
        elif '/' in [letter for letter in date_str]:
            return Utils.helper_fix_date_str2(date_str)
        else:
            print('issue converting date in scrape sheet from bank')
            exit

    def get_targeted_rows_for_scrape(self, scrape_df=None):
        deposit_list = []
        corr_count = 0
        for index, row in scrape_df.iterrows():
            if row['Description'] == 'DEPOSIT':
                dict1 = {}
                dict1 = {'date': self.adjust_deposit_date(
                    row['Processed Date']), 'amount': row['Amount'], 'dep_type': 'deposit'}
                deposit_list.append(dict1)
            if 'INCOMING' in row['Description']:
                dict1 = {}
                dict1 = {'date': row['Processed Date'],
                         'amount': row['Amount'], 'dep_type': 'rr'}
                deposit_list.append(dict1)

            if 'QUADEL' in row['Description']:
                dict1 = {}
                dict1 = {'date': row['Processed Date'],
                         'amount': row['Amount'], 'dep_type': 'hap'}
                deposit_list.append(dict1)

            '''following branches are for finding corrections'''

            if 'CHARGEBACK' in row['Description']:
                if 'FEE' not in row['Description']:
                    corr_count += 1
                    dict1 = {}
                    dict1 = {'date': row['Processed Date'],
                             'amount': row['Amount'], 'dep_type': 'corr'}
                    deposit_list.append(dict1)

            if 'CORRECTION' in row['Description']:
                corr_count += 1
                dict1 = {}
                dict1 = {'date': row['Processed Date'],
                         'amount': row['Amount'], 'dep_type': 'corr'}
                deposit_list.append(dict1)

        if corr_count == 0:
            dict1 = {'date': row['Processed Date'],
                     'amount': 0, 'dep_type': 'corr'}
            deposit_list.append(dict1)
        else:
            print('Warning: deposit correction has been processed')

        return deposit_list


class FileIndexer(Utils, Scrape, Reconciler):

    query_mode = Utils.dotdict({'xls': ('raw', ('.xls', '.xlsx')), 'pdf': (
        'raw', ('.pdf', '.pdf')), 'csv': ('raw', ('.csv', '.csv'))})
    rent_format = {'get_col': 0, 'split_col': 11,
                   'split_type': ' ', 'date_split': 2}
    deposits_format = {'get_col': 9, 'split_col': 9,
                       'split_type': '/', 'date_split': 0}
    style_term = Utils.dotdict({'rent': 'rent', 'deposits': 'deposits', 'opcash': 'opcash', 'hap': 'hap', 'r4r': 'rr',
                               'dep': 'dep', 'dep_detail': 'dep_detail', 'corrections': 'corrections', 'chargebacks': 'chargeback'})
    bank_acct_str = ' XXXXXXX1891'
    excluded_file_names = ['desktop.ini', 'beginning_balance_2022.xlsx']
    target_string = Utils.dotdict({'affordable': 'Affordable Rent Roll Detail/ GPR Report', 'bank': 'BANK DEPOSIT DETAILS',
                                  'quadel': 'QUADEL', 'r4r': 'Incoming Wire', 'oc_deposit': 'Deposit', 'corrections': 'Correction', 'chargebacks': 'Chargeback'})
    status_str = Utils.dotdict({'raw': 'raw', 'processed': 'processed'})

    def __init__(self, path=None, db=None, mode=None):
        self.path = path
        self.db = db
        self.mode = mode
        self.unproc_files1 = []
        self.unfinalized_months = []
        self.index_dict = {}
        self.index_dict_iter = {}
        self.indexed_list = []
        self.op_cash_list = []
        self.create_findex_list = [Findexer]
        self.pdf = StructDataExtract()
        self.scrape_path = Config.TEST_PATH

    def incremental_filer(self, pytest=False):
        print('incremental_filer(), FileIndexer method from file_indexer.py')
        print('\n')
        self.connect_to_db()

        print('showing unfinalized months: either no opcash, no tenant, or no rent_sheet write')

        """A FINALIZED MONTH ==
            1 OPCASH processed 
            2 DEPOSITS + ADJUSTMENTS == TENANT_PAY + NTP"""

        months_ytd, unfin_month, final_not_written = self.test_for_unfinalized_months()

        for item in unfin_month:
            print(item)

        """NEXT: is there anything to process?  ie are there new files in the path"""

        print('\n')
        unproc_files, dir_contents = self.test_for_unprocessed_file()

        if unproc_files == []:
            print('no new files to add')
            return [], unfin_month, final_not_written
        else:
            """YES, THERE ARE NEW FILES IN THE PATH"""
            for count, item in enumerate(unproc_files, 1):
                print(count, item)

            print('\n')

            if pytest is False:
                choice1 = int(input(
                    'running findexer now would input the above file(s)?  press 1 to proceed ... '))
            else:
                choice1 = 1

            if choice1 == 1:
                print('YES, I WANT TO ADD THIS FILE FINDEXER DB')
                self.index_dict = self.sort_directory_by_extension2()
                self.load_what_is_in_dir_as_indexed(dict1=self.index_dict_iter)
                self.runner_internals()
                new_files = self.get_report_type_from_xls_name(
                    records=self.index_dict)
                new_files = self.get_date_from_file_name(records=new_files)
                self.findex_reconcile_onesite_deposits_to_scrape_or_oc()
                return new_files, self.unfinalized_months, final_not_written
            else:
                print('exiting program from incremental_filer()')
                exit

    def incremental_filer_sub_1_for_dry_run(self, *args, **kwargs):
        # TODO: missing scrape report from nbofi flow
        target_month = kwargs.get('target_month')
        damages = []
        for item in Persistent.damages:
            for name, values in item.items():
                dict1 = {}
                match_date = Utils.helper_fix_date_str3(values[1])
                if match_date == target_month:
                    dict1 = {name: (values[0], values[1], values[2])}
                    damages.append(dict1)

        df = pd.DataFrame()
        opcash_dry_run = {
            'dep': 0, 'hap': 0, 'rr': 0, 'corr_sum': 0}
        for entry in kwargs['currently_availables']:
            for genus, path in entry.items():
                if genus == 'scrape' and path[0] is True:
                    scrape = Scrape()
                    df = scrape.get_df_of_scrape(path=path[1])
                    scrape_txn_list = scrape.get_targeted_rows_for_scrape(
                        scrape_df=df)
                    df = pd.DataFrame(scrape_txn_list)
                    df = df.groupby(df['dep_type']).sum(numeric_only=True)
                    df = df.to_dict('dict')
                if genus == 'opcash' and path[0] is True:
                    self.op_cash_list.append(path[1][0])
                    opcash_dry_run = self.rename_by_content_pdf(
                        bypass_write_to_db=True)
                if genus == 'deposits' and path[0] is True:
                    deposits_dry_run = self.survey_deposits_report_for_dry_run(
                        path[1])
                if genus == 'rent' and path[0] is True:
                    rent_dry_run = self.survey_rent_report_for_dry_run(
                        path[1], target_month=target_month)

        return {'opcash': opcash_dry_run, 'deposits': deposits_dry_run, 'rent': rent_dry_run, 'damages': damages, 'scrape': df}

    def build_index_runner(self):
        self.index_dict = self.articulate_directory()
        self.load_what_is_in_dir_as_indexed(dict1=self.index_dict)
        self.make_a_list_of_indexed(mode=self.query_mode.pdf)
        self.find_opcashes()
        self.type_opcashes()
        self.rename_by_content_pdf()
        df_list = []
        for stmt_path in self.op_cash_list:
            target_list = ['Incoming Wire', 'QUADEL', 'Deposit', 'Correction', 'Chargeback']
            df_list.append(self.pdf.nbofi_pdf_extract(stmt_path, target_list=target_list))
            df = pd.concat(df_list)
        print(df)
            # r4r = self.pdf.nbofi_pdf_extract(stmt_path, target_str='Incoming Wire')
            # hap = self.pdf.nbofi_pdf_extract(stmt_path, target_str='QUADEL')
            # dep = self.pdf.nbofi_pdf_extract(stmt_path, target_str='Deposit')
            # correction = self.pdf.nbofi_pdf_extract(stmt_path, target_str='Correction')
            # chargeback = self.pdf.nbofi_pdf_extract(stmt_path, target_str='Chargeback')
            # depdet = self.pdf.nbofi_pdf_extract(stmt_path, target_str=target_str)
        # elif style == self.style_term.corrections:
            # depdet = self.pdf.nbofi_pdf_extract(path, target_str=target_str)
        '''
        return_list = []
        kdict = {}
        if style == self.style_term.r4r:
            date, amount = self.pdf.nbofi_pdf_extract_rr(
                path, target_str=target_str)
        elif style == self.style_term.hap:
            date, amount = self.pdf.nbofi_pdf_extract_hap(
                path, target_str=target_str)
        elif style == self.style_term.dep:
            date, amount = self.pdf.nbofi_pdf_extract_deposit(
                path, target_str=target_str)
        elif style == self.style_term.dep_detail:
            depdet_list = self.pdf.nbofi_pdf_extract_deposit(
                path, style=style, target_str=target_str)
            return depdet_list
        elif style == self.style_term.corrections:
            date, amount = self.pdf.nbofi_pdf_extract_corrections(
                path, style=style, target_str=target_str, target_str2=target_str2, date=date)

        kdict[str(date)] = [amount, path, style]
        return_list.append(kdict)
        return return_list, date
        '''

        breakpoint()
    
    def build_index_runner2(self):
        """this function is just a list of the funcs one would run to create the index from a fresh start"""
        self.connect_to_db()
        self.index_dict = self.articulate_directory()
        self.load_what_is_in_dir_as_indexed(dict1=self.index_dict)
        self.runner_internals()
        self.load_scrape_data_historical()
        self.findex_reconcile_onesite_deposits_to_scrape_or_oc()

    def runner_internals(self):
        self.make_a_list_of_indexed(mode=self.query_mode.xls)
        if self.indexed_list:
            self.xls_wrapper()

        self.make_a_list_of_indexed(mode=self.query_mode.pdf)
        if self.indexed_list:
            self.pdf_wrapper()

        self.make_a_list_of_indexed(mode=self.query_mode.csv)
        if self.indexed_list:
            self.get_period_from_scrape_fn_and_mark_in_findexer()

    def xls_wrapper(self):
        self.find_by_content(style=self.style_term.rent,
                             target_string=self.target_string.affordable, format=self.rent_format)
        self.find_by_content(style=self.style_term.deposits,
                             target_string=self.target_string.bank, format=self.deposits_format)

    def pdf_wrapper(self):
        self.find_opcashes()
        self.type_opcashes()
        self.rename_by_content_pdf()

    def test_for_unfinalized_months(self):
        months_ytd = Utils.months_in_ytd(Config.current_year)

        # get fully finalized months
        finalized_months = [rec.month for rec in StatusObject().select().where(
            (StatusObject.tenant_reconciled == 1) &
            ((StatusObject.opcash_processed == 1) |
             (StatusObject.scrape_reconciled == 1)) &
            (StatusObject.rs_reconciled == 1)).namedtuples()]

        final_not_written = [rec.month for rec in StatusObject().select().where(
            (StatusObject.tenant_reconciled == 1) &
            ((StatusObject.opcash_processed == 1) |
             (StatusObject.scrape_reconciled == 1)) &
            (StatusObject.rs_reconciled != 1)).namedtuples()]

        self.unfinalized_months = sorted(
            list(set(months_ytd) - set(finalized_months)))

        return months_ytd, self.unfinalized_months, final_not_written

    def test_for_unprocessed_file(self):
        print('searching for new files in path:')
        processed_fn = [item.fn for item in Findexer().select().where(
            Findexer.status == 'processed').namedtuples()]

        directory_contents = self.articulate_directory2()

        unproc_files = list(set(directory_contents) - set(processed_fn))

        self.unproc_files1 = unproc_files

        return self.unproc_files1, directory_contents

    def connect_to_db(self, mode=None):
        if self.db.is_closed():
            self.db.connect()
        if mode == 'autodrop':
            self.db.drop_tables(models=self.create_findex_list)
        self.db.create_tables(models=self.create_findex_list)

    def articulate_directory2(self):
        dir_cont = [item.name for item in self.path.iterdir(
        ) if item.name not in self.excluded_file_names]
        return dir_cont

    def articulate_directory(self):
        dir_contents = [item for item in self.path.iterdir()]
        for item in dir_contents:
            full_path = Path(item)
            if item.name not in self.excluded_file_names:
                self.index_dict[full_path] = (item.suffix, item.name)
        return self.index_dict

    def sort_directory_by_extension2(self):
        """this can successfully handle unproc files only"""
        index_dict = {}
        for fn in self.unproc_files1:
            index_dict[Path.joinpath(self.path, fn)] = (Path(fn).suffix, fn)

        self.index_dict_iter = index_dict  # for testing, do not just erase this
        return index_dict

    def load_what_is_in_dir_as_indexed(self, dict1={}, autodrop=None, verbose=None):
        insert_dir_contents = [{'path': path, 'fn': name, 'c_date': path.lstat(
        ).st_ctime, 'indexed': 'true', 'file_ext': suffix} for path, (suffix, name) in dict1.items()]
        query = Findexer.insert_many(insert_dir_contents)
        query.execute()

    def make_a_list_of_indexed(self, **kw):
        self.indexed_list = [(item.path, item.doc_id) for item in Findexer().select().
                             where(Findexer.status == kw['mode'][0]).
                             where(
            (Findexer.file_ext == kw['mode'][1][0]) |
            (Findexer.file_ext == kw['mode'][1][1])).namedtuples()]

    def get_df_from_path_list(self, path):
        # TODO: THIS SHOULD BE COMBINED INTO CLASS IN BACKEND
        import xlrd
        for possible_path in path:
            try:
                wb = xlrd.open_workbook(possible_path, logfile=open(os.devnull, 'w'))
            except FileNotFoundError as e:
                print(e)
                print(f'{possible_path.suffix} does not exist, trying other extension type')
            df = pd.read_excel(wb)
        return df

    def survey_deposits_report_for_dry_run(self, path, *args, **kwargs):
        df = self.get_df_from_path_list(path)
        deposits = self.deposit_report_unpacker(df=df)
        
        return deposits

    def survey_rent_report_for_dry_run(self, path, *args, **kwargs):
        sample = DryRunRentRoll(path=path, date=kwargs['target_month'])
        dirty_nt_list, total_tenant_charges, cleaned_mos, computed_mis = sample.return_rentroll_data()
        return {'tenant_charges': total_tenant_charges, 'mos': cleaned_mos, 'mis': computed_mis, 'dirty_tenant_list': dirty_nt_list}

    def find_by_content(self, style, target_string=None, **kw):
        for path, doc_id in self.indexed_list:
            part_list = ((Path(path).stem)).split('_')
            if style in part_list:
                df1 = pd.read_excel(path)
                df2 = df1.iloc[:, 0].to_list()

                if target_string in df2:  # rent roll piece
                    period = self.df_date_wrapper(path, kw=kw['format'])
                    find_change = Findexer.get(Findexer.doc_id == doc_id)
                    find_change.period = period
                    find_change.status = self.status_str.processed
                    find_change.doc_type = style
                    find_change.save()

                if style == self.style_term.deposits:
                    deposits = self.deposit_report_unpacker(df=df1)
                    find_change = Findexer.get(Findexer.doc_id == doc_id)
                    find_change.depsum = deposits
                    find_change.save()

    def deposit_report_unpacker(self, df=None):
        deposits = df.iloc[:, 13].to_list()
        deposits = [item for item in deposits if isinstance(item, str)]
        deposits = deposits[:-1]
        deposits = deposits[1:]
        deposits = [deposit.replace(',', '') for deposit in deposits]
        deposits = str(sum([float(item) for item in deposits]))

        # can we get out ntp and laundry here?
        return deposits

    def df_date_wrapper(self, path, **kw):
        split_col = kw['kw']['split_col']
        df_date = pd.read_excel(path)
        df_date = df_date.iloc[:, kw['kw']['get_col']].to_list()
        try:
            df_date = df_date[split_col].split(kw['kw']['split_type'])
        except AttributeError as e:
            print(e)
            print(f'issue is with {path}')
            print(f'relevant kwargs: {kw}')
            breakpoint()
        period = df_date[kw['kw']['date_split']]
        period = period.rstrip()
        period = period.lstrip()
        month = period[:-4]
        year = period[-4:]
        period = year + '-' + month
        return period

    def find_opcashes(self):
        for item, doc_id in self.indexed_list:
            op_cash_path = self.pdf.select_stmt_by_str(
                path=item, target_str=self.bank_acct_str)
            if op_cash_path != None:
                self.op_cash_list.append(op_cash_path)

    def type_opcashes(self):
        for path in self.op_cash_list:
            doc_id = [item.doc_id for item in Findexer().select().where(
                Findexer.path == path).namedtuples()][0]
            find_change = Findexer.get(Findexer.doc_id == doc_id)
            find_change.doc_type = self.style_term.opcash
            find_change.save()

    def rename_by_content_pdf(self, **kwargs):
        for op_cash_stmt_path in self.op_cash_list:
            hap_iter_one_month, stmt_date = self.extract_deposits_by_type(
                op_cash_stmt_path, style=self.style_term.hap, target_str=self.target_string.quadel)
            date = stmt_date
            rr_iter_one_month, stmt_date1 = self.extract_deposits_by_type(
                op_cash_stmt_path, style=self.style_term.r4r, target_str=self.target_string.r4r)
            dep_iter_one_month, stmt_date2 = self.extract_deposits_by_type(
                op_cash_stmt_path, style=self.style_term.dep, target_str=self.target_string.oc_deposit)
            deposit_and_date_iter_one_month = self.extract_deposits_by_type(
                op_cash_stmt_path, style=self.style_term.dep_detail, target_str=self.target_string.oc_deposit)
            corrections_sum = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.corrections,
                                                            target_str=self.target_string.corrections, target_str2=self.target_string.chargebacks, date=date)
            Reconciler.findexer_assert_stmt_dates_match(
                stmt1_date=stmt_date, stmt2_date=stmt_date1)

            if kwargs.get('bypass_write_to_db'):
                return {'date': date,
                        'hap': Utils.unpacking_list_of_dicts(hap_iter_one_month),
                        'rr': Utils.unpacking_list_of_dicts(rr_iter_one_month),
                        'corr_sum': Utils.unpacking_list_of_dicts(corrections_sum[0]),
                        'dep': Utils.unpacking_list_of_dicts(dep_iter_one_month),
                        'dep_and_date': list(deposit_and_date_iter_one_month[0].values())[0]}

            else:
                self.write_deplist_to_db(hap_iter_one_month, rr_iter_one_month, dep_iter_one_month,
                                         deposit_and_date_iter_one_month, corrections_sum, stmt_date)
    
    def extract_deposits_by_type(self, path, style=None, target_str=None, target_str2=None, date=None):
        return_list = []
        kdict = {}
        if style == self.style_term.r4r:
            date, amount = self.pdf.nbofi_pdf_extract_rr(
                path, target_str=target_str)
        elif style == self.style_term.hap:
            date, amount = self.pdf.nbofi_pdf_extract_hap(
                path, target_str=target_str)
        elif style == self.style_term.dep:
            date, amount = self.pdf.nbofi_pdf_extract_deposit(
                path, target_str=target_str)
        elif style == self.style_term.dep_detail:
            depdet_list = self.pdf.nbofi_pdf_extract_deposit(
                path, style=style, target_str=target_str)
            return depdet_list
        elif style == self.style_term.corrections:
            date, amount = self.pdf.nbofi_pdf_extract_corrections(
                path, style=style, target_str=target_str, target_str2=target_str2, date=date)

        kdict[str(date)] = [amount, path, style]
        return_list.append(kdict)
        return return_list, date

    def write_deplist_to_db(self, hap_iter, rr_iter, depsum_iter, deposit_iter, corrections_iter, stmt_date):
        opcash_records = [(item.fn, item.doc_id) for item in Findexer().
                          select().
                          where(Findexer.doc_type == self.style_term.opcash).
                          namedtuples()]

        for name, doc_id in opcash_records:
            if self.get_date_from_opcash_name(name) == [*deposit_iter[0]][0]:
                proc_date = stmt_date
                rr = [*rr_iter[0].values()][0][0]
                hap = [*hap_iter[0].values()][0][0]
                depsum = [*depsum_iter[0].values()][0][0]
                deplist = json.dumps([{self.adjust_deposit_date(item[0]): item[1]} for item in [
                                     *deposit_iter[0].values()][0]])
                corr_sum = [*corrections_iter[0][0].values()][0][0]

                find_change = Findexer.get(Findexer.doc_id == doc_id)
                find_change.status = self.status_str.processed
                find_change.period = Utils.helper_fix_date(proc_date)
                find_change.hap = hap
                find_change.rr = rr
                find_change.depsum = depsum
                find_change.deplist = deplist
                find_change.corr_sum = corr_sum
                find_change.save()

    def get_report_type_from_xls_name(self, records=None):
        records1 = []
        for path, name in records.items():
            typ = name[1].split('_')[0]
            if typ == 'CHECKING':
                typ = 'scrape'
            dict1 = {typ: (path, path.name)}
            records1.append(dict1)
        return records1

    def get_date_from_file_name(self, records=None):
        records1 = []
        for item in records:
            for typ, data in item.items():
                if typ == 'deposits':
                    date_str = '-'.join(data[1].split('.')
                                        [0].split('_')[1:][::-1])
                elif typ == 'rent':
                    date_str = '-'.join(data[1].split('.')
                                        [0].split('_')[2:][::-1])
                elif typ == 'scrape':
                    date_str = data[1].split('_')[3][0:7]
                elif typ == 'op':
                    date_str = '-'.join(data[1].split('.')
                                        [0].split('_')[2:][::-1])
                    date_object = datetime.strptime(date_str, '%m-%Y')
                    date_str = date_object.strftime('%Y-%m')
                dict1 = {typ: (date_str, data[0])}
                records1.append(dict1)
        return records1

    def get_date_from_opcash_name(self, record):
        date_list = record.split('.')[0].split('_')[2:]
        date_list.reverse()
        date_list = ' '.join(date_list)
        return date_list

    def drop_findex_table(self):
        self.db.drop_tables(models=self.create_findex_list)

    def close_findex_table(self):
        self.db.close()
