import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import xlrd

from backend import (DryRunRentRoll, Findexer, PopulateTable, Reconciler,
                     StatusObject)
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
        
        if 'Posted Date' in scrape_df.columns:
            # from nbofi business side (NEW)
            date_row = 'Posted Date'
            deposit_match = 'Deposit'
            corr_match = 'Correction'
        elif 'Processed Date' in scrape_df.columns:
            # from nbofi client side (should be deprecated)
            date_row = 'Processed Date'
            deposit_match = 'DEPOSIT'
            corr_match = 'CORRECTION'
        
        for _, row in scrape_df.iterrows():
            if row['Description'] == deposit_match:
                dict1 = {}
                dict1 = {'date': self.adjust_deposit_date(
                                        row[date_row]), 
                                        'amount': row['Amount'], 
                                        'dep_type': 'deposit'}
                deposit_list.append(dict1)
            if 'INCOMING' in row['Description']:
                dict1 = {}
                dict1 = {'date': row[date_row],
                         'amount': row['Amount'], 
                         'dep_type': 'rr'}
                deposit_list.append(dict1)

            if 'QUADEL' in row['Description']:
                dict1 = {}
                dict1 = {'date': row[date_row],
                         'amount': row['Amount'], 
                         'dep_type': 'hap'}
                deposit_list.append(dict1)

            '''following branches are for finding corrections'''

            if 'CHARGEBACK' in row['Description']:
                if 'FEE' not in row['Description']:
                    corr_count += 1
                    dict1 = {}
                    dict1 = {'date': row[date_row],
                             'amount': row['Amount'], 
                             'dep_type': 'corr'}
                    deposit_list.append(dict1)

            if corr_match in row['Description']:
                corr_count += 1
                dict1 = {}
                dict1 = {'date': row[date_row],
                         'amount': row['Amount'], 
                         'dep_type': 'corr'}
                deposit_list.append(dict1)

        if corr_count == 0:
            dict1 = {'date': row[date_row],
                     'amount': 0, 
                     'dep_type': 'corr'}
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

    def incremental_filer(self, explicit_month_to_load=None, pytest=False):
        print('incremental_filer(), FileIndexer method from file_indexer.py')
        print('\n')
        self.connect_to_db()

        print('showing unfinalized months: either no opcash, no tenant, or no rent_sheet write')

        """A FINALIZED MONTH ==
            1 OPCASH processed 
            2 DEPOSITS + ADJUSTMENTS == TENANT_PAY + NTP"""

        months_ytd, unfin_month, final_not_written = self.test_for_unfinalized_months(explicit_month_to_load=explicit_month_to_load)

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
            
            self.index_dict = self.sort_directory_by_extension2()
            self.load_what_is_in_dir_as_indexed(dict1=self.index_dict_iter)

            if choice1 == 1:
                print('YES, I WANT TO ADD THIS FILE FINDEXER DB')
                self.runner_internals()
                new_files = self.get_report_type_from_xls_name(
                    records=self.index_dict)
                new_files = self.get_date_from_file_name(records=new_files)
                self.findex_reconcile_onesite_deposits_to_scrape_or_oc()
            else:
                new_files = self.get_report_type_from_xls_name(
                    records=self.index_dict)
                new_files = self.get_date_from_file_name(records=new_files)
                print('no new files to add in file_indexer.py; exiting')
                sys.exit()
                
            return new_files, self.unfinalized_months, final_not_written

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
                    scrapes = df.to_dict('dict')
                if genus == 'opcash' and path[0] is True:
                    self.op_cash_list.append(path[1])
                    opcash_dry_run = self.pdf_to_df_to_db(bypass_write_to_db=True)
                if genus == 'deposits' and path[0] is True:
                    deposits_dry_run = self.survey_deposits_report_for_dry_run(
                        path[1])
                if genus == 'rent' and path[0] is True:
                    rent_dry_run = self.survey_rent_report_for_dry_run(
                        path[1], target_month=target_month)

        return {'opcash': opcash_dry_run, 
                'deposits': deposits_dry_run, 
                'rent': rent_dry_run, 
                'damages': damages, 
                'scrape': scrapes}

    def pdf_to_df_to_db(self, bypass_write_to_db=None):
        # TODO fix corr_sum & chargeback logic
        # TODO could do a faster query insert here
        target_list = ['Incoming Wire', 'QUADEL',
                       'Deposit', 'Correction', 'Chargeback']
        for path in self.op_cash_list:
            df, stmt_date = self.pdf.nbofi_pdf_extract(
                path=path, target_list=target_list)

            correction = df[df['type'].str.contains('correction')]
            correction = correction.groupby(
                correction['period']).sum(numeric_only=True)

            chargeback = df[df['type'].str.contains('chargeback')]
            chargeback = chargeback.groupby(
                chargeback['period']).sum(numeric_only=True)

            hap = df[df['type'].str.contains('hap')]
            hap = hap.groupby(hap['period']).sum(numeric_only=True)

            deposits = df[df['type'].str.contains('Deposit')]
            depsum = deposits.groupby(
                deposits['period']).sum(numeric_only=True)
            deplist = pd.Series(deposits.amount.values,
                                index=deposits.date.astype(str))
            deplist = [{time: amount} for time, amount in deplist.items()]

            r4r = df[df['type'].str.contains('rr')]
            r4r = r4r.groupby(r4r['period']).sum(numeric_only=True)
            
            if depsum.empty:
                depsum = '0'
            else:
                depsum = str(depsum.iloc[0].values[0])

            if r4r.empty:
                r4r = '0'
            else:
                r4r = str(r4r.iloc[0].values[0])

            if hap.empty:
                hap = '0'
            else:
                hap = str(hap.iloc[0].values[0])

            if chargeback.empty:
                chargeback = '0'
            else:
                chargeback = str(chargeback.iloc[0].values[0])
            
            if correction.empty:
                corr_sum = '0'
            else:
                corr_sum = str(correction.iloc[0].values[0])
                
            # chargeback is currently added to corr_sum!!                
            corr_sum = float(corr_sum) + float(chargeback)            

                
            if bypass_write_to_db is None:
                opcash_records = [(item.fn, item.doc_id) for item in Findexer().
                                select().
                                where(Findexer.path == path).
                                namedtuples()]
                find_change = Findexer.get(Findexer.doc_id == opcash_records[0][1])

                find_change.status = self.status_str.processed
                find_change.period = stmt_date
                find_change.depsum = depsum
                find_change.hap = hap
                find_change.rr = r4r
                find_change.chargeback = chargeback
                find_change.corr_sum = str(round(corr_sum, 2)) 
                if deplist:
                    find_change.deplist = json.dumps(deplist)
                else:
                    find_change.deplist = '0'
                find_change.save()
            else:
                return {'depsum': depsum, 'deplist': deplist, 'hap': hap, 'r4r': r4r, 'corr_sum': corr_sum}

    def build_index_runner(self):
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
        # speed improvements here
        self.find_opcashes()
        self.type_opcashes()
        self.pdf_to_df_to_db()

    def test_for_unfinalized_months(self, explicit_month_to_load=None):
        months_ytd = Utils.months_in_ytd(Config.dynamic_current_year, 
                                         last_range_month=explicit_month_to_load)

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
        if isinstance(path, list) == False:
            path = [path]
        
        for possible_path in path:
            try:
                wb = xlrd.open_workbook(
                    possible_path, logfile=open(os.devnull, 'w'))
            except FileNotFoundError as e:
                print(e)
                print(
                    f'{possible_path.suffix} does not exist, trying other extension type')
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
                df1 = Utils.handle_excel_formats(path)
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
        if path[-1] == 'x':
            df_date = pd.read_excel(path)
        else:
            filename = xlrd.open_workbook(path,
                                          logfile=open(os.devnull, 'w'))
            df_date = pd.read_excel(filename)
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
                tup1 = (data[0], date_str, typ)
                records1.append(tup1)
        return records1

    def drop_findex_table(self):
        self.db.drop_tables(models=self.create_findex_list)

    def close_findex_table(self):
        self.db.close()
