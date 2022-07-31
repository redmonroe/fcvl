import json
import os
from datetime import datetime
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd

from backend import Findexer, PopulateTable, StatusObject
from config import Config
from pdf import StructDataExtract
from utils import Utils

class Scrape:

    def get_period_from_scrape_fn_and_mark_in_findexer(self):
        """this just provides row in findexer db & marks as processed; does not mark_scrape reconciled as True or file out hap, tenant, etc cols"""
        for item, doc_id in self.indexed_list:
            scrape_file = Findexer.get(Findexer.doc_id==doc_id)
            date_str = [part.split('_') for part in item.split('/')][-1][-2]
            date_str = '-'.join(date_str.split('-')[0:2])
            scrape_file.status = self.status_str.processed
            scrape_file.period = date_str
            scrape_file.doc_type = 'scrape'
            scrape_file.save()

    def load_scrape_data_historical(self):
        scrapes = [(row.path, row.period) for row in Findexer.select().where(Findexer.doc_type == 'scrape').namedtuples()]

        for scrape_path, scrape_period in scrapes:
            scrape_txn_list = self.load_directed_scrape(path_to_scrape=scrape_path, target_date=scrape_period)

            check = sum([item['amount'] for item in scrape_txn_list])

            dep_sum = sum([item['amount'] for item in scrape_txn_list if item['dep_type'] == 'deposit'])

            hap_sum = sum([item['amount'] for item in scrape_txn_list if item['dep_type'] == 'hap'])

            rr_sum = sum([item['amount'] for item in scrape_txn_list if item['dep_type'] == 'rr'])

            corr_sum = sum([item['amount'] for item in scrape_txn_list if item['dep_type'] == 'corr'])

            dep_list = [{item['date']:item['amount']} for item in scrape_txn_list if item['dep_type'] == 'deposit']

            breakpoint()
            double_check = dep_sum + hap_sum + corr_sum + rr_sum
            assert check == round(double_check, 2)            

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
        populate.load_scrape_to_db(deposit_list=scrape_txn_list, target_date=target_date)

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

        most_recent_scrape = self.get_most_recent_scrape(most_recent_dict=mr_dict)

        return self.get_targeted_rows_for_scrape(scrape_df=most_recent_scrape) 

    def get_most_recent_scrape(self, most_recent_dict=None):
        return pd.read_csv(max(list(most_recent_dict)))   

    def get_df_of_scrape(self, path=None):
        return pd.read_csv(path)

    def get_targeted_rows_for_scrape(self, scrape_df=None):
        deposit_list = []
        corr_count = 0
        for index, row in scrape_df.iterrows():
            if row['Description'] == 'DEPOSIT':
                dict1 = {}
                date_str = row['Processed Date']
                if '-' in [letter for letter in date_str]:
                    date_str = Utils.helper_fix_date_str(date_str)
                elif '/' in [letter for letter in date_str]:
                    date_str = date_str
                else:
                    print('issue converting date in scrape')
                    exit
                
                dict1 = {'date': date_str, 'amount': row['Amount'], 'dep_type': 'deposit'}
                deposit_list.append(dict1)
            if 'INCOMING' in row['Description']:
                dict1 = {}
                dict1 = {'date': row['Processed Date'], 'amount': row['Amount'], 'dep_type': 'rr'}                
                deposit_list.append(dict1)

            if 'QUADEL' in row['Description']:
                dict1 = {}
                dict1 = {'date': row['Processed Date'], 'amount': row['Amount'], 'dep_type': 'hap'}                
                deposit_list.append(dict1)

            if 'CHARGEBACK' in row['Description']:
                if 'FEE' not in row['Description']:
                    corr_count += 1
                    dict1 = {}
                    dict1 = {'date': row['Processed Date'], 'amount': row['Amount'], 'dep_type': 'corr'}
                    deposit_list.append(dict1)

        if corr_count == 0:
            dict1 = {'date': row['Processed Date'], 'amount': 0, 'dep_type': 'corr'}
            deposit_list.append(dict1)
        
        return deposit_list

class FileIndexer(Utils, Scrape):

    '''move to config.json'''
    query_mode = Utils.dotdict({'xls': ('raw', ('.xls', '.xlsx')), 'pdf': ('raw', ('.pdf', '.pdf')), 'csv': ('raw', ('.csv', '.csv'))})
    rent_format = {'get_col': 0, 'split_col': 11, 'split_type': ' ', 'date_split': 2}
    deposits_format = {'get_col': 9, 'split_col': 9, 'split_type': '/', 'date_split': 0}
    style_term = Utils.dotdict({'rent': 'rent', 'deposits': 'deposits', 'opcash': 'opcash', 'hap': 'hap', 'r4r': 'rr', 'dep': 'dep', 'dep_detail': 'dep_detail', 'corrections': 'corrections'})
    bank_acct_str = ' XXXXXXX1891'
    excluded_file_names = ['desktop.ini', 'beginning_balance_2022.xlsx']
    target_string = Utils.dotdict({'affordable':'Affordable Rent Roll Detail/ GPR Report', 'bank': 'BANK DEPOSIT DETAILS', 'quadel': 'QUADEL', 'r4r': 'Incoming Wire', 'oc_deposit': 'Deposit', 'corrections': 'Chargeback'  })
    status_str = Utils.dotdict({'raw': 'raw', 'processed': 'processed'})

    def __init__(self, path=None, db=None, mode=None):
        self.path = path
        self.db = db
        self.mode = mode
        self.unproc_file_for_testing = []
        self.unfinalized_months = []
        self.index_dict = {}
        self.index_dict_iter = {} 
        self.indexed_list = []
        self.op_cash_list = []
        self.create_findex_list = [Findexer]
        self.pdf = StructDataExtract()
        self.scrape_path = Config.TEST_MM_SCRAPE

    def iter_build_runner(self):
        print('iter_build_runner; a FileIndexer method')        
        self.connect_to_db() 
        months_ytd, unf_months = self.test_for_unfinalized_months()

        if len(self.unfinalized_months) > 0:
            unproc_files, directory_contents = self.test_for_unprocessed_file()
        else:
            print('no unfinalized months; you are presumptively caught up!')
            print('exiting program')
            exit

        if len(unproc_files) == 0:
            print('there are no new files in path')
            print('here we should look to see whether we want to make any new rent sheets')
            return [], []
        else:
            print('adding new files to findexer')
            self.index_dict = self.sort_directory_by_extension2() 
            self.load_what_is_in_dir_as_indexed(dict1=self.index_dict_iter)
    
            self.runner_internals()        
            
            new_files_dict = self.get_report_type_from_xls_name(records=self.index_dict)
            new_files_dict = self.get_date_from_xls_name(records=new_files_dict)

            return new_files_dict, self.unfinalized_months

    def build_index_runner(self):
        self.connect_to_db()
        self.index_dict = self.articulate_directory()
        self.load_what_is_in_dir_as_indexed(dict1=self.index_dict)
        self.runner_internals()    
        self.load_scrape_data_historical()    

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
        self.find_by_content(style=self.style_term.rent, target_string=self.target_string.affordable, format=self.rent_format)
        self.find_by_content(style=self.style_term.deposits, target_string=self.target_string.bank, format=self.deposits_format)
    
    def pdf_wrapper(self):
        self.find_opcashes()
        self.type_opcashes()    
        self.rename_by_content_pdf()

    def test_for_unfinalized_months(self):
        months_ytd = Utils.months_in_ytd(Config.current_year)
    
        # get fully finalized months
        finalized_months = [rec.month for rec in StatusObject().select().where((StatusObject.tenant_reconciled==1) &
                    (StatusObject.opcash_processed==1)).namedtuples()]

        # are there any unfinalized months?
        self.unfinalized_months = list(set(months_ytd) - set(finalized_months))

        return months_ytd, self.unfinalized_months

    def test_for_unprocessed_file(self):
        print('searching for new files in path')
        processed_fn = [item.fn for item in Findexer().select().where(Findexer.status=='processed').namedtuples()]  

        directory_contents = self.articulate_directory2()    

        unproc_files = list(set(directory_contents) - set(processed_fn))

        self.unproc_file_for_testing = unproc_files  
        
        return self.unproc_file_for_testing, directory_contents

    def connect_to_db(self, mode=None):
        if self.db.is_closed():
            self.db.connect()
        if mode == 'autodrop':
            self.db.drop_tables(models=self.create_findex_list)
        self.db.create_tables(models=self.create_findex_list)

    def articulate_directory2(self):
        dir_cont = [item.name for item in self.path.iterdir() if item.name not in self.excluded_file_names] 
        return dir_cont
   
    def articulate_directory(self):
        dir_contents = [item for item in self.path.iterdir()] 
        for item in dir_contents:
            full_path = Path(item)
            if item.name not in self.excluded_file_names:
                self.index_dict[full_path] = (item.suffix, item.name)        
        return self.index_dict

    def sort_directory_by_extension2(self):
        index_dict = {}
        for fn in self.unproc_file_for_testing:
            index_dict[Path.joinpath(self.path, fn)] = (Path(fn).suffix, fn)
        
        self.index_dict_iter = index_dict # for testing, do not just erase this
        return index_dict        

    def load_what_is_in_dir_as_indexed(self, dict1={}, autodrop=None, verbose=None):
        insert_dir_contents = [{'path': path, 'fn': name, 'c_date': path.lstat().st_ctime, 'indexed': 'true', 'file_ext': suffix} for path, (suffix, name) in dict1.items()]
        query = Findexer.insert_many(insert_dir_contents)
        query.execute()

    def make_a_list_of_indexed(self, **kw):
        self.indexed_list = [(item.path, item.doc_id) for item in Findexer().select().
                where(Findexer.status==kw['mode'][0]).
                where(
                (Findexer.file_ext == kw['mode'][1][0]) |
                (Findexer.file_ext == kw['mode'][1][1])).namedtuples()]

    def find_by_content(self, style, target_string=None, **kw):
        for path, doc_id in self.indexed_list:
            part_list = ((Path(path).stem)).split('_')
            if style in part_list:
                df = pd.read_excel(path)
                df = df.iloc[:, 0].to_list()
                if target_string in df:
                    period = self.df_date_wrapper(path, kw=kw['format'])
                    find_change = Findexer.get(Findexer.doc_id==doc_id)
                    find_change.period = period
                    find_change.status = self.status_str.processed
                    find_change.doc_type = style
                    find_change.save()

    def df_date_wrapper(self, path, **kw):
        split_col = kw['kw']['split_col']
        df_date = pd.read_excel(path)
        df_date = df_date.iloc[:, kw['kw']['get_col']].to_list()
        df_date = df_date[split_col].split(kw['kw']['split_type'])
        period = df_date[kw['kw']['date_split']]
        period = period.rstrip()
        period = period.lstrip()        
        month = period[:-4]
        year = period[-4:]
        period = year + '-' + month
        return period

    def find_opcashes(self):
        for item, doc_id in self.indexed_list:
            op_cash_path = self.pdf.select_stmt_by_str(path=item, target_str=self.bank_acct_str)
            if op_cash_path != None:
                self.op_cash_list.append(op_cash_path)

    def type_opcashes(self):
        for path in self.op_cash_list:
            doc_id = [item.doc_id for item in Findexer().select().where(Findexer.path == path).namedtuples()][0]
            find_change = Findexer.get(Findexer.doc_id==doc_id)
            find_change.doc_type = self.style_term.opcash
            find_change.save()

    def rename_by_content_pdf(self):
        for op_cash_stmt_path in self.op_cash_list:
            hap_iter_one_month, stmt_date = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.hap, target_str=self.target_string.quadel)
            rr_iter_one_month, stmt_date1 = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.r4r, target_str=self.target_string.r4r)
            dep_iter_one_month, stmt_date2 = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.dep, target_str=self.target_string.oc_deposit)
            deposit_and_date_iter_one_month = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.dep_detail, target_str=self.target_string.oc_deposit)
            corrections_sum = self.extract_deposits_by_type(op_cash_stmt_path, style=self.style_term.corrections, target_str=self.target_string.corrections)
            assert stmt_date == stmt_date1

            self.write_deplist_to_db(hap_iter_one_month, rr_iter_one_month, dep_iter_one_month, deposit_and_date_iter_one_month, corrections_sum, stmt_date)

    def extract_deposits_by_type(self, path, style=None, target_str=None):
        return_list = []
        kdict = {}
        if style == self.style_term.r4r:
            date, amount = self.pdf.nbofi_pdf_extract_rr(path, target_str=target_str)
        elif style == self.style_term.hap:
            date, amount = self.pdf.nbofi_pdf_extract_hap(path, target_str=target_str)
        elif style == self.style_term.dep:
            date, amount = self.pdf.nbofi_pdf_extract_deposit(path, target_str=target_str)
        elif style == self.style_term.dep_detail:
            depdet_list = self.pdf.nbofi_pdf_extract_deposit(path, style=style, target_str=target_str)
            return depdet_list
        elif style == self.style_term.corrections:
            date, amount = self.pdf.nbofi_pdf_extract_corrections(path, style=style, target_str=target_str)

        kdict[str(date)] = [amount, path, style]
        return_list.append(kdict)            
        return return_list, date
    
    def write_deplist_to_db(self, hap_iter, rr_iter, depsum_iter, deposit_iter, corrections_iter, stmt_date):
        opcash_records = [(item.fn, item.doc_id) for item in Findexer().
            select().
            where(Findexer.doc_type==self.style_term.opcash).
            namedtuples()]

        for name, doc_id in opcash_records:
            if self.get_date_from_opcash_name(name) == [*deposit_iter[0]][0]:
                proc_date = stmt_date
                rr = [*rr_iter[0].values()][0][0]
                hap = [*hap_iter[0].values()][0][0] 
                depsum = [*depsum_iter[0].values()][0][0]
                deplist = json.dumps([*deposit_iter[0].values()])
                corr_sum = [*corrections_iter[0][0].values()][0][0]
                
                find_change = Findexer.get(Findexer.doc_id==doc_id)
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

    def get_date_from_xls_name(self, records=None):
        records1 = []
        for item in records:
            for typ, data in item.items():
                if typ == 'deposits':
                    date_str = '-'.join(data[1].split('.')[0].split('_')[1:][::-1])
                elif typ == 'rent':
                    date_str = '-'.join(data[1].split('.')[0].split('_')[2:][::-1])
                elif typ == 'scrape':
                    date_str = data[1].split('_')[3][0:7]
                    breakp
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


















