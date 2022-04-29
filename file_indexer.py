import json
import os
from datetime import datetime
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd

from config import Config
from pdf import StructDataExtract
from backend import Findexer


class FileIndexer:

    create_findex_list = [Findexer]
    index_dict = {}
    raw_list = []
    op_cash_list = []
    pdf = StructDataExtract()
    hap_list = []
    rr_list = []
    dep_list = []
    deposit_and_date_list = []

    def __init__(self, path=None, db=None, mode=None):
        self.path = path
        self.db = db
        self.mode = mode

    def build_index_runner(self):
        self.connect_to_db(mode='autodrop')
        self.directory_contents = self.articulate_directory()
        self.index_dict = self.sort_directory_by_extension() # this doesn't do the sorting anymore but we still use it
        self.load_what_is_in_dir()

        self.make_a_list_of_raw(mode='xls')
        if self.raw_list:
            self.find_by_content(style='rent', target_string='Affordable Rent Roll Detail/ GPR Report')
            self.find_by_content(style='deposits', target_string='BANK DEPOSIT DETAILS')

        self.make_a_list_of_raw(mode='pdf')
        if self.raw_list:
            self.find_opcashes()
            self.type_opcashes()
            self.rename_by_content_pdf()

    def connect_to_db(self, mode=None):
        if self.db.is_closed():
            self.db.connect()
        if mode == 'autodrop':
            self.db.drop_tables(models=self.create_findex_list)
        self.db.create_tables(models=self.create_findex_list)

    def articulate_directory(self):
        self.directory_contents = [item for item in self.path.iterdir() if item.suffix != '.ini'] 
        return self.directory_contents

    def sort_directory_by_extension(self, verbose=None):
        for item in self.directory_contents:
            full_path = Path(item)
            if item.name != 'desktop.ini':
                self.index_dict[full_path] = (item.suffix, item.name)
        
        if verbose:
            pprint(self.index_dict)
        return self.index_dict

    def load_what_is_in_dir(self, autodrop=None, verbose=None):
        insert_dir_contents = [{'path': path, 'fn': name, 'indexed': 'false', 'file_ext': suffix} for path, (suffix, name) in self.index_dict.items()]
        query = Findexer.insert_many(insert_dir_contents)
        query.execute()

    def make_a_list_of_raw(self, mode=None):
        '''instead of making this from a list in memory (faster), I will make the list from db, so that I can control whether to process it or reprocessed it; otherwise, everything that is in dir will just be fully processed again'''
        if mode == 'xls':   
            self.raw_list = [(item.path, item.doc_id) for item in Findexer().select().where(Findexer.status=='raw').where(
                (Findexer.file_ext == '.xlsx') | (Findexer.file_ext == '.xls')).namedtuples()]

        if mode == 'pdf':   
            self.raw_list = [(item.path, item.doc_id) for item in Findexer().select().where(Findexer.status=='raw').where(Findexer.file_ext == '.pdf').namedtuples()]

    def find_by_content(self, style, target_string=None):
        if style == 'rent':
            get_col = 0
            split_col = 11
            split_type = ' '
            date_split = 2

        if style == 'deposits':
            get_col = 9
            split_col = 9
            split_type = '/'
            date_split = 0

        for path, doc_id in self.raw_list:
            part_list = ((Path(path).stem)).split('_')
            if style in part_list:
                df = pd.read_excel(path)
                df = df.iloc[:, 0].to_list()
                if target_string in df:
                    period = self.df_date_wrapper(path, get_col=get_col, split_col=split_col, split_type=split_type, date_split=date_split)
                    find_change = Findexer.get(Findexer.doc_id==doc_id)
                    find_change.period = period
                    find_change.status = 'processed'
                    find_change.doc_type = style
                    find_change.save()

    def df_date_wrapper(self, item, get_col=None, split_col=None, split_type=None, date_split=None):
        df_date = pd.read_excel(item)
        df_date = df_date.iloc[:, get_col].to_list()
        df_date = df_date[split_col].split(split_type)
        period = df_date[date_split]
        period = period.rstrip()
        period = period.lstrip()        
        month = period[:-4]
        year = period[-4:]
        period = year + '-' + month

        return period

    def find_opcashes(self):
        for item, doc_id in self.raw_list:
            op_cash_path = self.pdf.select_stmt_by_str(path=item, target_str=' XXXXXXX1891')
            if op_cash_path != None:
                self.op_cash_list.append(op_cash_path)

    def type_opcashes(self):
        for path in self.op_cash_list:
            doc_id = [item.doc_id for item in Findexer().select().where(Findexer.path == path).namedtuples()][0]
            find_change = Findexer.get(Findexer.doc_id==doc_id)
            find_change.doc_type = 'opcash'
            find_change.save()

    def rename_by_content_pdf(self):
        for op_cash_stmt_path in self.op_cash_list:
            hap_iter_one_month, stmt_date = self.extract_deposits_by_type(op_cash_stmt_path, style='hap', target_str='QUADEL')
            rr_iter_one_month, stmt_date1 = self.extract_deposits_by_type(op_cash_stmt_path, style='rr', target_str='Incoming Wire')
            dep_iter_one_month, stmt_date2 = self.extract_deposits_by_type(op_cash_stmt_path, style='dep', target_str='Deposit')
            deposit_and_date_iter_one_month = self.extract_deposits_by_type(op_cash_stmt_path, style='dep_detail', target_str='Deposit')
            assert stmt_date == stmt_date1
        
            self.hap_list.append(hap_iter_one_month)
            self.rr_list.append(rr_iter_one_month)
            self.dep_list.append(dep_iter_one_month)
            self.deposit_and_date_list.append(deposit_and_date_iter_one_month)

            self.write_deplist_to_db(hap_iter_one_month, rr_iter_one_month, dep_iter_one_month, deposit_and_date_iter_one_month, stmt_date)

    def extract_deposits_by_type(self, path, style=None, target_str=None):
        return_list = []
        kdict = {}
        if style == 'rr':
            date, amount = self.pdf.nbofi_pdf_extract_rr(path, target_str=target_str)
        elif style == 'hap':
            date, amount = self.pdf.nbofi_pdf_extract_hap(path, target_str=target_str)
        elif style == 'dep':
            date, amount = self.pdf.nbofi_pdf_extract_deposit(path, target_str=target_str)
        elif style == 'dep_detail':
            depdet_list = self.pdf.nbofi_pdf_extract_deposit(path, style=style, target_str=target_str)
            return depdet_list

        kdict[str(date)] = [amount, path, style]
        return_list.append(kdict)            
        return return_list, date
    
    def write_deplist_to_db(self, hap_iter, rr_iter, depsum_iter, deposit_iter, stmt_date):
        print('Writing deposit list to db')
        opcash_records = [(item.fn, item.doc_id) for item in Findexer().
            select().
            where(Findexer.doc_type=='opcash').
            namedtuples()]

        for name, doc_id in opcash_records:
            if self.get_date_from_opcash_name(name) == [*deposit_iter[0]][0]:
                proc_date = stmt_date
                rr = [*rr_iter[0].values()][0][0]
                hap = [*hap_iter[0].values()][0][0] 
                depsum = [*depsum_iter[0].values()][0][0]
                deplist = json.dumps([*deposit_iter[0].values()])
                
                find_change = Findexer.get(Findexer.doc_id==doc_id)
                find_change.status = 'processed'
                find_change.period = self.helper_fix_date(proc_date)
                find_change.hap = hap
                find_change.rr = rr
                find_change.depsum = depsum
                find_change.deplist = deplist
                find_change.save()

    def get_date_from_opcash_name(self, record):
        date_list = record.split('.')[0].split('_')[2:]
        date_list.reverse()
        date_list = ' '.join(date_list)
        return date_list

    def helper_fix_date(self, raw_date):    
        f_date = datetime.strptime(raw_date, '%m %Y')
        f_date = f_date.strftime('%Y-%m')
        return f_date

    def drop_findex_table(self):
        self.db.drop_tables(models=self.create_findex_list)

    def close_findex_table(self):
        self.db.close()

    def load_mm_scrape(self, list1=None):
        path = Config.TEST_MM_SCRAPE

        for fn in path.iterdir():
            if fn.name != 'desktop.ini':
                df = pd.read_csv(fn)
                
        deposit_list = []
        for index, row in df.iterrows():
            if row['Description'] == 'DEPOSIT':
                dict1 = {}
                dict1 = {'date': row['Processed Date'], 'amount': row['Amount']}
                deposit_list.append(dict1)

        return deposit_list
















