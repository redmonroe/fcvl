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


class FileIndexer(Utils):

    '''move to instance variables'''
    create_findex_list = [Findexer]
    index_dict = {}
    raw_list = []
    op_cash_list = []
    pdf = StructDataExtract()
    hap_list = []
    rr_list = []
    dep_list = []
    corrections_list = []
    deposit_and_date_list = []
    scrape_path = Config.TEST_MM_SCRAPE

    '''move to config.json'''
    doc_mode = Utils.dotdict({'xls': 'xls', 'pdf': 'pdf', 'csv': 'csv'})
    ext_mode = Utils.dotdict({'xls': '.xls', 'pdf': '.pdf', 'xlsx': '.xlsx', 'csv': '.csv'})
    style_term = Utils.dotdict({'rent': 'rent', 'deposits': 'deposits', 'opcash': 'opcash', 'hap': 'hap', 'r4r': 'rr', 'dep': 'dep', 'dep_detail': 'dep_detail', 'corrections': 'corrections'})
    bank_acct_str = ' XXXXXXX1891'
    excluded_file_names = ['desktop.ini', 'beginning_balance_2022.xlsx']
    target_string = Utils.dotdict({'affordable':'Affordable Rent Roll Detail/ GPR Report', 'bank': 'BANK DEPOSIT DETAILS', 'quadel': 'QUADEL', 'r4r': 'Incoming Wire', 'oc_deposit': 'Deposit', 'corrections': 'Chargeback'  })
    status_str = Utils.dotdict({'raw': 'raw', 'processed': 'processed'})

    def __init__(self, path=None, db=None, mode=None):
        self.path = path
        self.db = db
        self.mode = mode
        self.unproc_file_for_testing = None
        self.index_dict = {}
        self.index_dict_iter = {} 
        self.op_cash_list = []

    def iter_build_runner(self):
        print('iter_build_runner; a FileIndexer method')
        self.connect_to_db() # no autodrop
        populate = PopulateTable()
        months_ytd = Utils.months_in_ytd(Config.current_year)
        
        finalized_months = [rec.month for rec in StatusObject().select().where(       (StatusObject.tenant_reconciled==1) &
                    (StatusObject.opcash_processed==1)).namedtuples()]

        # get finalized months

        breakpoint()



        breakpoint()


        processed_fn = [item.fn for item in Findexer().select().where(Findexer.status=='processed').namedtuples()]        
        directory_contents = self.articulate_directory2()        
        unproc_file = list(set(directory_contents) - set(processed_fn))
        self.unproc_file_for_testing = unproc_file    
        index_dict = self.sort_directory_by_extension2() 
        self.load_what_is_in_dir_as_indexed(dict1=self.index_dict_iter)
       
        self.make_a_list_of_indexed(mode=self.doc_mode.xls)
        print('evaluating:', self.indexed_list)
        if self.indexed_list:
            self.find_by_content(style=self.style_term.rent, target_string=self.target_string.affordable)
            self.find_by_content(style=self.style_term.deposits, target_string=self.target_string.bank)
        
        self.make_a_list_of_indexed(mode=self.doc_mode.pdf)
        if self.indexed_list:
            self.find_opcashes()
            self.type_opcashes()
            self.rename_by_content_pdf()

        self.make_a_list_of_indexed(mode=self.doc_mode.csv)
        if self.indexed_list:
            self.get_period_from_scrape_fn()
        print('evaluating:', self.indexed_list)

    def build_index_runner(self):
        self.connect_to_db()
        self.directory_contents = self.articulate_directory()
        self.index_dict = self.sort_directory_by_extension() # this doesn't do the sorting anymore but we still use it
        self.load_what_is_in_dir_as_indexed(dict1=self.index_dict)
        self.make_a_list_of_indexed(mode=self.doc_mode.xls)
        if self.indexed_list:
            self.find_by_content(style=self.style_term.rent, target_string=self.target_string.affordable)
            self.find_by_content(style=self.style_term.deposits, target_string=self.target_string.bank)

        self.make_a_list_of_indexed(mode=self.doc_mode.pdf)
        if self.indexed_list:
            self.find_opcashes()
            self.type_opcashes()
            self.rename_by_content_pdf()

        self.make_a_list_of_indexed(mode=self.doc_mode.csv)
        if self.indexed_list:
            self.get_period_from_scrape_fn()

    def load_mm_scrape(self, list1=None):
        '''still rough and raw'''
        mr_dict = {}
        for fn in self.scrape_path.iterdir():
            if fn.suffix == '.csv' and fn.name not in self.excluded_file_names:
                mr_dict[fn] = fn.lstat().st_ctime

        most_recent_scrape = pd.read_csv(max(list(mr_dict)))        

        deposit_list = []
        for index, row in most_recent_scrape.iterrows():
            if row['Description'] == 'DEPOSIT':
                dict1 = {}
                dict1 = {'date': row['Processed Date'], 'amount': row['Amount'], 'dep_type': 'deposit'}
                deposit_list.append(dict1)
            if 'QUADEL' in row['Description']:
                dict1 = {}
                dict1 = {'date': row['Processed Date'], 'amount': row['Amount'], 'dep_type': 'hap'}
                deposit_list.append(dict1)
      
        return deposit_list

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
        return [item for item in self.path.iterdir()] 

    def sort_directory_by_extension2(self, verbose=None):
        index_dict = {}
        for fn in self.unproc_file_for_testing:
            index_dict[Path.joinpath(self.path, fn)] = (Path(fn).suffix, fn)
        
        self.index_dict_iter = index_dict # for testing, do not just erase this
        return index_dict        

    def sort_directory_by_extension(self, verbose=None):
        for item in self.directory_contents:
            full_path = Path(item)
            if item.name not in self.excluded_file_names:
                self.index_dict[full_path] = (item.suffix, item.name)        
        if verbose:
            pprint(self.index_dict)
        return self.index_dict

    def load_what_is_in_dir_as_indexed(self, dict1={}, autodrop=None, verbose=None):
        insert_dir_contents = [{'path': path, 'fn': name, 'c_date': path.lstat().st_ctime, 'indexed': 'true', 'file_ext': suffix} for path, (suffix, name) in dict1.items()]
        query = Findexer.insert_many(insert_dir_contents)
        query.execute()

    def make_a_list_of_indexed(self, mode=None):
        '''instead of making this from a list in memory (faster), I will make the list from db, so that I can control whether to process it or reprocessed it; otherwise, everything that is in dir will just be fully processed again'''
        if mode == self.doc_mode.xls:   
            self.indexed_list = [(item.path, item.doc_id) for item in Findexer().select().where(Findexer.status==self.status_str.raw).where(
                (Findexer.file_ext == self.ext_mode.xlsx) | (Findexer.file_ext == self.ext_mode.xls)).namedtuples()]

        if mode == self.doc_mode.pdf:   
            self.indexed_list = [(item.path, item.doc_id) for item in Findexer().select().where(Findexer.status==self.status_str.raw).where(Findexer.file_ext == self.ext_mode.pdf).namedtuples()]

        if mode == self.doc_mode.csv:
            self.indexed_list = [(item.path, item.doc_id) for item in Findexer().select().where(Findexer.status==self.status_str.raw).where(Findexer.file_ext == self.ext_mode.csv).namedtuples()]

    def find_by_content(self, style, target_string=None):
        '''kwargs into loop below'''
        '''this should be moved up'''
        if style == self.style_term.rent:
            get_col = 0
            split_col = 11
            split_type = ' '
            date_split = 2

        '''this should be moved up'''
        if style == self.style_term.deposits:
            get_col = 9
            split_col = 9
            split_type = '/'
            date_split = 0              

        for path, doc_id in self.indexed_list:
            part_list = ((Path(path).stem)).split('_')
            if style in part_list:
                df = pd.read_excel(path)
                df = df.iloc[:, 0].to_list()
                if target_string in df:
                    period = self.df_date_wrapper(path, get_col=get_col, split_col=split_col, split_type=split_type, date_split=date_split)
                    find_change = Findexer.get(Findexer.doc_id==doc_id)
                    find_change.period = period
                    find_change.status = self.status_str.processed
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
        
            # this is for testing visibility, maybe optimization later
            self.hap_list.append(hap_iter_one_month)
            self.rr_list.append(rr_iter_one_month)
            self.dep_list.append(dep_iter_one_month)
            self.deposit_and_date_list.append(deposit_and_date_iter_one_month)
            self.corrections_list.append(corrections_sum)

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

    def get_date_from_opcash_name(self, record):
        date_list = record.split('.')[0].split('_')[2:]
        date_list.reverse()
        date_list = ' '.join(date_list)
        return date_list

    def get_period_from_scrape_fn(self):
        for item, doc_id in self.indexed_list:
            scrape_file = Findexer.get(Findexer.doc_id==doc_id)
            date_str = [part.split('_') for part in item.split('/')][-1][-2]
            date_str = '-'.join(date_str.split('-')[0:2])
            scrape_file.status = 'processed'
            scrape_file.period = date_str
            scrape_file.doc_type = 'scrape'
            scrape_file.save()

    def drop_findex_table(self):
        self.db.drop_tables(models=self.create_findex_list)

    def close_findex_table(self):
        self.db.close()


















