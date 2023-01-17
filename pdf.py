import csv
import io
import itertools
import math
import os
from collections import defaultdict
from datetime import datetime as dt
from pprint import pprint

import pandas as pd
import pdftotext

from config import Config


class StructDataExtract:

    def __init__(self):
        self.deposits_list = None

    @staticmethod
    def escrow_wrapper(output_path=None):
        StructDataExtract.merchants_pdf_extract(output_path=output_path)

    @staticmethod
    def merchants_pdf_extract(output_path=None):
        dir1 = Config.TEST_FCVL_BASE
        rel_path = 'escrow/escrow_history_05_2022.pdf'
        abs_file_path = os.path.join(dir1, rel_path)

        with open(abs_file_path, "rb") as f:
            pdf = pdftotext.PDF(f)

        # Read all the text into one string
        with open('output.txt', 'w') as f:
            f.write("\n\n".join(pdf))

        report_dict = {
            'Insurance': 'Ending',
            'MIP': 'Replacement',
            'Replacement': None,
        }

        for start, end in report_dict.items():
            with open('output.txt', 'rb') as f:
                file1 = open('output.txt', 'r')
                StructDataExtract.merchants_pdf(
                    output_path=output_path, file1=file1, start_string=start, end_string=end)

        file1.close()

    @staticmethod
    def merchants_pdf(output_path=None, file1=None, start_string=None, end_string=None, start_idx=0, end_idx=0):
        index = [(count, line) for count, line in enumerate(file1)]

        start_index = [count for (count, line)
                       in index if start_string in line]
        start_index = start_index[start_idx] + 1
        if end_string != None:
            end_index = [count for (count, line)
                         in index if end_string in line]
            end_index = end_index[end_idx] + 1
        else:
            end_index = 100000

        insurance_lines = [
            line for (count, line) in index if count > start_index and count < end_index]
        start_balance = insurance_lines[0]
        insurance_lines = [
            line for line in insurance_lines if 'Beginning' not in line]
        insurance_lines = [line for line in insurance_lines if len(
            line) > 1]  # strips weird page break formatting out
        insurance_lines = [line.split(' ') for line in insurance_lines]
        insurance_lines = [[item for item in list1 if item != '']
                           for list1 in insurance_lines]

        start_balance = start_balance.split(' ')
        start_balance = [
            item for item in start_balance if item.startswith('$') == True]
        start_balance = [item.strip() for item in start_balance]
        start_balance = [item.strip('$') for item in start_balance]
        start_balance = [item.replace(',', '') for item in start_balance]
        start_balance = [float(item) for item in start_balance]

        start_row = [str(0) for item in range(6)]
        start_row[5] = start_balance.pop()

        start_transform = [0 for item in insurance_lines]

        debits_transform = [0 for item in insurance_lines]
        interest_transform = [0 for item in insurance_lines]

        dates = [[item.strip() for item in list1 if item.startswith(
            '0') == True or item.startswith('1') == True] for list1 in insurance_lines]
        dates = [item[0] if len(item) != 0 else item for item in dates]

        amount = [[item for item in list1 if item.startswith(
            '$') == True] for list1 in insurance_lines]
        amount = [[item.strip() for item in list1 if item.startswith(
            '$0.00') == False] for list1 in amount]
        amount = [item[0] if len(item) != 0 else item for item in amount]

        amount = [item.strip('$') if type(
            item) == str else '0' for item in amount]
        amount = [item.replace(',', '') for item in amount]
        amount = [float(item) for item in amount]

        desc = [[item for item in list1 if item.isalpha() == True]
                for list1 in insurance_lines]
        desc = [" ".join(item) for item in desc]

        result = pd.DataFrame(
            {'date': dates,
             'desc': desc,
             'credit': amount,
             'interest': interest_transform,
             'debit': debits_transform,
             'start': start_transform
             })

        result.loc[-1] = start_row  # adding a row
        result.index = result.index + 1  # shifting index
        result = result.sort_index()  # sorting by index

        to_move_index = []
        for index, dsc in result.iterrows():
            if 'Interest' in dsc['desc']:
                kdict = {index: dsc['credit']}
                to_move_index.append(kdict)

        for index, value in [(k, v) for pair in to_move_index for (k, v) in pair.items()]:
            result.at[index, 'interest'] = value
            result.at[index, 'credit'] = 0.00

        to_move_index2 = []
        for index, dsc in result.iterrows():
            if 'Disbursement' in dsc['desc']:
                kdict = {index: dsc['credit']}
                to_move_index2.append(kdict)

        for index, value in [(k, v) for pair in to_move_index2 for (k, v) in pair.items()]:
            result.at[index, 'debit'] = value
            result.at[index, 'credit'] = 0.00

        print(result.head(1))
        abs_file_path = os.path.join(
            output_path, f'escrow/{start_string}.xlsx')

        result.to_excel(abs_file_path)
        
    def open_pdf_and_return_one_str(self, path):
        with open(path, "rb") as f:
            pdf = pdftotext.PDF(f)   
            
        f1 = "\n\n".join(pdf)
                     
        f2 = io.StringIO(f1)
            
        return f2

    def open_pdf_and_output_txt(self, path, txtfile=None):
        db_file = os.path.abspath(os.path.join(
            os.path.dirname(__file__), txtfile))
        with open(path, "rb") as f:
            pdf = pdftotext.PDF(f)
        
        # Read all the text into one string
        with open(db_file, 'w') as f:
            f.write("\n\n".join(pdf))

        with open(db_file, 'rb') as f:
            file1 = open(db_file, 'r')

        return file1

    def select_stmt_by_str(self, path, target_str):
        file1 = self.open_pdf_and_return_one_str(path)

        index = [(count, line) for count, line in enumerate(file1)]
        type_test = False
        for count, line in index:
            if target_str in line:
                type_test = True
                return path

    def get_stmt_date(self, indexed_lines, path=None):

        date_line = [
            line for count, line in indexed_lines if 'Date' and 'FALL CREEK VILLAGE I' in line]
        date_line = date_line[0]
        date_line = date_line.split(' ')
        date_line = [line for line in date_line if '/' in line]
        date1 = date_line.pop()
        date2 = dt.strptime(date1, "%m/%d/%y")
        stmt_date = date2.strftime("%m %Y")

        return stmt_date
    
    def nbofi_pdf_extract(self, path, style=None, target_list=None):
        dfs = []
        file1 = self.open_pdf_and_return_one_str(path)
        index = [(count, line) for count, line in enumerate(file1)]
        stmt_date = self.get_stmt_date(index)
        stmt_year, stmt_month = stmt_date[-4:], stmt_date[:2]
        for target_str in target_list:
            lines = [line for count, line in index if target_str in line]
            if target_str == 'Deposit':
                lines = [line for line in lines if 'Correction' not in line]
            lines = [line.split(' ') for line in lines]
            linex = []
            for list1 in lines:
                list1 = [item for item in list1 if item != '']
                list1[0] = list1[0] + '/' + stmt_year
                if list1[1] == 'Incoming':
                    list1 = [list1[0], 'rr', list1[3]]
                if list1[1] == 'CONS':
                    list1 = [list1[0], 'hap', list1[5]]
                if list1[1] == 'Chargeback' and list1[2] != 'Fee':
                    list1 = [list1[0], 'chargeback', list1[3]]
                if list1[2] == 'Correction':
                    list1 = [list1[0], 'correction', list1[4]]
                if list1[1] == 'Vault':
                    list1 = [list1[0], 'Deposit', list1[4]]
                    
                list1.append(stmt_year + ' ' + stmt_month)
                    
                linex.append(list1)
            df = pd.DataFrame(linex)
            dfs.append(df)
                
        df = pd.concat(dfs)
        df = df[df[1].str.contains('Deposits/Credits')==False]
        df = df[df[2].str.contains('Fee')==False]
        df[2] = df[2].str.replace(r'\n', '', regex=True)
        df[2] = df[2].str.replace(',', '')
        df[2] = df[2].str.replace('-', '')
        df[0] = pd.to_datetime(df[0], format='%m/%d/%Y')
        df[2] = df[2].astype('float64') 
        df = df.drop(df.columns[[4, 5, 6]], axis=1)
        df = df.rename({0: 'date', 1: 'type', 2: 'amount', 3: 'period'}, axis='columns')
        return df, stmt_year + '-' + stmt_month

 
