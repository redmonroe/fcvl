import csv
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
            'Insurance':'Ending', 
            'MIP': 'Replacement', 
            'Replacement': None,
        }

        for start, end in report_dict.items():
            with open('output.txt', 'rb') as f:
                file1 = open('output.txt', 'r')
                StructDataExtract.merchants_pdf(output_path=output_path, file1=file1, start_string=start, end_string=end)

        file1.close()

    @staticmethod
    def merchants_pdf(output_path=None, file1=None, start_string=None, end_string=None, start_idx=0, end_idx=0):
        index = [(count, line) for count, line in enumerate(file1)]

        start_index = [count for (count, line) in index if start_string in line]
        start_index = start_index[start_idx] + 1
        if end_string != None:
            end_index = [count for (count, line) in index if end_string in line]
            end_index = end_index[end_idx] + 1
        else:
            end_index = 100000

        insurance_lines = [line for (count, line) in index if count > start_index and count < end_index]
        start_balance = insurance_lines[0]
        insurance_lines = [line for line in insurance_lines if 'Beginning' not in line]
        insurance_lines = [line for line in insurance_lines if len(line) > 1] #strips weird page break formatting out
        insurance_lines = [line.split(' ') for line in insurance_lines]
        insurance_lines = [[item for item in list1 if item != ''] for list1 in insurance_lines]

        start_balance = start_balance.split(' ') 
        start_balance = [item for item in start_balance if item.startswith('$') == True] 
        start_balance = [item.strip() for item in start_balance] 
        start_balance = [item.strip('$')  for item in start_balance]
        start_balance = [item.replace(',', '')  for item in start_balance]
        start_balance = [float(item)  for item in start_balance]

        start_row = [str(0) for item in range(6)] 
        start_row[5] = start_balance.pop()

        start_transform = [0 for item in insurance_lines]

        debits_transform = [0 for item in insurance_lines]
        interest_transform = [0 for item in insurance_lines]

        dates = [[item.strip() for item in list1 if item.startswith('0') == True or item.startswith('1') == True] for list1 in insurance_lines]
        dates = [item[0] if len(item) != 0 else item for item in dates]

        amount = [[item for item in list1 if item.startswith('$') == True] for list1 in insurance_lines]
        amount = [[item.strip() for item in list1 if item.startswith('$0.00') == False] for list1 in amount]
        amount = [item[0] if len(item) != 0 else item for item in amount]


        amount = [item.strip('$') if type(item) == str else '0' for item in amount]
        amount = [item.replace(',', '')  for item in amount]
        amount = [float(item)  for item in amount]

        desc = [[item for item in list1 if item.isalpha() == True] for list1 in insurance_lines]
        desc = [" ".join(item) for item in desc]

        result = pd.DataFrame(
            {'date': dates,
            'desc': desc,
            'credit': amount,
            'interest': interest_transform, 
            'debit': debits_transform,
            'start' : start_transform
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
        abs_file_path = os.path.join(output_path, f'escrow/{start_string}.xlsx')
        # breakpoint()

        result.to_excel(abs_file_path)

    def open_pdf_and_output_txt(self, path, txtfile=None):
        db_file = os.path.abspath(os.path.join(os.path.dirname( __file__ ), txtfile))
        with open(path, "rb") as f:
            pdf = pdftotext.PDF(f)

        # Read all the text into one string
        with open(db_file, 'w') as f:
            f.write("\n\n".join(pdf))

        with open(db_file, 'rb') as f:
            file1 = open(db_file, 'r')

        return file1
    
    def select_stmt_by_str(self, path, target_str):
        file1 = self.open_pdf_and_output_txt(path, txtfile='temp_output.txt')
    
        index = [(count, line) for count, line in enumerate(file1)]
        type_test = False
        for count, line in index:
            if target_str in line:
                type_test = True
                return path

    def get_indexed_lines_from_txtfile(self, txtfile, target_str):
        index = [(count, line) for count, line in enumerate(txtfile)]        

        hap_line = [(count, line) for count, line in index if target_str in line]
        hap_line = [line for count, line in hap_line]
        hap_line = [line.split(' ') for line in hap_line]
        target_line = hap_line.pop()

        return target_line, index

    def get_stmt_date(self, indexed_lines, path=None):

        date_line = [line for count, line in indexed_lines if 'Date' and 'FALL CREEK VILLAGE I' in line]
        date_line = date_line[0]
        date_line = date_line.split(' ')
        date_line = [line for line in date_line if '/' in line]
        date1 = date_line.pop()
        date2 = dt.strptime(date1, "%m/%d/%y")
        stmt_date = date2.strftime("%m %Y")

        return stmt_date

    def get_cleaned_target_line(self, target_line, path=None, no_pop=None):
        hap_line = [line for line in target_line if type(line) == str]
        hap_line = [line for line in hap_line if line.isalnum() == False]
        hap_line = [line for line in hap_line if '.' in line]
        hap_line = [line.strip() for line in hap_line]
        hap_line = [line.replace('-', '') for line in hap_line]
        hap_line = [line.replace(',', '')  for line in hap_line]
        for count, line in enumerate(hap_line, 1):
            print(count, line)
        # breakpoint()
        try:
            target = [float(line)  for line in hap_line]
        except ValueError as e:
            print(e)
            print(f'path: {path}')
            target = 0
            breakpoint()
        if no_pop:
            target = target.pop()

        return target

    def get_date_from_line(self, target_line):
        hap_line = [line for line in target_line if type(line) == str]
        hap_line = [line for line in hap_line if line.isalnum() == False]
        hap_line = [line for line in hap_line if '/' in line]
        hap_line = [line.strip() for line in hap_line]
        date = [line.replace(',', '')  for line in hap_line]
        date = date.pop()

        return date

    def nbofi_pdf_extract_deposit(self, path, style=None, target_str=None):
        txtfile = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'temp_output.txt' ))
        file1 = self.open_pdf_and_output_txt(path, txtfile=txtfile)

        line_list = []
        date_list = []
        index = [(count, line) for count, line in enumerate(file1)]
        line = [(count, line) for count, line in index if target_str in line]
        line = [line for count, line in line]
        deposit_lines = [line.split(' ') for line in line]
        deposit_lines = [line for line in deposit_lines if 'Correction' not in line]
        for hap_line in deposit_lines:
            stmt_date = self.get_stmt_date(index)
            stmt_year = stmt_date[-4:]
            target = self.get_cleaned_target_line(hap_line, path=path)
            target_date = self.get_date_from_line(hap_line)
            target_date = target_date + '/' + stmt_year
            date_list.append(target_date)
            line_list.append(target[0])

        line_list.pop(0)
        date_list.pop(0)
        combined_list = list(zip(date_list, line_list))
        if style =='dep_detail':
            self.deposits_list = []
            self.deposits_list.append({stmt_date: combined_list})
            return self.deposits_list
        return stmt_date, sum(line_list)

    def nbofi_pdf_extract_hap(self, path, style=None, target_str=None):
        file1 = self.open_pdf_and_output_txt(path, txtfile='temp_output.txt')
        hap_line, index = self.get_indexed_lines_from_txtfile(file1, target_str)
        target = self.get_cleaned_target_line(hap_line)

        date2 = self.get_stmt_date(index, path=path)

        return date2, target[0]

    def nbofi_pdf_extract_rr(self, path, style=None, target_str=None):
        file1 = self.open_pdf_and_output_txt(path, txtfile='temp_output.txt')        
        line_list = []
        index = [(count, line) for count, line in enumerate(file1)]
        stmt_date = self.get_stmt_date(index)
        line = [(count, line) for count, line in index if target_str in line]
        line = [line for count, line in line]
        lines = [line.split(' ') for line in line]
        if line == []:
            return stmt_date, 0
        line = line.pop()
        for hap_line in lines:
            target = self.get_cleaned_target_line(hap_line)
            line_list.append(target[0])

        return stmt_date, sum(line_list)

    def corrections_helper(self, list1=None, target_str1=None, stmt_date=None):

        lines = [line.split(' ') for line in list1]
        lines = [line for line in lines if target_str1 not in line][0]
        lines = [line.rstrip() for line in lines if line != '']
        lines = [line for line in lines if line.endswith('-')]
        sum_corr = sum([float(line.replace('-', '')) for line in lines])
        return sum_corr

    def corrections_helper2(self, list1=None):
        lines = [line.split(' ') for line in list1]
        lines = [' '.join(line).split() for line in lines]
        lines = sum(lines, [])
        lines = [line for line in lines if line.endswith('-')]
        return sum([float(line.replace('-', '')) for line in lines])
    
    def nbofi_pdf_extract_corrections(self, path, style=None, target_str=None, target_str2=None, date=None):
        file1 = self.open_pdf_and_output_txt(path, txtfile='temp_output.txt')
        index = [(count, line) for count, line in enumerate(file1)]
        stmt_date = self.get_stmt_date(index)
        corrections = [line for count, line in index if target_str in line]
        chargebacks = [line for count, line in index if target_str2 in line]

        if chargebacks != []:
            sum_chargebacks = self.corrections_helper(list1=chargebacks, target_str1='Fee', stmt_date=stmt_date)
        else: 
            sum_chargebacks = 0

        if corrections != []:
            sum_corrections = self.corrections_helper2(list1=corrections)
        else:
            sum_corrections = 0
            
        total_corrections = sum_corrections + sum_chargebacks
        print('Extracting corrections & chargebacks from opcash:', date, 'total:', total_corrections)
        
        return stmt_date, total_corrections
    
 
