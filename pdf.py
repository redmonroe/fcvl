import os
import csv, itertools
import pdftotext
import pandas as pd
from datetime import datetime as dt
from config import Config
import math 
from collections import defaultdict

class StructDataExtract:
    def merchants_pdf(self, file1=None, start_string=None, end_string=None, start_idx=0, end_idx=0):
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

        result.to_csv(f'{start_string}.csv')
    
    def merchants_pdf_extract():
        script_dir = os.path.dirname(__file__)
        rel_path = 'data/escrow_ytd'
        abs_file_path = os.path.join(script_dir, rel_path)

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
                merchants_pdf(file1=file1, start_string=start, end_string=end)

        file1.close()

    def qb_extract_p_and_l(self, filename, keyword=None, path=None):
        db_file = 'data/qb_output.txt'
        
        abs_file_path = os.path.join(path, filename)
        print(abs_file_path)
        df = pd.read_excel(abs_file_path)
        
        extract = df.loc[df['Fall Creek Village I'].str.contains(keyword, na=False)]
        extract = extract.values[0]
        amount = [item for item in extract if type(item) != str]
        
        df = pd.read_excel(abs_file_path, header=4)
        date = list(df.columns)
        date = date[1:]
        target_date_dict = dict(zip(date, amount))
        total = {k: v for (k, v) in target_date_dict.items() if 'Total' in k}
        dict_wo_total = {k: v for (k, v) in target_date_dict.items() if 'Total' not in k}
        dict_wo_total_and_mtd = {k:v for (k, v) in dict_wo_total.items() if '-' not in k}
        fixed_target_date_dict = {dt.strptime(k, '%b %Y'): v for (k, v) in dict_wo_total_and_mtd.items() if '-' not in k}
        fixed_target_date_dict = {k.strftime('%m %Y'): v for (k, v) in fixed_target_date_dict.items()}
        fixed_target_date_dict = {dateq: (0 if math.isnan(amount) else amount) for (dateq, amount) in fixed_target_date_dict.items() }
    
        return fixed_target_date_dict

    def qb_extract_security_deposit(self, filename, path=None):

        abs_file_path = os.path.join(path, filename)
        
        df = pd.read_excel(abs_file_path)    
        
        df = df.loc[df['Unnamed: 2'].str.contains('Deposit', na=False)]
        dates_list = list(df['Unnamed: 1'])
        amount_list = list(df['Unnamed: 8'])
        tup_list = [(dt.strptime(dateq,  '%m/%d/%Y'), amount) for dateq, amount in zip(dates_list, amount_list)]
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
    
    def nbofi_pdf_extract_hap(self, filename, path=None):
        db_file = 'data/bank_output.txt'

        abs_file_path = os.path.join(path, filename)
        print('path to statement:', abs_file_path)
        
        with open(abs_file_path, "rb") as f:
            pdf = pdftotext.PDF(f)

        # Read all the text into one string
        with open(db_file, 'w') as f:
            f.write("\n\n".join(pdf))

        with open(db_file, 'rb') as f:
            file1 = open(db_file, 'r')
        
        # index = [(count, line) for count, line in enumerate(file1)]
        
        # hap_line = [(count, line) for count, line in index if 'QUADEL' in line]
        # hap_line = [line for count, line in hap_line]
        # hap_line = [line.split(' ') for line in hap_line]
        # hap_line = hap_line.pop()
        # hap_line = [line for line in hap_line if type(line) == str]
        # hap_line = [line for line in hap_line if line.isalnum() == False]
        # hap_line = [line for line in hap_line if '.' in line]
        # hap_line = [line.strip() for line in hap_line]
        # hap_line = [line.replace(',', '')  for line in hap_line]
        # hap_line = [float(line)  for line in hap_line]
        # hap = hap_line.pop()

        # date_line = [line for count, line in index if 'Date' in line]
        # date_line = date_line[0]
        # date_line = date_line.split(' ')
        # date_line = [line for line in date_line if '/' in line]
        # date1 = date_line.pop()
        # date2 = dt.strptime(date1, "%m/%d/%y")
        # date2 = date2.strftime("%m %Y")

        # return date2, hap
