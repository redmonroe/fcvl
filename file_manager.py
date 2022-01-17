from config import Config
import os
from os import listdir
from os.path import isfile, join
from google_api_calls_abstract import simple_batch_update

def path_to_statements(path=None, keyword=None):
    script_dir = os.path.dirname(__file__)																										
    abs_file_path = os.path.join(script_dir, path)
    onlyfiles = [f for f in listdir(abs_file_path) if keyword in f]
    return abs_file_path, onlyfiles

def write_hap(service, sheet_id, wrange, dim, target_date_dict, dateq, data):
    match_bool = False 
    for qbo_date, amount in target_date_dict.items():
        if dateq == qbo_date:
            print('match', dateq, qbo_date)
            match_amount = amount
    if match_amount == data:
        match_bool = True
    if match_bool == True:
        data = [match_amount]
        simple_batch_update(service, sheet_id, wrange, data, dim)    
    else:
        print("you're bank statement & qbo records do not match")
        print("you probably have that problem where the xlsx file isn't working until it is saved: rewrite to csv, or postgres?")


