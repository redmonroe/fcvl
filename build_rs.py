import time
from datetime import datetime
from config import Config, my_scopes
from db_utils import DBUtils
from google_api_calls_abstract import GoogleApiCalls
from auth_work import oauth
from file_indexer import FileIndexer
from checklist import Checklist
from setup_year import YearSheet
from setup_month import MonthSheet
import dataset
import pandas as pd
from google_api_calls_abstract import GoogleApiCalls

class BuildRS(MonthSheet):
    def __init__(self, full_sheet=None, path=None, mode=None, discard_pile=None, db=None, findex_table=None, test_service=None, sleep=None, checklist_db=None, findex_db=None):
        if mode == 'testing':
            self.db = Config.test_build_db
            self.mode = 'testing'
            self.full_sheet=Config.TEST_RS
            self.findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, mode='testing', table='findex')
            self.service = test_service
            self.mformat = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing', test_service=self.service)
            self.calls = GoogleApiCalls()
        elif mode == 'dev':
            self.full_sheet = full_sheet
            self.service = oauth(my_scopes, 'sheet')
            self.findex = FileIndexer(path=path, discard_pile=discard_pile, db=findex_db, table=findex_table)
            self.checklist_db = checklist_db

        self.sleep = sleep
        self.wrange_pay = '!K2:K68'
        self.wrange_ntp = '!K71:K71'
        self.file_input_path = path
        self.user_text = f'Options:\n PRESS 1 to show current sheets in RENT SHEETS \n PRESS 2 TO VIEW ITEMS IN {self.file_input_path} \n PRESS 3 for MONTHLY FORMATTING, PART ONE (that is, update intake sheet in {self.file_input_path} (xlsx) \n PRESS 4 for MONTHLY FORMATTING, PART TWO: format rent roll & subsidy by month and sheet\n >>>'
        self.df = None
        self.tablename = 'build'
        self.checklist = Checklist()
        self.hap_list = self.findex.hap_list
        self.rr_list = self.findex.rr_list
        self.dep_list = self.findex.dep_list
        self.deposit_and_date_list = self.findex.deposit_and_date_list
        self.checklist = None

    def automatic_build(self, checklist_mode=None, key=None):
        '''this is the hook into the program for the checklist routine'''
        self.checklist = Checklist(db=self.checklist_db)
        self.checklist.make_checklist(mode=checklist_mode)
        self.findex.reset_files_for_testing()
        self.findex.build_index_runner()

        # start with what documents I have -> then fire formatting off of that

        self.check_triad_processed()






        # if rs_write == False or yfor == False try making again?
        # object initialization should be moved into object imho
        # ys = YearSheet(full_sheet=self.full_sheet, checklist=self.checklist)
        # shnames = ys.auto_control()

        # init findexer

        


   
            # buildrs.automatic_build(key='DEP')
    # buildrs.automatic_build(key='RENTROLL')
    # buildrs.automatic_build(key='cash')
    # buildrs.show_table()


        # if key == 'ALL':
        #     ## THIS DOES NOT WORK YET
        # else:
        #     list_true = self.get_by_kw(key=key, selected=items_true)

    def check_triad_processed(self):
        print('\nsearching findex_db for processed files')


        # def extraction_wrapper_for_transaction_detail(choice, func=None, path=None, keyword=None):

            # path, files = path_to_statements(path=path, keyword=keyword)    
            # #date_dict_groupby_m = qb_extract_security_deposit(files[0], path=path)
            # date_dict_groupby_m = func(files[0], path=path)
            # result = {amount for (dateq, amount) in date_dict_groupby_m.items() if dateq == choice}
            # is_empty_set = (len(result) == 0)
            # if is_empty_set:
            #     data = [0]
            #     return data
            # else:
            #     data = [min(result)]
            #     return data

        # sec_dep_qb = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_security_deposit, path=path_security_deposit, keyword='Security')

        items_true = self.get_processed_items_list()
        period_set = set()
        for item in items_true:
            period_set.add(item['period'])
            print(item, '*')

        period_list = list(period_set)

        count = 0
        for proc_item in items_true:
            for period in period_list:
                if period == proc_item['period'] and proc_item['status'] == 'processed':
                    count += 1
                    print(period, count)
            # rentrolls_true = self.get_by_kw(key='RENTROLL', selected=items_true)
            # deposits_true = self.get_by_kw(key='DEP', selected=items_true)
            # opcash_true = self.get_by_kw(key='cash', selected=items_true)
            
        # breakpoint()
    
    def auto_build_storage_to_erase(self):

        '''rentroll and monthly formatting'''
        if key == 'RENTROLL':
            for item in list_true:
                dt_object = self.fix_date(item['period'])
                '''trigger formatting of dt_object named sheet'''
                self.mformat.export_month_format(dt_object)
                self.mformat.push_one_to_intake(input_file_path=item['path'])
                self.checklist.check_mfor(dt_object)
                self.month_write_col(dt_object)
                self.checklist.check_rr_proc(dt_object)

        '''deposits push'''
        if key == 'DEP':
            for item in list_true:
                '''get raw deposit items to sql'''
                dt_object = self.fix_date(item['period'])
                df = self.read_excel(path=item['path'])
                df = self.remove_nan_lines(df=df)
                self.to_sql(df=df)
                dt_code = item['period'][-2:]
                '''group objects by tenant name or unit: which was it?'''
                payment_list, grand_total, ntp, df = self.push_to_sheet_by_period(dt_code=dt_code)
                self.write_payment_list(dt_object, payment_list)
                self.write_ntp(dt_object, [str(ntp)])
                self.print_summary(payment_list, grand_total, ntp, df)
                self.checklist.check_dep_proc(dt_object)

        if key == 'cash':
            if self.mode == 'testing':
                self.findex.build_index_runner()
                for hap, rr in zip(self.findex.hap_list, self.findex.rr_list):
                    dict1 = {}
                    dict1['formatted_hap_date'] = self.fix_date2(list(hap.keys())[0])
                    dict1['hap_date'] = list(hap.keys())[0]
                    dict1['hap_amount'] = list(hap.values())[0][0]
                    dict1['rr_date'] = list(rr.keys())[0]
                    dict1['rr_amount'] = list(rr.values())[0][0]

                for deposit_group in self.findex.deposit_and_date_list:
                    dict1['deposit_date'] = list(deposit_group.keys())[0]
                    dict1['deposit_list'] = list(deposit_group.values())[0]

                self.export_deposit_detail(data=dict1)
                self.checklist.check_depdetail_proc(dict1['hap_date'])
                self.write_sum_forumula1()
                reconciled_bool = self.check_totals_reconcile()
                if reconciled_bool:
                    self.checklist.check_grand_total_ok(dict1['hap_date'])
                else:
                    month = dict1['hap_date']
                    print(f'rent sheet for {month} does not balance.')

        return items_true

    def push_to_sheet_by_period(self, dt_code):
        print('pushing to sheet with code:', dt_code)
        db = self.db
        tablename = self.tablename
        results_list = []
        for result in db[tablename]:
            if result['dt_code'] == dt_code:
                results_list.append(result)

        df = self.lists_to_df(results_list)
        grand_total = self.grand_total(df=df)        
        df, laundry_income = self.return_and_remove_ntp(df=df, col='unit', remove_str='0')        
        df = self.group_df(df=df)
        no_unit_number = self.group_df(df=laundry_income, just_return_total=True)

        unit_index = self.get_units()
        unit_index = self.make_unit_index(unit_index)

        unit_index_df = pd.DataFrame(unit_index, columns= ['Rank', 'unit'])
        payment_list = self.merge_indexes(df, unit_index_df) 

        ntp = no_unit_number 
        return payment_list, grand_total, ntp, df

    def print_summary(self, payment_list, grand_total, ntp, df):
        print(df.head(10))
        print('grand_total:', grand_total)
        print('tenant_payments:', sum(payment_list))
        print('ntp:', ntp)
        assert sum(payment_list) + ntp == grand_total, 'the total of your tenant & non-tenant payments does not match.  you probably need to catch more types of nontenant transactions'
        print('payments balance=ok!')

    def write_ntp(self, sheet_choice, data_string):
        self.calls.update_int(self.service, self.full_sheet, data_string, sheet_choice + self.wrange_ntp, 'USER_ENTERED')
        
    def write_payment_list(self, sheet_choice, data_list):
        self.calls.simple_batch_update(self.service, self.full_sheet, sheet_choice + self.wrange_pay, data_list, "COLUMNS")

    def merge_indexes(self, df1, df2):
        merged_df = pd.merge(df1, df2, on='unit', how='outer')
        final_df = merged_df.sort_values(by='Rank', axis=0)
        final_df = final_df.fillna(0)
        payment_list = final_df['pay'].tolist()
        return payment_list
    
    def lists_to_df(self, lists):
        df = pd.DataFrame(lists)
        return df

    def return_and_remove_ntp(self, df, col=None, remove_str=None):
        ntp_item = df.loc[df[col] == remove_str]
        for item in ntp_item.index:
            df.drop(labels=item, inplace=True)
        return df, ntp_item

    def fix_date2(self, date):
        dt_object = datetime.strptime(date, '%m %Y')
        dt_object = datetime.strftime(dt_object, '%b %Y').lower()
        return dt_object

    def fix_date(self, date):
        dt_object = datetime.strptime(date, '%Y-%m')
        dt_object = datetime.strftime(dt_object, '%b %Y').lower()
        return dt_object

    def grand_total(self, df):
        grand_total = sum(df['pay'].tolist())
        return grand_total
    
    def group_df(self, df, just_return_total=False):
        df = df.groupby(['name', 'unit'])['pay'].sum()
        if just_return_total:
            df = df[0]
        return df

    def get_units(self):
        results_list = Config.units
        return results_list

    def to_sql(self, df):
        table = self.db[self.tablename]
        table.drop()
        for index, row in df.iterrows():
            table.insert(dict(
                deposit_id=row[0],
                unit=row[1],
                name=row[2],
                date=row[3],
                pay=float(row[4]),
                dt_code=row[5],                
                ))

    def make_unit_index(self, units):
        final_list = []
        idx_list = []
        for index, unit in enumerate(units): # indexes units from sheet
            idx_list.append(int(index))
            final_list.append(unit)

        unit_index = tuple(zip(idx_list, final_list))
        return unit_index

    def show_table(self, table=None):

        db = self.db
        for results in db[self.tablename]:
            print(results)

    def get_by_kw(self, key=None, selected=None):
        selected_items = []
        for item in selected:
            name = item['fn'].split('_')
            if key in name:
                selected_items.append(item)
        return selected_items

    def get_processed_items_list(self):
        print('\nmaking list of processed items')
        items_true = []
        for item in self.findex.db[self.findex.tablename]:
            if item['status'] == 'processed':
                items_true.append(item)
        
        return items_true 

    def read_excel(self, path, verbose=False):
        df = pd.read_excel(path, header=9)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(20))

        columns = ['deposit_id', 'unit', 'name', 'date_posted', 'amount', 'date_code']

        bde = df['BDEPID'].tolist()
        unit = df['Unit'].tolist()
        name = df['Name'].tolist()
        date = df['Date Posted'].tolist()
        pay = df['Amount'].tolist()
        dt_code = [datetime.strptime(item, '%m/%d/%Y') for item in date if type(item) == str]
        dt_code = [str(datetime.strftime(item, '%m')) for item in dt_code]

        zipped = zip(bde, unit, name, date, pay, dt_code)
        self.df = pd.DataFrame(zipped, columns=columns)

        return self.df

    def remove_nan_lines(self, df):
        df = df.dropna(thresh=2)
        df = df.fillna(0)
        return df

    def set_user_choice(self):
        self.user_choice = int(input(self.user_text))

    def reset_full_sheet(self):
        titles_dict = self.show_current_sheets()
        calls = GoogleApiCalls()

        intake_ok = False
        for name, id2 in titles_dict.items():
            if name == 'intake':
                intake_ok = True
                calls.clear_sheet(self.service, self.full_sheet, f'intake!A1:ZZ100')
                break

        if intake_ok == False:
            calls.make_one_sheet(self.service, self.full_sheet, 'intake')
        
        # removal all sheets but intake
        for name, id2, in titles_dict.items():
            if name != 'intake':
                calls.del_one_sheet(self.service, self.full_sheet, id2)
       
        time.sleep(self.sleep)
        print(f'sleeping for {self.sleep} seconds')
    
    def reset_databases(self):
        pass



