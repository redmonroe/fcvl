from backend import ProcessingLayer, PopulateTable, StatusObject
from config import Config
from utils import Utils
from backend import UrQuery

class WhereAreWe(ProcessingLayer):

    def __init__(self, **kwargs):
        self.build = kwargs['build']
        self.path = kwargs['path']
        self.full_sheet = kwargs['full_sheet']
        self.db = self.build.main_db
        self.populate = PopulateTable()

    def _todo_(self):
        """basic idea:

        When we start it up at the end of the month, user is basically confused about what the state of the db is
        
        
        
        
        """
        pass

    def query_practice(self):
        first_dt, last_dt = self.populate.make_first_and_last_dates(date_str='2022-01')

        query_fields = {'move_in_date': first_dt, 'move_out_date': last_dt}

        query = UrQuery()
        query.ur_query(model_str='Tenant', query_dict=query_fields)
        results = query.ur_query(model_str='Tenant')
        filt_results = [row.move_in_date for row in results.namedtuples()]
        print(filt_results)

        print(filt_results)
    

    def select_month(self, range=None):
        """could set explicit range if wanted"""

        """
        PULL in the shit from player: SHOW_STATUS()
        
        
        
        
        
        """


        query = UrQuery()
        date, _ = Utils.enumerate_choices_for_user_input(chlist=Utils.months_in_ytd(Config.current_year))
        first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=date)

        ## beginning month rent roll and vacancy
        tenants_at_1, vacants, tenants_at_2 = self.populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

        ## opcash 
        opcash = [row.dep_sum for row in query.ur_query(model_str='OpCash', query_tup= [('date', first_dt), ('date', last_dt)], operators_list=['>=', '<=']).namedtuples()]

        ## did opcash reconcile to deposits
        did_opcash_or_scrape_reconcile_with_deposit_report = [row for row in query.ur_query(model_str='StatusObject').namedtuples() if row.month == date]
        # breakpoint()  

        
        self.print_rows(date=date, beg_tenants=tenants_at_1, opcash=opcash, reconcile_status=did_opcash_or_scrape_reconcile_with_deposit_report)

    def print_rows(self, date=None, **kwargs):
        print(f'current month: {date}')
        print(f'reconciliation status: {kwargs["reconcile_status"]}')
        print(f'occupieds at first of month: {len(kwargs["beg_tenants"])}')

    def show_status_table(self, **kw):

        """ this exists to show the status of the FileIndexer table
        table cols:
            - months year to date
            - deposit detail reports by month
            - rentroll reports by month
            - scrapes available by month
            - tenant payments (from ?)
            - non-tenant payments (from what doc?)
            - total payments (from ??)
            - total deposits related to payments from oc
            - deposit corrections
            - what do I want to show whether reconciled?
        """
        from file_indexer import FileIndexer  # circular import work around

        populate  = PopulateTable()
        findex = FileIndexer(path=kw['path'], db=kw['db'])

        months_ytd, unfin_month = findex.test_for_unfinalized_months()    

        status_objects = populate.get_all_status_objects() 
    
        deposits = self.status_table_finder_helper(months_ytd, type1='deposits')

        dep_recon = populate.get_all_findexer_recon_status(type1='deposits')
        
        deposit_rec = [(row.recon) for row in Findexer.select()]
        rents = self.status_table_finder_helper(months_ytd, type1='rent')
        scrapes = self.status_table_finder_helper(months_ytd, type1='scrape') 

        tp_list, ntp_list, total_list, opcash_amt_list, dc_list = self.make_mega_tup_list_for_table(months_ytd)

        dl_tup_list = list(zip(deposits, rents, scrapes, tp_list, ntp_list, total_list, opcash_amt_list, dc_list, dep_recon)) 
        
        header = ['month', 'deps', 'rtroll', 'scrapes', 'ten_pay', 'ntp', 'tot_pay',  'oc_dep', 'dc', 'pay_rec_f?']

        # ['oc_proc', 'ten_rec', 'rs_rec', 'scrape_rec']
        table = [header]
        for item, dep in zip(status_objects, dl_tup_list):
            row_list = []
            row_list.append(item.month)
            row_list.append(str(dep[0][0]))
            row_list.append(str(dep[1][0]))
            row_list.append(str(dep[2][0]))
            row_list.append(str(dep[3][0]))
            row_list.append(str(dep[4][0]))
            row_list.append(str(dep[5][0]))
            row_list.append(str(dep[6][0]))
            row_list.append(str(dep[7][0]))
            row_list.append(str(dep[8][0]))
            # row_list.append(str(item.opcash_processed))
            # row_list.append(str(item.tenant_reconciled))
            # row_list.append(str(item.rs_reconciled))
            # row_list.append(str(item.scrape_reconciled))
            table.append(row_list)
        
        print('\n'.join([''.join(['{:9}'.format(x) for x in r]) for r in table]))

    def status_table_finder_helper(self, months_ytd, type1=None):
        populate  = PopulateTable()
        output = populate.get_all_findexer_by_type(type1=type1)
        output_months = [month for name, month in output]
        return [(True, month) if month in output_months else (False, month) for month in months_ytd] 

    def make_mega_tup_list_for_table(self, months_ytd):
        tp_list = []
        ntp_list = []
        total_list = []
        opcash_amt_list = []
        dc_list = []

        for month in months_ytd:
            first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=month)
        
            ten_payments = sum([float(row[2]) for row in self.populate.get_payments_by_tenant_by_period(first_dt=first_dt, last_dt=last_dt)])
            tp_tup = (ten_payments, first_dt)
            tp_list.append(tp_tup)

            ntp = sum(self.populate.get_ntp_by_period(first_dt=first_dt, last_dt=last_dt))
            ntp_tup = (ntp, first_dt)
            ntp_list.append(ntp_tup)

            total = float(ten_payments) + float(ntp)
            total_list.append((total, first_dt))
            
            opcash = self.populate.get_opcash_by_period(first_dt=first_dt, last_dt=last_dt)
            if opcash:
                oc_tup = (opcash[0][4], first_dt)
            else:
                oc_tup = (0, first_dt)
            opcash_amt_list.append(oc_tup)

            if opcash:
                dc_tup = (opcash[0][5], first_dt)
                dc_list.append(dc_tup)
            else:
                """branch if not opcash for month to try to get corr_amount from findexer"""
                '''should assert it with a month reading'''
                '''
                '''
                scrapes = self.populate.get_scrape_detail_by_month_by_type(type1='corr', first_dt=first_dt, last_dt=last_dt)

                dc_tup = (sum([float(n) for n in scrapes]), first_dt)
                print('current branch')
                dc_list.append(dc_tup)


        return tp_list, ntp_list, total_list, opcash_amt_list, dc_list  