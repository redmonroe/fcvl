from backend import PopulateTable, ProcessingLayer, StatusObject, UrQuery
from config import Config
from errors import Errors
from scrape import Scrape
from utils import Utils


class WhereAreWe(ProcessingLayer):

    def __init__(self, **kwargs):
        self.build = kwargs['build']
        self.path = kwargs['path']
        self.full_sheet = kwargs['full_sheet']
        self.db = self.build.main_db
        self.populate = PopulateTable()
        self.player = ProcessingLayer()
        self.most_recent_good_month, self.good_months = self.player.get_mr_good_month()
        self.ur_query = UrQuery()

    def _todo_(self):
        """basic idea:

        When we start it up at the end of the month, user is basically confused about what the state of the db is
        
        
        
        
        """
        pass

    def load_canon(self):
        targets = ['morris, michael', 'woods, leon', 'fielder, emily']

        date = '2022-01'
        first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=date)

        for target in targets:
            self.ur_query.all_available_by_fk_by_period(target=target, first_dt=first_dt, last_dt=last_dt)

    def select_month(self, date=None):
        """could set explicit range if wanted"""

        query = UrQuery()

        first_incomplete_month = Utils.get_next_month(target_month=self.most_recent_good_month)

        if date:
            date = self.most_recent_good_month
        else:            
            date, _ = Utils.enumerate_choices_for_user_input(chlist=self.good_months)

        first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=date)

        ## beginning month rent roll and vacancy
        tenants_at_1, vacants, tenants_at_2 = self.populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

        ## opcash 
        opcash = [row.dep_sum for row in query.ur_query(model_str='OpCash', query_tup= [('date', first_dt), ('date', last_dt)], operators_list=['>=', '<=']).namedtuples()]

        ## did opcash reconcile to deposits
        did_opcash_or_scrape_reconcile_with_deposit_report = [row for row in query.ur_query(model_str='StatusObject').namedtuples() if row.month == date]

        ## replacement reserve
        replacement_reserve = [row.rr for row in query.ur_query(model_str='Findexer', query_tup= [('period', date)], operators_list=['=='] ).namedtuples() if row.doc_type == 'scrape']

        if replacement_reserve == []:
            replacement_reserve = [row.rr for row in query.ur_query(model_str='Findexer', query_tup= [('period', date)], operators_list=['=='] ).namedtuples() if row.doc_type == 'opcash']

        #hap
        hap = [row.hap for row in query.ur_query(model_str='Findexer', query_tup= [('period', date)], operators_list=['=='] ).namedtuples() if row.doc_type == 'scrape']

        if hap == []:
            hap = [row.hap for row in query.ur_query(model_str='Findexer', query_tup= [('period', date)], operators_list=['=='] ).namedtuples() if row.doc_type == 'opcash']
        
        #damages
        if [row for row in query.ur_query(model_str='Damages', query_tup= [('dam_date', first_dt), ('dam_date', last_dt)], operators_list=['>=', '<='] ).namedtuples()] == []:
            damage_sum = 0
            dam_types = []
        else:
            damages = [row for row in query.ur_query(model_str='Damages', query_tup= [('dam_date', first_dt), ('dam_date', last_dt)], operators_list=['>=', '<='] ).namedtuples()]
            damage_sum = sum([float(row.dam_amount) for row in damages])
            dam_types = [row.dam_type for row in damages]

        #laundry, ntp, other
        if [row for row in query.ur_query(model_str='NTPayment', query_tup= [('date_posted', first_dt), ('date_posted', last_dt)], operators_list=['>=', '<='] ).namedtuples()] == []:
            laundry_sum = 0
            other_sum = 0
        else: 
            laundry = [row for row in query.ur_query(model_str='NTPayment', query_tup= [('date_posted', first_dt), ('date_posted', last_dt)], operators_list=['>=', '<='] ).namedtuples()]

            laundry_sum = sum([float(row.amount) for row in laundry if row.genus == 'laundry'])

            other_sum = sum([float(row.amount) for row in laundry if row.genus == 'other'])
        
        #MIs
        mi_payments = []
        if [row for row in query.ur_query(model_str='MoveIn', query_tup= [('mi_date', first_dt), ('mi_date', last_dt)], operators_list=['>=', '<='] ).namedtuples()] == []:
            mis = {'none': 'none'}
        else:
            mis = [{row.name: str(row.mi_date)} for row in query.ur_query(model_str='MoveIn', query_tup= [('mi_date', first_dt), ('mi_date', last_dt)], operators_list=['>=', '<='] ).namedtuples()]           

            for name, _ in [(k, v) for rec in mis for (k, v) in rec.items()]:                
                mi_tp = query.get_single_ten_pay_by_period(first_dt=first_dt, last_dt=last_dt, name=name)
                mi_payments.append(mi_tp)

        self.print_rows(
            date=date, 
            beg_tenants=tenants_at_1, 
            opcash=opcash, reconcile_status=did_opcash_or_scrape_reconcile_with_deposit_report, 
            replacement_reserve=replacement_reserve, 
            hap=hap, 
            mis=mis,
            mi_payments=mi_payments,
            damage_sum=damage_sum, 
            dam_types=dam_types, 
            laundry=laundry_sum,
            other=other_sum, 
            last_reconciled_month=self.most_recent_good_month
            )

        self.what_do_we_have(first_incomplete_month=first_incomplete_month)
        breakpoint()

        

        """WILL NEED TO PUT OPCASH IN TESTS; IT IS IN CANONICAL"""
        #TODO
        # mi rent
        # mi sd
        # adjustments        
        
        # what do we need to process next month?
        # do membership tests on what_do_we_have
            # do we have the docs ? yes, go
                # no, can we scrape
                # no? can we manually download


        # what are supposed to return if we have nothing?

        # fix move-outs
        # do you want to process next month?
            # iter build here
            # can we stop processing and drop prior to lengthy write?
        # run scrape from here?

    def print_rows(self, date=None, **kwargs):
        print(f'selected month: {date}\n')

        print(f'\t opcash and deposit sheet reconcile: {kwargs["reconcile_status"][0].tenant_reconciled}')
        print(f'\t scrape and deposit sheet reconcile: {kwargs["reconcile_status"][0].scrape_reconciled}')
        print(f'\t rent sheet produced: {kwargs["reconcile_status"][0].rs_reconciled}')
        print(f'\t rent sheet reconciled: {kwargs["reconcile_status"][0].rs_reconciled}')
        print(f'\t excel sheet reconciled: {kwargs["reconcile_status"][0].excel_reconciled}')
        print(f'\t balance letters produced: {kwargs["reconcile_status"][0].bal_letters}')
        print(f'occupieds at first of month: {len(kwargs["beg_tenants"])}')
        print('*' * 45)
        print(f'MI/MOS')
        for k, v in [(k, v) for x in kwargs["mis"] for (k, v) in x.items()]:
            print(f'\tname: {v}, date: {k} ')

        for rec in kwargs['mi_payments']:
            print(f'\tname: {rec[0]}, payments: {rec[1]}')
        print(f'\tno of move_ins: {len(kwargs["mis"])}')
        print('*' * 45)
        print(f'subcategories for {date}')
        print(f'\t replacement reserve: {kwargs["replacement_reserve"][0]}')
        print(f'\t hap: {kwargs["hap"][0]}')
        print(f'\t damages: {kwargs["damage_sum"]}')
        print(f'\t damage types: {kwargs["dam_types"]}')
        print(f'\t laundry: {kwargs["laundry"]}')
        print(f'\t other(ntp): {kwargs["other"]}')
        print('*' * 45)
        print(f'last reconciled month: {kwargs["last_reconciled_month"]}')

    def what_do_we_have(self, first_incomplete_month=None, **kwargs):
        
        scrape = Scrape()
        what_do_we_have_for_next_month = [row.doc_type for row in self.ur_query.ur_query(model_str='Findexer', query_tup= [('period', first_incomplete_month)], operators_list=['=='] ).namedtuples()]           

        is_first_pw_incomplete_month_over = Utils.is_target_month_over(target_month=first_incomplete_month)        

        if 'deposits' not in what_do_we_have_for_next_month:
            if is_first_pw_incomplete_month_over:
                print(f'{first_incomplete_month} is over; attempt to download deposit report this month')
                print('attempting to get deposit report from realpage')
                x = scrape.pw_context(path='an example path')
                # scrape.pw_context(genus="current_month_deposits", path=self.path)
                breakpoint()
        breakpoint()
            
        
        
        
        if 'opcash' not in what_do_we_have_for_next_month:
            if is_first_pw_incomplete_month_over:
                print(f'{first_incomplete_month} is over; attempt to download opcash')
                print('attempting to get opcash from nbofi')
        
        if 'rent' not in what_do_we_have_for_next_month:
            print('is month closed do you want to try to get rent roll.')
        else:
            print('three doc types are present: do you want to try to process month')

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