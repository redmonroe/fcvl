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
    
    def greetings(self, **kwargs):
        print(kwargs)

    def filter_rows(self, **kwargs):
        print(kwargs)



    def print_rows(self, date=None, **kwargs):
        print(f'current month: {date}')
        print(f'reconciliation status: {kwargs["reconcile_status"]}')
        print(f'occupieds at first of month: {len(kwargs["beg_tenants"])}')
        breakpoint()