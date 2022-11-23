from backend import ProcessingLayer, PopulateTable, StatusObject
from config import Config
from utils import Utils


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

    def select_month(self, range=None):
        """could set explicit range if wanted"""
        date, _ = Utils.enumerate_choices_for_user_input(chlist=Utils.months_in_ytd(Config.current_year))
        first_dt, last_dt = self.populate.make_first_and_last_dates(date_str=date)

        ## beginning month rent roll and vacancy
        tenants_at_1, vacants, tenants_at_2 = self.populate.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dt)

        ## opcash 
        opcash = self.populate.get_opcash_sum_by_period(first_dt=first_dt, last_dt=last_dt)

        ## did opcash reconcile to deposits
        did_opcash_or_scrape_reconcile_with_deposit_report = self.populate.get_all_by_rows_by_argument(model1=StatusObject)

        
        
        
        
        breakpoint()



    
    def greetings(self, **kwargs):
        print(kwargs)
        breakpoint()

    def filter_rows(self, date=None):