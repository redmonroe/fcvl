import sys

from backend import PopulateTable, ProcessingLayer, StatusObject, UrQuery, DryRunRentRoll
from config import Config
from errors import Errors
from iter_rs import IterRS
from scrape import PWScrape
from utils import Utils


class WhereAreWe(ProcessingLayer):

    def __init__(self, **kwargs):
        self.build = kwargs['build']
        self.path = kwargs['path']
        self.full_sheet = kwargs['full_sheet']
        self.download_path = Config.SCRAPE_TESTING_SAVE_PATH
        self.db = self.build.main_db
        self.populate = PopulateTable()
        self.player = ProcessingLayer()
        self.query = UrQuery()
        self.testing = True
        self.times = 0
        self.iter = IterRS(full_sheet=self.full_sheet, path=self.path,
                           mode='testing', test_service=None, pytest=None)
        self.most_recent_good_month, self.good_months = self.player.get_mr_good_month()
        self.ur_query = UrQuery()
        self.support_doc_types_list = ['deposits', 'opcash', 'rent', 'scrape']
        self.first_incomplete_month = Utils.get_next_month(
            target_month=self.most_recent_good_month)

    def select_month(self, date=None):
        """could set explicit range if wanted"""
        # query = UrQuery()
        if date:
            date = self.most_recent_good_month
        else:
            date, _ = Utils.enumerate_choices_for_user_input(
                chlist=self.good_months)

        first_dt, last_dt = self.populate.make_first_and_last_dates(
            date_str=date)

        # beginning month rent roll and vacancy
        tenants_at_1, vacants, tenants_at_2 = self.populate.get_rent_roll_by_month_at_first_of_month(
            first_dt=first_dt, last_dt=last_dt)

        # opcash
        opcash = [row.dep_sum for row in self.query.ur_query(model_str='OpCash', query_tup=[(
            'date', first_dt), ('date', last_dt)], operators_list=['>=', '<=']).namedtuples()]

        # did opcash reconcile to deposits
        did_opcash_or_scrape_reconcile_with_deposit_report = [row for row in self.query.ur_query(
            model_str='StatusObject').namedtuples() if row.month == date]

        # replacement reserve
        replacement_reserve = [row.rr for row in self.query.ur_query(model_str='Findexer', query_tup=[(
            'period', date)], operators_list=['==']).namedtuples() if row.doc_type == 'scrape']

        if replacement_reserve == []:
            replacement_reserve = [row.rr for row in self.query.ur_query(model_str='Findexer', query_tup=[(
                'period', date)], operators_list=['==']).namedtuples() if row.doc_type == 'opcash']

        # hap
        hap = [row.hap for row in self.query.ur_query(model_str='Findexer', query_tup=[(
            'period', date)], operators_list=['==']).namedtuples() if row.doc_type == 'scrape']

        if hap == []:
            hap = [row.hap for row in self.query.ur_query(model_str='Findexer', query_tup=[(
                'period', date)], operators_list=['==']).namedtuples() if row.doc_type == 'opcash']

        # damages
        if [row for row in self.query.ur_query(model_str='Damages', query_tup=[('dam_date', first_dt), ('dam_date', last_dt)], operators_list=['>=', '<=']).namedtuples()] == []:
            damage_sum = 0
            dam_types = []
        else:
            damages = [row for row in self.query.ur_query(model_str='Damages', query_tup=[(
                'dam_date', first_dt), ('dam_date', last_dt)], operators_list=['>=', '<=']).namedtuples()]
            damage_sum = sum([float(row.dam_amount) for row in damages])
            dam_types = [row.dam_type for row in damages]

        #laundry, ntp, other
        if [row for row in self.query.ur_query(model_str='NTPayment', query_tup=[('date_posted', first_dt), ('date_posted', last_dt)], operators_list=['>=', '<=']).namedtuples()] == []:
            laundry_sum = 0
            other_sum = 0
        else:
            laundry = [row for row in self.query.ur_query(model_str='NTPayment', query_tup=[(
                'date_posted', first_dt), ('date_posted', last_dt)], operators_list=['>=', '<=']).namedtuples()]

            laundry_sum = sum([float(row.amount)
                              for row in laundry if row.genus == 'laundry'])

            other_sum = sum([float(row.amount)
                            for row in laundry if row.genus == 'other'])

        # MIs
        mi_payments = []
        if [row for row in self.query.ur_query(model_str='MoveIn', query_tup=[('mi_date', first_dt), ('mi_date', last_dt)], operators_list=['>=', '<=']).namedtuples()] == []:
            mis = {'none': 'none'}
        else:
            mis = [{row.name: str(row.mi_date)} for row in self.query.ur_query(model_str='MoveIn', query_tup=[
                ('mi_date', first_dt), ('mi_date', last_dt)], operators_list=['>=', '<=']).namedtuples()]

            for name, _ in [(k, v) for rec in mis for (k, v) in rec.items()]:
                mi_tp = self.query.get_single_ten_pay_by_period(
                    first_dt=first_dt, last_dt=last_dt, name=name)
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

        target_month, currently_availables, first_pw_incomplete_month = self.what_do_we_have(
            first_incomplete_month=self.first_incomplete_month, allow_print=False)

        count = 0
        print('*' * 45)
        print(
            f'\nfor target month: {target_month}, the following items are ready:')
        print('*' * 45)
        print(
            f'target month {target_month} is over? {first_pw_incomplete_month}.')
        print('*' * 45)
        for item in currently_availables:
            for genus, available in item.items():
                print(f'\t{genus}: {available[0]} |  path: {available[1]}')
                if available[0] == True:
                    count += 1
        print('*' * 45)
        print(f'ready to dry run? {target_month}.')
        print('*' * 45)

        if count == 3:
            print(self.path)
            dry_run_iter = self.iter.dry_run(
                currently_availables=currently_availables, target_month=target_month)
        else:
            print('you dont have enough files to do a dry run')
            breakpoint()
            sys.exit(0)
            
        report_deposits = dry_run_iter["deposits"]

        print('*' * 45)
        print(f'DRY RUN FOR {target_month}: rent roll')
        print('*' * 45)
        print(f'\tmove outs {target_month}: {dry_run_iter["rent"]["mos"]}')
        print(f'\tmove-ins {target_month}: {dry_run_iter["rent"]["mis"]}')
        print(
            f'\ttenant charges {target_month}: {dry_run_iter["rent"]["tenant_charges"]}')
        # still want beginning vacancy and ending vacancy actuals not just from subtractions and additions
        print('*' * 45)
        print(f'\tdeposits for {target_month}: {report_deposits}')
        print(
            f'\tdamage charges/credits for {target_month}: {dry_run_iter["damages"]} ')
        print(f'DRY RUN FOR {target_month}: opcash/scrape from ban')
        print('*' * 45)
        if first_pw_incomplete_month:
            print(f'deposits report for {target_month} via opcash.')
            bank_deposits = self.opcash_printer(target_month=target_month,
                                dry_run_iter=dry_run_iter)
            deposits_discrepancy = bank_deposits - round(float(report_deposits), 2)
        else:
            print(f'deposits report for {target_month} via scrape.')
            for item in dry_run_iter['scrape']['amount'].items():
                print(f'\t{item[0]}: {item[1]}')
                
        print('*' * 45)
        print(f'DEPOSITS DISCREPANCY = ${deposits_discrepancy}')
        print('negative number means bank shows higher amount than report')

        # TODO
        # what can we reconcile?
        # THEN LOOP BACK TO RECONCILIATION AND THEN IF ALL IS WELL WE CAN TRY TO RECONCILE

        # mi rent
        # mi sd
        # adjustments

    def opcash_printer(self, target_month=None, dry_run_iter=None):
        print('\n')
        print(f'opcash summary for {target_month}.')
        print('*' * 45)
        print(f'\tdeposits {target_month}: {dry_run_iter["opcash"]["dep"]}')
        print(f'\thap {target_month}: {dry_run_iter["opcash"]["hap"]}')
        print(f'\trr {target_month}: {dry_run_iter["opcash"]["rr"]}')
        print(
            f'\tdeposit corrections on opcash side: {dry_run_iter["opcash"]["corr_sum"]}')
        print('*' * 45)
        return dry_run_iter["opcash"]["dep"]

    def print_rows(self, date=None, **kwargs):
        print(f'selected month: {date}\n')

        print(
            f'\t opcash and deposit sheet reconcile: {kwargs["reconcile_status"][0].tenant_reconciled}')
        print(
            f'\t scrape and deposit sheet reconcile: {kwargs["reconcile_status"][0].scrape_reconciled}')
        print(
            f'\t rent sheet produced: {kwargs["reconcile_status"][0].rs_reconciled}')
        print(
            f'\t rent sheet reconciled: {kwargs["reconcile_status"][0].rs_reconciled}')
        print(
            f'\t excel sheet reconciled: {kwargs["reconcile_status"][0].excel_reconciled}')
        print(
            f'\t balance letters produced: {kwargs["reconcile_status"][0].bal_letters}')
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

    def user_input_loop(self):
        try:
            return int(input("Press 1 to continue or 2 to exit..."))
        except ValueError as e:
            print('invalid input')

    def user_input_outer_loop(self):
        ready = True
        if self.testing == True:
            user_input = 1
        else:
            user_input = self.user_input_loop()

        while ready:
            if user_input == 1:
                ready = False
            elif user_input == 2:
                break
                sys.exit(0)

    def make_file_name(self, genus=None, period=None):
        period = period.split('-')
        if genus == 'deposits':
            return [f'{genus}_{period[1]}_{period[0]}.xls', f'{genus}_{period[1]}_{period[0]}.xlsx']
        if genus == 'opcash':
            return [f'op_cash_{period[0]}_{period[1]}.pdf']
        if genus == 'rent':
            return [f'rent_roll_{period[1]}_{period[0]}.xls', f'rent_roll_{period[1]}_{period[0]}.xlsx']
        if genus == 'scrape':
            scrape_name = [
                f'CHECKING_1891_Transactions_{period[0]}-{period[1]}', '.csv']
            return scrape_name

    def scrape_type_runner(self, target=None, doc_type=None, filename=None, scrape_func=None, **kwargs):
        if doc_type == target:
            save_path = self.download_path / filename[0]
            result = scrape_func(path=save_path, times=self.times)
        else:
            if kwargs['allow_print']:
                print(f'scraping not implemented for {doc_type} currently.')
            result = 'playwright scraping error'
            is_available = {doc_type: (False, 'you went down wrong branch.')}

        if result == 'playwright scraping error':
            print('try to manually download {} for {} to {}'.format(
                doc_type, kwargs['first_incomplete_month'], self.path))
            self.user_input_outer_loop()
            try:
                is_available = self.iter.is_new_file_available(
                    genus=doc_type, filename=filename)
            except ValueError as e:
                print(e)
                is_available = {doc_type: (False, 'scrape is not available.')}

        try:
            return is_available
        except UnboundLocalError as e:
            breakpoint()

    def just_longer_message(self, doc_type, **kwargs):
        if kwargs['is_first_pw_incomplete_month_over']:  # look for opcash over scrape
            if kwargs['allow_print']:
                print(
                    f'{kwargs["first_incomplete_month"]} is over; attempt to download {doc_type} report this period.')
                print('trying realpage first...')
                print(
                    f'currently attempting to scrape {doc_type} for {self.times} attempts...')

    def check_for_presence(self, doc_type=None, **kwargs):
        # TODO how to handle previous versus current, #truncate save file date
        # TODO need less fragile way to scrape
        scrape = PWScrape()

        filename = self.make_file_name(
            genus=doc_type, period=kwargs["first_incomplete_month"])
        possible_file_locations = [self.path / fn for fn in filename]

        if doc_type in kwargs['what_do_we_have_for_next_month']:
            # check findex table, if that fails try scrape, then try manual
            is_available = {doc_type: (True, possible_file_locations)}
        else:
            if doc_type == 'deposits':
                is_available = self.scrape_type_runner(target='deposits', doc_type=doc_type, filename=filename, scrape_func=scrape.pw_deposits,
                                                       allow_print=kwargs['allow_print'], first_incomplete_month=kwargs["first_incomplete_month"])
                self.just_longer_message(doc_type, first_incomplete_month=kwargs["first_incomplete_month"], allow_print=kwargs[
                                         'allow_print'], is_first_pw_incomplete_month_over=kwargs['is_first_pw_incomplete_month_over'])

            if doc_type == 'rent':
                is_available = self.scrape_type_runner(target='rent', doc_type=doc_type, filename=filename, scrape_func=scrape.playwright_rentroll_scrape,
                                                       allow_print=kwargs['allow_print'], first_incomplete_month=kwargs["first_incomplete_month"])
                self.just_longer_message(doc_type, first_incomplete_month=kwargs["first_incomplete_month"], allow_print=kwargs[
                                         'allow_print'], is_first_pw_incomplete_month_over=kwargs['is_first_pw_incomplete_month_over'])

            if kwargs['is_first_pw_incomplete_month_over']:
                if doc_type == 'opcash':
                    is_available = self.scrape_type_runner(target='opcash', doc_type=doc_type, filename=filename, scrape_func=scrape.playwright_nbofi_opcash,
                                                           allow_print=kwargs['allow_print'], first_incomplete_month=kwargs["first_incomplete_month"])
                    self.just_longer_message(doc_type, first_incomplete_month=kwargs["first_incomplete_month"], allow_print=kwargs[
                                             'allow_print'], is_first_pw_incomplete_month_over=kwargs['is_first_pw_incomplete_month_over'])

            if doc_type == 'opcash' and kwargs['is_first_pw_incomplete_month_over'] == False:
                is_available = {doc_type: (
                    False, 'target month is not over; not opcash')}

            if doc_type == 'scrape':
                is_available = self.scrape_type_runner(target='scrape', doc_type=doc_type, filename=filename, scrape_func=scrape.playwright_nbofi_scrape,
                                                       allow_print=kwargs['allow_print'], first_incomplete_month=kwargs["first_incomplete_month"])
                self.just_longer_message(doc_type, first_incomplete_month=kwargs["first_incomplete_month"], allow_print=kwargs[
                                         'allow_print'], is_first_pw_incomplete_month_over=kwargs['is_first_pw_incomplete_month_over'])
        try:
            return is_available
        except UnboundLocalError as e:
            breakpoint()

    def what_do_we_have(self, first_incomplete_month=None, **kwargs):
        what_do_we_have_for_next_month = [row.doc_type for row in self.ur_query.ur_query(
            model_str='Findexer', query_tup=[('period', first_incomplete_month)], operators_list=['==']).namedtuples()]
        is_first_pw_incomplete_month_over = Utils.is_target_month_over(
            target_month=first_incomplete_month)

        currently_availables = [{}]
        for doc_type in self.support_doc_types_list:
            is_target_file_available = self.check_for_presence(doc_type=doc_type,
                                                               what_do_we_have_for_next_month=what_do_we_have_for_next_month, first_incomplete_month=first_incomplete_month,
                                                               is_first_pw_incomplete_month_over=is_first_pw_incomplete_month_over, allow_print=kwargs['allow_print'])
            currently_availables.append(is_target_file_available)

        return first_incomplete_month, currently_availables, is_first_pw_incomplete_month_over

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

        populate = PopulateTable()
        findex = FileIndexer(path=kw['path'], db=kw['db'])

        months_ytd, unfin_month = findex.test_for_unfinalized_months()

        status_objects = populate.get_all_status_objects()

        deposits = self.status_table_finder_helper(
            months_ytd, type1='deposits')

        dep_recon = populate.get_all_findexer_recon_status(type1='deposits')

        deposit_rec = [(row.recon) for row in Findexer.select()]
        rents = self.status_table_finder_helper(months_ytd, type1='rent')
        scrapes = self.status_table_finder_helper(months_ytd, type1='scrape')

        tp_list, ntp_list, total_list, opcash_amt_list, dc_list = self.make_mega_tup_list_for_table(
            months_ytd)

        dl_tup_list = list(zip(deposits, rents, scrapes, tp_list,
                           ntp_list, total_list, opcash_amt_list, dc_list, dep_recon))

        header = ['month', 'deps', 'rtroll', 'scrapes', 'ten_pay',
                  'ntp', 'tot_pay',  'oc_dep', 'dc', 'pay_rec_f?']

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

        print('\n'.join([''.join(['{:9}'.format(x)
              for x in r]) for r in table]))

    def status_table_finder_helper(self, months_ytd, type1=None):
        populate = PopulateTable()
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
            first_dt, last_dt = self.populate.make_first_and_last_dates(
                date_str=month)

            ten_payments = sum([float(row[2]) for row in self.populate.get_payments_by_tenant_by_period(
                first_dt=first_dt, last_dt=last_dt)])
            tp_tup = (ten_payments, first_dt)
            tp_list.append(tp_tup)

            ntp = sum(self.populate.get_ntp_by_period(
                first_dt=first_dt, last_dt=last_dt))
            ntp_tup = (ntp, first_dt)
            ntp_list.append(ntp_tup)

            total = float(ten_payments) + float(ntp)
            total_list.append((total, first_dt))

            opcash = self.populate.get_opcash_by_period(
                first_dt=first_dt, last_dt=last_dt)
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
                scrapes = self.populate.get_scrape_detail_by_month_by_type(
                    type1='corr', first_dt=first_dt, last_dt=last_dt)

                dc_tup = (sum([float(n) for n in scrapes]), first_dt)
                print('current branch')
                dc_list.append(dc_tup)

        return tp_list, ntp_list, total_list, opcash_amt_list, dc_list
