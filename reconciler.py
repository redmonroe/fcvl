from utils import Utils


class Reconciler:

    @staticmethod
    def master_sum_from_payments_totaler(ten_payments=None, non_ten_pay=None, delete_mentries=None, period=None):
        breakpoint()
        try:
            if delete_mentries:
                sum_from_payments = ten_payments + non_ten_pay - delete_mentries
                print(f'Reconciling prior to StatusObject write for period {period}: tenant payments: {ten_payments} nontenant payments: {non_ten_pay} less deleted manual entries: {delete_mentries} = {sum_from_payments}.')
              
            else:
                sum_from_payments = ten_payments + non_ten_pay
                print(f'Reconciling prior to StatusObject write for period {period}: tenant payments: {ten_payments} nontenant payments: {non_ten_pay} = {sum_from_payments}.')
            return sum_from_payments
        except TypeError as e:
            print(e)
            breakpoint()
            raise

    @staticmethod
    def iter_build_assert_scrape_total_match_deposits(scrape_deposit_sum=None, grand_total_from_deposits=None, genus=None, period=None):
        try: 
            assert scrape_deposit_sum == grand_total_from_deposits
            print(f'Final check for {genus} if payment reports == scrape for {period}:')
            print(f'{scrape_deposit_sum} == {grand_total_from_deposits}.\n')
            return True
        except AssertionError as e:
            print(f'\nAssertionError in MonthSheet {genus} deposits do not match payments report for period {period}.')
            print(f'{genus}:{scrape_deposit_sum} does not equal payment report:{grand_total_from_deposits}.\n')
            raise    

    @staticmethod
    def month_sheet_final_check(onesite_total=None, nbofi_total=None, period=None, genus=None):
        try: 
            assert onesite_total == nbofi_total 
            print(f'Final check for {genus} if payment reports == bank deposits {period}:')
            print(f'{onesite_total[0][0]} == {nbofi_total[0][0]}.\n')
            return True
        except AssertionError as e:
            print(f'\nAssertionError in MonthSheet {genus} deposits do not match payments report for period {period}.')
            print(f'{genus}:{onesite_total[0][0]} does not equal payment report:{nbofi_total[0][0]}.\n')
            raise    

    @staticmethod
    def backend_processing_layer_assert_bank_deposits_tenant_deposits(bank_deposits=None, sum_from_payments_report=None, period=None, genus=None):
        # breakpoint()
        try: 
            assert bank_deposits == sum_from_payments_report 
            print(f'Reconciling {genus} deposits to payments report for {period}:')
            print(f'{bank_deposits} == {sum_from_payments_report}.\n')
            return True
        except AssertionError as e:
            print(f'\nAssertionError in backend processing_layer {genus} deposits do not match payments report for period {period}.')
            print(f'{genus}:{bank_deposits} does not equal payment report:{sum_from_payments_report}.\n')
            raise    

    @staticmethod
    def findexer_assert_stmt_dates_match(stmt1_date=None, stmt2_date=None):
        try: 
            assert stmt1_date == stmt2_date
            print(f'\nReconciling opcash statement dates for {stmt1_date} in file_indexer.py\n')
        except AssertionError as e:
            print(f'\nAssertionError found in reconciling opcash statement dates in file_indexer.py for {stmt1}')
            raise    

    @staticmethod
    def findexer_assert_scrape_catches_all_target_txns(period=None, sum_of_parts=None, check=None):
        try:
            assert float(Utils.decimalconv(str(sum_of_parts))) ==  round(check, 2)
            print(f'\nReconciling scrape sums for {period}, confirming {sum_of_parts} equals {check}.\n')
        except AssertionError as e:
            print(f'\nAssertionError found in reconciling scrape in file_indexer.py for {period}')
            print(f'{sum_of_parts} does not equal {check}\n')            
            raise    
    
    def findex_reconcile_onesite_deposits_to_scrape_or_oc(self):
        """this func exists to perform a relatively complicated reconciliation between deposits.xls from onesite and the scrape excel sheet and/or the monthly opcash

        we need to determine whether it makes more sense to change scrape payment summary (should find 'amount' col) or to add back the amount if scrape & opcash amounts agree

        """
        deposits_xls = self.findex_reconcile_helper(typ='deposits')
        scrapes = self.findex_reconcile_helper(typ='scrape')
        opcashes = self.findex_reconcile_helper(typ='opcash')

        scrape_match = self.findex_iteration_helper(list1=deposits_xls, list2=scrapes, target_str='0', fill_str='empty')
        opcash_match = self.findex_iteration_helper(list1=deposits_xls, list2=opcashes, target_str='0', fill_str='empty')

        # find failed deposits reports from onesite; these give scrape nothing to reconcile against and should fail
        dep_xls_prob = [item for item in scrape_match if item[1] == 'empty']
        
        # remove failed deposits reports   
        opcash_match = [item for item in opcash_match if item[1] != 'empty']
        scrape_match = [item for item in scrape_match if item[1] != 'empty']
                
        match_both = list(set(scrape_match).intersection(set(opcash_match)))
        scrape_only = list(set(scrape_match).difference(opcash_match))
        opcash_only = list(set(opcash_match).difference(scrape_match))

        self.findex_reconcile_helper_writer(list1=scrape_only, recon_str='scrape_only')
        self.findex_reconcile_helper_writer(list1=opcash_only, recon_str='opcash_only')
        self.findex_reconcile_helper_writer(list1=dep_xls_prob, recon_str='dep_prob')
        self.findex_reconcile_helper_writer(list1=match_both, recon_str='FULL')

    def findex_reconcile_helper_writer(self, list1=None, recon_str=None):
        from backend import Findexer
        for item in list1:
            deposit_id = [(row.doc_id, row.period) for row in Findexer.select().
                where(Findexer.doc_type == 'deposits').
                where(Findexer.period == item[0]).namedtuples()][0]
            deposit = Findexer.get(deposit_id[0])
            deposit.recon = recon_str
            deposit.save()        
        
    def findex_reconcile_helper(self, typ=None):
        from backend import Findexer
        return [(row.period, row.depsum) for row in Findexer.select().
            where(Findexer.doc_type == typ).namedtuples()]
    
    def findex_iteration_helper(self, list1=None, list2=None, target_str=None, fill_str=None):
        return_list = []
        for item in list1:
            for it in list2:
                if item[0] == it[0]:
                    if item[1] == target_str:
                        return_list.append((item[0], fill_str))
                    else:
                        return_list.append((item[0], item[1]))
        return return_list
    