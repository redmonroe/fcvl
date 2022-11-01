
class Reconciler:

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
    
    # def check_db_tp_and_ntp(self, grand_total=None, first_dt=None, last_dt=None):
    #     '''checks if there are any payments in the database for the month'''
    #     '''contains its own assertion; this is an important part of the process'''
    #     all_tp = [float(rec.amount) for rec in Payment.select().
    #             where(Payment.date_posted >= first_dt).
    #             where(Payment.date_posted <= last_dt)]
    #     all_ntp = [float(rec.amount) for rec in NTPayment.select().
    #             where(NTPayment.date_posted >= first_dt).
    #             where(NTPayment.date_posted <= last_dt)]
        
    #     assert sum(all_ntp) + sum(all_tp) == grand_total
    #     return all_tp, all_ntp