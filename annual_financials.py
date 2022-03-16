    # this needs to be moved to own file, but do it with some forethought for chrissakes!

    # do I want to do something with "TOTal" part of p and l?  I have it busted out into its own dict
    # do I want to do something with "mtd" part of p and L?  I have it also busted out into its own dict
class AnnFin:   
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
 
    def pick_bank_statements(choice=None, list_of_statements=None):

        for item in bank_statements_ytd:
            item2 = item.split('.')
            item2 = item2[0] 
            item2 = item2.split(' ')
            join_item = ' '.join(item2[2:4])
            if choice == join_item:
                stmt_list.append(item) 
                # print(item)
        return stmt_list

    def extraction_wrapper_for_transaction_detail(choice, func=None, path=None, keyword=None):

        path, files = path_to_statements(path=path, keyword=keyword)    
        #date_dict_groupby_m = qb_extract_security_deposit(files[0], path=path)
        date_dict_groupby_m = func(files[0], path=path)
        result = {amount for (dateq, amount) in date_dict_groupby_m.items() if dateq == choice}
        is_empty_set = (len(result) == 0)
        if is_empty_set:
            data = [0]
            return data
        else:
            data = [min(result)]
            return data

    LAUNDRY_RANGE_FROM_RS = '!N71:N71'
    RR_RANGE_FROM_RS = '!D80:D80'
    SEC_DEP_RANGE_FROM_RS = '!N73:N73'
    CURRENT_YEAR_RS = Config.RS_2022
    month_match_dict = {
        'jan': 1, 
        'feb': 2, 
        'mar': 3, 
        'apr': 4, 
        'may': 5, 
        'june': 6, 
        'july': 7, 
        'aug': 8, 
        'sep': 9, 
        'oct': 10, 
        'nov': 11, 
        'dec': 12, 
        }

    # choice = str(input('enter target month (mm/yyyy): '))
    service = oauth(my_scopes, 'sheet')
    sheet_id = Config.rec_act_2021
    worksheet_name = Config.TEST_REC_ACT
    hap_range = Config.current_year_hap
    laundry_range = Config.current_year_laundry
    sec_dep_range = Config.current_year_sec_dep
    rr_range = Config.current_year_rr

    dim = 'COLUMNS'

    choice = '01 2022' #need to reup December qbo, right now still showing 1-29 of december
    print('you picked:', choice)
    year_choice = choice.split(' ')
    month_choice = year_choice[0]
    year_choice = year_choice[1]
    # pick reports here
    if year_choice == '2022':
        bank_stmts = Config.path_qbo_test_reports
        p_and_l = Config.path_qbo_test_reports
        path_security_deposit = Config.path_qbo_test_reports
    
    three_letter_month = [str(month_str) for month_str, month_int in month_match_dict.items() if int(month_choice) == month_int]

    titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
    target_sheet = {sheet_name for (sheet_name, sheet_id) in titles_dict.items() if three_letter_month[0] in sheet_name}
    target_sheet = min(target_sheet)

    sh_col = Liltilities.get_letter_by_choice(int(month_choice), 0)
    hap_wrange = f'{worksheet_name}!{sh_col}{hap_range}:{sh_col}{hap_range}'
    laundry_wrange =f'{worksheet_name}!{sh_col}{laundry_range}:{sh_col}{laundry_range}' 
    sec_dep_wrange =f'{worksheet_name}!{sh_col}{sec_dep_range}:{sh_col}{sec_dep_range}' 
    rr_wrange = f'{worksheet_name}!{sh_col}{rr_range}:{sh_col}{rr_range}' 

    stmt_list = []
    target_bank_stmt_path, bank_statements_ytd = path_to_statements(path=bank_stmts, keyword='op cash')
    target_report = pick_bank_statements(choice=choice, list_of_statements=bank_statements_ytd)
    dateq, hap_stmt = nbofi_pdf_extract_hap(target_report[0], path=target_bank_stmt_path)
    
    target_pl_path, profit_and_loss_ytd = path_to_statements(path=p_and_l, keyword='Profit')
    hap_date_dict = qb_extract_p_and_l(profit_and_loss_ytd[0], keyword='5121', path=target_pl_path)
    for dateh, amount in hap_date_dict.items():
        if dateh == choice:
            hap_qbo = amount

    if hap_stmt == hap_qbo:
        data = [hap_stmt]
        simple_batch_update(service, sheet_id, hap_wrange, data, dim)
    else:
        print('hap does not balance between rs and qbo.')
        print('hap from bank', hap_stmt, '|', type(hap_stmt), 'rr from qb=', hap_qbo, '|', type(hap_qbo))
        simple_batch_update(service, sheet_id, hap_wrange, [100000000], dim)  

    # get laundry_income_rs
    laundry_income_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + LAUNDRY_RANGE_FROM_RS)
    laundry_income_rs = float(laundry_income_rs[0][0])

    laundry_date_dict = qb_extract_p_and_l(profit_and_loss_ytd[0], keyword='5910', path=target_pl_path)
    for dateq, amount in laundry_date_dict.items():
        if dateq == choice:
            laundry_income_qbo = float(amount)

    if laundry_income_rs == laundry_income_qbo:
        simple_batch_update(service, sheet_id, laundry_wrange, [laundry_income_rs], dim)
    else:
        print('laundry does not balance between rs and qb')
        print('laundr rs=', laundry_income_rs, '|', type(laundry_income_rs, 'laundry qb=', laundry_income_qbo, '|', type(laundry_income_qbo)))

     # sec dep
    sec_dep_qb = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_security_deposit, path=path_security_deposit, keyword='Security')
    sec_dep_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + SEC_DEP_RANGE_FROM_RS)

    if float(sec_dep_rs[0][0]) == float(sec_dep_qb[0]):
        simple_batch_update(service, sheet_id, sec_dep_wrange, sec_dep_qb, dim)
    else:
        print('sec_dp does not balance between rs and qb.  Have I adjusted on rs.')
        print('sd rs=', sec_dep_rs, '|', type(sec_dep_rs), 'sd qb=', sec_dep_qb, '|', type(sec_dep_qb))

    ## rr from qbo
    rr_qbo = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_security_deposit, path=Config.rr_2021, keyword='rr')
    ## rr from rent_sheets (see above)
    rr_rs = broad_get(service, CURRENT_YEAR_RS, target_sheet + RR_RANGE_FROM_RS)
    rr_rs = float(rr_rs[0][0])

    if rr_rs == rr_qbo:
        simple_batch_update(service, sheet_id, rr_wrange, rr_rs, dim)
    else:
        print('rr does not balance between rs and qbo.')
        print('rr from rs=', rr_rs, '|', type(rr_rs), 'rr from qb=', rr_qbo, '|', type(rr_qbo))
        print('WRITING PLUG PENDING JANUARY STATEMENTS AND ABILITY TO WORK ON LIVE DATA')
        simple_batch_update(service, sheet_id, rr_wrange, [100000000], dim)
    # deposit detail from qbo: need a group by swing here

    data = extraction_wrapper_for_transaction_detail(choice, func=qb_extract_deposit_detail, path=Config.deposit_detail_2021, keyword='deposit')
    print(data)

    print('joe')