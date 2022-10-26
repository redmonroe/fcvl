from datetime import datetime
from pprint import pprint

from docx import Document

from auth_work import oauth
from config import Config
from utils import Utils


class Letters():

    def __init__(self, db):
        self.main_db = db

    def setup_tables(self, mode=None):
        from backend import PopulateTable
        populate = PopulateTable()
        self.create_tables_list1 = populate.return_tables_list()
        if self.main_db.is_closed() == True:
            self.main_db.connect()
        if mode == 'create_only':
            self.main_db.create_tables(self.create_tables_list1)
        elif mode == 'drop_and_create':
            self.main_db.drop_tables(models=self.create_tables_list1)
            self.main_db.create_tables(self.create_tables_list1)
        return populate

    def fix_name(self, unit, address_boilerplate=None):
        unit_no = unit[0]
        name = unit[1].split(',')
        name = [n.rstrip().lstrip().capitalize() for n in name]
        name = (' ').join(name[::-1])
        address = address_boilerplate
        address.insert(1, unit_no)
        address.insert(0, name)
        return address
    
    def get_addresses(self):
        from pprint import pprint

        from backend import QueryHC, Unit
        self.setup_tables(mode='create_only')
        units = [(row.unit_name, row.tenant, row.last_occupied) for row in Unit.select().namedtuples()]

        if units == []:
            print('Units table is empty')
            exit
        else:

            # fix names
            # try except error if Units table is empty

            # pprint([unit for unit in units])
            addresses = []
            for unit in units:
                if unit[0].split('-')[0] == 'CD':
                    addresses.append(self.fix_name(unit, address_boilerplate=Config.ADDRESS_CD))
                elif unit[0].split('-')[0] == 'PT':
                    addresses.append(self.fix_name(unit, address_boilerplate=Config.ADDRESS_PT))
        
            breakpoint()

    def get_doc_title(self, doc, service_docs): #doc is DOCS_FILE_ID
        document = service_docs.documents().get(documentId=doc).execute()

        print('The title of the document is: {}'.format(document.get('title')))

    def create_folder(self, service_drive, folder_name):
        file_metadata = {
        'name': f'{folder_name}',
        'mimeType': 'application/vnd.google-apps.folder'
                        }
        file = service_drive.files().create(body=file_metadata,
                                        fields='id').execute()
        print('Folder ID: %s' % file.get('id'))

    def create_blank_doc(self, service_docs, service_drive, title): 
        # from docs, there is also a method from drive
        title = f'{title}'
        body = {
            'title': title
        }
        doc = service_docs.documents() \
            .create(body=body).execute()
        print('Created document with title: {0}'.format(
            doc.get('title')))
        print('Created document with id: {0}'.format(
            doc.get('id')))

    def insert_text(self, service_docs, doc_id, text1, text2, text3):
        requests = [
            {
                'insertText': {
                    'location': {
                        'index': 1,
                    },
                    'text': text1
                }
            },
                    {
                'insertText': {
                    'location': {
                        'index': 2,
                    },
                    'text': text2
                }
            }
                ]

        result = service_docs.documents().batchUpdate(
            documentId=doc_id, body={'requests': requests}).execute()

    @staticmethod
    def run_script(service, deploy_id, function_name, parameters=None):
        request = {
        'function': function_name, 
        'devMode': True, 
        'parameters': [parameters]
        }

        response = service.scripts().run(
                        body=request,
                        scriptId=deploy_id
                        ).execute()
        pprint(response)

    def get_bal_let_parameters(self, balance_list=None):
        from backend import ProcessingLayer

        player = ProcessingLayer()
        balance_letters = player.show_balance_letter_list_mr_reconciled()
        name_list = []
        unit_list = []
        bal_due_list = []

        for record in balance_letters:
            formatted_name = [name.rstrip().lstrip().capitalize() for name in record.tenant_name.split(',')]
            name_list.append((' ').join(formatted_name[::-1]))
            unit_list.append(record.unit)
            bal_due_list.append(str(record.end_bal))

        formatted_date = datetime.utcnow()
        parameters = {
            'current_date' : datetime.strftime(datetime.utcnow(), '%Y-%m-%d'), 
            'display_month': formatted_date.strftime('%B'),
            'display_year': str(formatted_date.year),
            'unit': unit_list, 
            'name': name_list, 
            'bal_due': bal_due_list, 
            }

        return parameters

    def work_orders(self):
        service_scripts = oauth(Config.my_scopes, 'script')
        service = oauth(Config.my_scopes, 'sheet')
        deploy_id = 'AKfycbw5Sdkkqq6f34el5uz_wRGSWdORN8BNyzay2HYLyh6JIW1hwcGTn06zYVR5RMuUiFr_JA'
        Letters.run_script(service=service_scripts, deploy_id=deploy_id, function_name='myFunction', 
        # parameters=parameters
        ) 

    def bal_let_pprint_parameters(self, parameters):
        print('current date', parameters['current_date'], parameters['display_year'])
        print('display_month', parameters['display_month'])

        bal_data = list(zip(parameters['unit'], parameters['name'], parameters['bal_due']))
        print('\n')
        for item in bal_data:
            row = '{:<9s} {:<20} {:<9s}'.format(item[0], item[1], item[2])
            print(row)
        print('\n')

    def balance_letters(self):
        from backend import ProcessingLayer, db
        '''if testing, I can simulate a prod  database by breakpointing db before teardown'''
        '''if there is an issue, check deployment id'''
        db.connect()
        player = ProcessingLayer()
        parameters = self.get_bal_let_parameters(balance_list=player.show_balance_letter_list_mr_reconciled())
        self.bal_let_pprint_parameters(parameters)
        choice = str(input('Send these results to google script & make balance letters? y/n '))
        if choice == 'y':
            Letters.run_script(service=oauth(Config.my_scopes, 'script'), deploy_id=Config.BALANCE_LETTER_DEPLOY_ID, function_name='balanceLetter', parameters=parameters)
        else:
            print('exiting program')
            exit

    def get_prev_daterange_by_month(self, year, month, prev):
        import calendar
        from datetime import date
        if year is None:
            today = datetime.date.today()
            year = today.year
        if month is None:
            today = datetime.date.today()
            month = today.month
        since = []
        till = []
        for i in range(prev):
            if month == 0:
                year -= 1
                month = 12
            _, num_days = calendar.monthrange(year, month)
            since.append(datetime.strptime(str(date(year, month, 1)), "%Y-%m-%d"))
            till.append(datetime.strptime(str(date(year, month, num_days)), "%Y-%m-%d"))
            month -= 1
        return since, till

    def rent_receipts_plus_balance(self):
        from backend import QueryHC
        print('rent receipts plus balance table')
        query = QueryHC()

        target_date = '2022-07'
        first_dt, last_dt = query.make_first_and_last_dates(date_str=target_date)

        rent_roll = query.get_rent_roll_by_month_at_first_of_month(first_dt=first_dt, last_dt=last_dts)

        '''consider dataframe for merging'''

        breakpoint()






        lookback_months = 6
        today = datetime.today()
        first_date_of_months, last_date_of_months = self.get_prev_daterange_by_month(2022, 7, lookback_months)
        dates = list(zip(first_date_of_months, last_date_of_months))

        
        earliest_date_in_range = first_date_of_months[-1]
        last_date_in_range = last_date_of_months[0]

        '''should do some sort of group by operation, get last date of most recent months & first date of earliest month: should get all payments'''

        total_payments = query.get_payments_by_tenant_by_period(first_dt=earliest_date_in_range, last_dt=last_date_in_range, cumsum=True)

        all_payments = query.        get_payments_by_tenant_by_period(first_dt=earliest_date_in_range, last_dt=last_date_in_range)

        for count, tup in enumerate(all_payments, 1):
            print(count, tup[0])

        breakpoint()
        # set target date

        # get beginning balance for all current tenants
        # get total payments by tenant by period
        # get total charges by tenant by period



        '''
        titles_dict = Utils.get_existing_sheets(oauth(Config.my_scopes, 'sheet'), Config.TEST_RS)
        idx_list = Utils.existing_ids({name:id2 for name, id2 in titles_dict.items() if name != 'intake'})
        sheet_choice = idx_list[int(input('Please select a sheet to make receipts from: '))]
        parameters = {
        'current_date' : datetime.strftime(datetime.utcnow(), '%Y-%m-%d'), 
        'display_month': str(input('Type display month as you wish it to appear? ')),
        'sheet_choice': sheet_choice[1][0], 
        'rent_sheet': Config.TEST_RS, 
        }

        pprint(parameters)
        choice = str(input('Send these results to google script & make receipts? y/n '))
        if choice == 'y':
            Letters.run_script(service=oauth(Config.my_scopes, 'script'), deploy_id=Config.receipts_table_test_deploy_id, function_name="test1", parameters=parameters) 
        else:
            print('exiting program')
            exit
        '''

    def rent_receipts(self):
        '''if there is an issue, check deployment id'''            
        titles_dict = Utils.get_existing_sheets(oauth(Config.my_scopes, 'sheet'), Config.TEST_RS)
        idx_list = Utils.existing_ids({name:id2 for name, id2 in titles_dict.items() if name != 'intake'})
        sheet_choice = idx_list[int(input('Please select a sheet to make receipts from: '))]
        parameters = {
        'current_date' : datetime.strftime(datetime.utcnow(), '%Y-%m-%d'), 
        'display_month': str(input('Type display month as you wish it to appear? ')),
        'sheet_choice': sheet_choice[1][0], 
        'rent_sheet': Config.TEST_RS, 
        }

        pprint(parameters)
        choice = str(input('Send these results to google script & make receipts? y/n '))
        if choice == 'y':
            Letters.run_script(service=oauth(Config.my_scopes, 'script'), deploy_id=Config.receipts_deploy_id, function_name="test1", parameters=parameters) 
        else:
            print('exiting program')
            exit
    
class DocxWriter(Letters):

    def sample_func(self):
        print('hi from docx')



