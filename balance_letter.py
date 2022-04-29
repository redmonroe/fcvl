from backend import StatusRS, db
from datetime import datetime
from config import Config
from auth_work import oauth

template = Config.BALANCE_LETTER_TEMPLATE
output_file = Config.BALANCE_LETTER_OUTPUT

def get_parameters(balance_list=None):

    status = StatusRS()
    balance_letters = status.show_balance_letter_list_mr_reconciled()
    name_list = []
    unit_list = []
    bal_due_list = []

    for record in balance_letters:
        formatted_name = [name.rstrip().lstrip().capitalize() for name in record.tenant_name.split(',')]
        name_list.append((' ').join(formatted_name[::-1]))
        unit_list.append(record.unit)
        bal_due_list.append(str(record.end_bal))
        breakpoint()

    formatted_date = datetime.utcnow()
    current_date = datetime.strftime(formatted_date, '%Y-%m-%d')
    parameters = {
        "current_date" : current_date, 
        "display_month": formatted_date.strftime('%B'),
        "display_year": str(formatted_date.year),
        "unit": unit_list, 
        "name": name_list, 
        "bal_due": bal_due_list, 
        }

    return parameters

def run_script(service, deploy_id, function_name, parameters):

    request = {
    "function": function_name, 
    "devMode": True, 
    "parameters": [parameters]
    }

    print(request)
    response = service.scripts().run(
                    body=request,
                    scriptId=deploy_id
                    ).execute()
    print(request, 'response:', response)

def balance_letters():
    '''if testing, I can simulate a prod  database by breakpointing db before teardown'''
    db.connect()
    status = StatusRS()
    service = oauth(Config.my_scopes, 'script')
    deploy_id = 'AKfycbyyIjoqLq-VGYEv-gVexvQXp6F0Z-yz1H7rn1X71TZC2VTHI0-HqLcJh-AwRy7KJmxA'
    deploy_id = Config.BALANCE_LETTER_DEPLOY_ID
    function_name = 'balanceLetter'
    balance_list = status.show_balance_letter_list_mr_reconciled()
    parameters = get_parameters(balance_list=balance_list)

    print(parameters)
    choice = str(input("send these results to google script & make receipts? y/n"))

    if choice == 'y':
        run_script(service=service, deploy_id=deploy_id, function_name=function_name, parameters=parameters) 


