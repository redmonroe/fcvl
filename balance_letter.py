from backend import StatusRS, db
from datetime import datetime
from config import Config
from auth_work import oauth

template = Config.BALANCE_LETTER_TEMPLATE
output_file = Config.BALANCE_LETTER_OUTPUT

def get_parameters(balance_list=None):

    # for record in balance_list:

        # parameters = {
        # "current_date" : formatted_date, 
        # "display_month": display_month,
        # "sheet_choice": sheet_choice[1][0]
        # }

    formatted_date = datetime.utcnow()
    current_date = datetime.strftime(formatted_date, '%Y-%m-%d')
    parameters = {
        "current_date" : current_date, #_
        "display_month": formatted_date.strftime('%B'),
        "display_year": str(formatted_date.year),
        # "unit": unit, 
        # "name": name, 
        # "bal_due": end_bal, 
        }
    # breakpoint()
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
    deploy_id = 'AKfycbyWdwyvtCZLzYyaxOf_LREQh-OLDpwcTYwg51qCLB86GQl3IWe0iZ3-Mj-1a6KYgs14'
    # Config.BALANCE_LETTER_DEPLOY_ID
    function_name = 'balanceLetter'
    balance_list = status.show_balance_letter_list_mr_reconciled()
    parameters = get_parameters(balance_list=balance_list)


    # print(parameters)
    # choice = str(input("send these results to google script & make receipts? y/n"))

    # if choice == 'y':
    breakpoint()
    run_script(service=service, deploy_id=deploy_id, function_name=function_name, parameters=parameters) 

    '''add letter generated col to db'''

