import os.path
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.service_account import ServiceAccountCredentials

from config import YTD_DEPOSIT_BOOK, RENT_SHEETS2022, READ_RANGE_HAP, READ_RANGE_PAY_PRE, R_RANGE_INTAKE_UNITS
from config import my_token, my_oauth_credentials_json, my_scopes, SCOPES_API, my_token_api

import gspread


def oauth(SCOPES, type):
    creds = None

    if os.path.exists(my_token):
        with open(my_token, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                my_oauth_credentials_json, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(my_token, 'wb') as token:
            pickle.dump(creds, token)

    if type == 'sheet':
        service = build('sheets', 'v4', credentials=creds)
    elif type == 'doc':
        service = build('docs', 'v1', credentials=creds)
    elif type == 'calendar':
        service = build('calendar', 'v3', credentials=creds)
    elif type == 'drive':
        service= build('drive', 'v3', credentials=creds)
    elif type == 'script':
        service = build('script', 'v1', credentials=creds)
    else:
        "service not found."

    print(f'\nOpening {type} {service} with oauth . . . ')
    print(f'with scopes: {SCOPES}\n')
    return service

def open_sheet(SCOPES_API, my_token_api, wb_string, ws_string):

    creds = ServiceAccountCredentials.from_json_keyfile_name(my_token_api, SCOPES_API)
    gc = gspread.authorize(creds)
    db = gc.open(wb_string)
    opened_db = db.worksheet(ws_string)
    print(f'\nOpening sheet {opened_db} with Service Api . . .')
    print(f'with scopes: {SCOPES_API}\n')

    return opened_db