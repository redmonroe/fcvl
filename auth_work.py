import os.path
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from config import (R_RANGE_INTAKE_UNITS, READ_RANGE_HAP, READ_RANGE_PAY_PRE,
                    RENT_SHEETS2022, SCOPES_API, Config,
                    my_oauth_credentials_json, my_scopes, my_token,
                    my_token_api)


def oauth(SCOPES, type, mode=None):
    creds = None

    if mode == 'testing':
        ## dealing with pathing issues from running tests from subdir; tricky business
        if os.path.exists(Config.testing_my_token):
            with open(Config.testing_my_token, 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    Config.testing_my_oauth_credentials_json, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(Config.testing_my_token, 'wb') as token:
                pickle.dump(creds, token)

    else:
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
    return service

