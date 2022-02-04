
class GoogleApiCalls:

    def simple_batch_update(self, service, sheet_id, wrange, data, dim):
        print(f"Updating {sheet_id} with batch call to {wrange}...")
        body_request = {
                        'value_input_option': 'RAW',
                        'data': [
                                {'range': wrange,
                                'majorDimension': dim, #'ROW' or 'COLUMN'
                                'values': [data]
                                }
                        ],
                        }

        request = service.spreadsheets().values().batchUpdate(spreadsheetId=sheet_id, body=body_request)
        response = request.execute()
        print(response)

    def batch_get(self, service, sheet_id, range, col_num): 
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=range, majorDimension="ROWS").execute()

        values = result.get('values', [])
        col = []
        if not values:
            print('No data found.')
        else:
            for COLUMN in values:
                col.append(COLUMN[col_num])
        return col

    def update(self, service, sheet_choice, data, write_range, value_input_option='RAW'):
        sheet = service.spreadsheets()
    
        spreadsheet_id = sheet_choice

        range_ = write_range  # TODO: Update placeholder value.

        value_input_option = 'RAW'  #

        value_range_body = {"range": write_range,
                            "majorDimension": "COLUMNS",
                            "values": [data]
        }

        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()


    def write_formula_column(self, service, sheet_choice, data, write_range):
        print(f"Writing {data} to {write_range}  . . .")
        sheet = service.spreadsheets()
        spreadsheet_id = sheet_choice
        range_ = write_range  # TODO: Update placeholder value.
        # How the input data should be interpreted.
        value_input_option = 'USER_ENTERED'
        value_range_body = {
                            "values": [data]
        }

        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()

    def format_row(self, service, sheet_choice, write_range, r_or_c, name_list): # and writing strings to ranges passed
        print(f"Formatting {r_or_c} . . .")
        sheet = service.spreadsheets()
        range_ = write_range  # TODO: Update placeholder value.

        value_input_option = 'USER_ENTERED'  #

        value_range_body = {"range": write_range,
                            "majorDimension": r_or_c,
                            "values": [name_list]
        }

        request = service.spreadsheets().values().update(spreadsheetId=sheet_choice, range=range_, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()

    def clear_sheet(self, service, sheet_choice, clear_range):
        service = service
        spreadsheet_id = sheet_choice

        batch_clear_values_request_body = {
                                        'ranges': [clear_range],

                                        }
        request = service.spreadsheets().values().batchClear(spreadsheetId=spreadsheet_id, body=batch_clear_values_request_body)
        response = request.execute()

    def broad_get(self, service, spreadsheet_id, range):
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=range).execute()
        values = []
        values_ = result.get('values', [])
        for value in values_:
            values.append(value)
        return values
    
    def make_one_sheet(self, service, spreadsheet_id, sheet_title):
        sh_id = spreadsheet_id

        data = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_title,
                        'tabColor': {
                            'red': 0.44,
                            'green': 0.99,
                            'blue': 0.50
                        }
                    }
                }
            }]
        }

        response = service.spreadsheets().batchUpdate(
            spreadsheetId=sh_id,
            body=data
        ).execute()

    def del_one_sheet(self, service, spreadsheet_id, id):
        f"Deleting sheet {id} . . . "
        sh_id = spreadsheet_id

        data = {"requests": [
                {"deleteSheet": {"sheetId": f'{id}'}
                } ]  }
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=sh_id,
            body=data
        ).execute()
        print(response)

