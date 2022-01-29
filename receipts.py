# sample project: https://github.com/gsuitedevs/python-samples/tree/master/docs/mail-merge
from auth_work import oauth
from config import my_scopes, Config   


class RentReceipts(object):

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

    def create_blank_doc(self, service_docs, service_drive, title): # from docs, there is also a method from drive
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
    def run_script(service, deploy_id, function_name, parameters):

        request = {
        "function": function_name, 
        "devMode": True, 
        "parameters": [parameters]
        }

        response = service.scripts().run(
                        body=request,
                        scriptId=deploy_id
                        ).execute()
        print(request, 'response:', response)

    @staticmethod 
    def rent_receipts():
        from fcvfin import FileHandler, BookFormat, UI
        from liltilities import get_existing_sheets
        from config import CURRENT_YEAR_RS
        from datetime import datetime
        
        fh = FileHandler()
        bf = BookFormat()
        ui = UI()
        service_scripts = oauth(my_scopes, 'script')
        service = oauth(my_scopes, 'sheet')
    
        deploy_id = "AKfycbyAEXFweZewXCkAYqg0DdsYFZmEZEZxUfhAwz9XVHSt2qJu9_Sx14n8hqzskm57fkze"
    
        titles_dict = get_existing_sheets(service, CURRENT_YEAR_RS)
        idx_list = bf.existing_ids(titles_dict)
        choice = ui.prompt("Please select a sheet to make receipts from:")
        sheet_choice = idx_list[choice]
        display_month = str(input("type display month you wish to appear?"))

        # display current date
        # replace { display_month } in template
        formatted_date = datetime.utcnow()
        formatted_date = datetime.strftime(formatted_date, '%Y-%m-%d')
        parameters = {
        "current_date" : formatted_date, 
        "display_month": display_month,
        "sheet_choice": sheet_choice[1][0]
        }

        print(parameters)

    
        RentReceipts.run_script(service=service_scripts, deploy_id=deploy_id, function_name="test1", parameters=parameters) 
    

