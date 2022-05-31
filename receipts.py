# sample project: https://github.com/gsuitedevs/python-samples/tree/master/docs/mail-merge
from datetime import datetime

from auth_work import oauth
from config import Config, my_scopes
from utils import Utils


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

    def rent_receipts(self):
        
        service_scripts = oauth(my_scopes, 'script')
        service = oauth(my_scopes, 'sheet')
    
        deploy_id = "AKfycby3_qnppVYUo9g7DE3dQgu2l_xd97td8smvs66gExs8AOH00CPlxT2ciXjbS4l94qD0"
    
        titles_dict = Utils.get_existing_sheets(service, Config.TEST_RS)
        titles_dict = {name:id2 for name, id2 in titles_dict.items() if name != 'intake'}
        idx_list = Utils.existing_ids(titles_dict)
        choice = int(input("Please select a sheet to make receipts from: "))
        sheet_choice = idx_list[choice]
        display_month = str(input("type display month you wish to appear? "))

        # display current date
        # replace { display_month } in template
        formatted_date = datetime.utcnow()
        formatted_date = datetime.strftime(formatted_date, '%Y-%m-%d')
        parameters = {
        "current_date" : formatted_date, 
        "display_month": display_month,
        "sheet_choice": sheet_choice[1][0], 
        "rent_sheet": Config.TEST_RS, 
        }

        print(parameters)
        choice = str(input("send these results to google script & make receipts? y/n "))

        if choice == 'y':
            RentReceipts.run_script(service=service_scripts, deploy_id=deploy_id, function_name="test1", parameters=parameters) 
    

