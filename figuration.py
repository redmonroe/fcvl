from auth_work import oauth
from backend import PopulateTable
from build_rs import BuildRS
from config import Config
from iter_rs import IterRS
from setup_month import MonthSheet


class Figuration:

    def __init__(self, mode='testing', method='iter', path=None, full_sheet=None, pytest=None):
        '''default to iterative, testing config, sheet, path'''
        self.mode = mode

        if path:
            self.path = path
        else:
            self.path = Config.TEST_PATH

        if full_sheet:
            self.full_sheet = full_sheet
        else:
            self.full_sheet = Config.TEST_RS

        if pytest:
            self.pytest = pytest
        else:
            self.pytest = False

        self.method = method

        if self.method == 'iter':
            self.method = IterRS
        else:
            self.method = BuildRS

        if self.mode == 'testing':
            self.build = self.method(path=self.path, full_sheet=self.full_sheet, mode=self.mode, pytest=self.pytest)
            self.service = oauth(Config.my_scopes, 'sheet', mode=self.mode)
            self.ms = MonthSheet(full_sheet=self.full_sheet, path=self.path, mode=self.mode, test_service=self.service)
        if self.mode == 'production':
            self.path = Config.PROD_PATH
            self.full_sheet = Config.PROD_RS
            self.build = self.method(path=self.path, full_sheet=self.full_sheet)
            self.service = oauth(Config.my_scopes, 'sheet')
            self.ms = MonthSheet(full_sheet=self.full_sheet, path=self.path)

    def return_configuration(self):
        return self.path, self.full_sheet, self.build, self.service, self.ms
    
    def reset_db(self):
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        self.build.main_db.drop_tables(models=create_tables_list1)
        if self.build.main_db.get_tables() == []:
            print('db successfully dropped')
