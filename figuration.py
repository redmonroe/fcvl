from auth_work import oauth
from backend import PopulateTable
from build_rs import BuildRS
from config import Config
from iter_rs import IterRS
from setup_month import MonthSheet


class Figuration:

    def __init__(self,
                 mode='testing', 
                 method='iter', 
                 path=None, 
                 presentation_layer=None, 
                 close_layer=None,
                 pytest=None):
        '''default to iterative, testing config, sheet, path'''
        self.mode = mode

        if path:
            self.path = path
        else:
            self.path = Config.TEST_PATH

        if presentation_layer:
            self.presentation_layer = presentation_layer 
        else:
            self.presentation_layer = Config.TEST_RS
            
        if close_layer:
            self.close_layer = close_layer
        else:
            self.close_layer = Config.TEST_CLOSE_LAYER

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
            self.build = self.method(
                path=self.path, 
                presentation_layer=self.presentation_layer, 
                mode=self.mode, 
                pytest=self.pytest)
            self.service = oauth(Config.my_scopes, 
                                 'sheet', 
                                 mode=self.mode)
            self.ms = MonthSheet(presentation_layer=self.presentation_layer, 
                                 path=self.path,
                                 mode=self.mode, 
                                 test_service=self.service)
        if self.mode == 'production':
            self.path = Config.PROD_PATH
            self.presentation_layer = Config.PROD_RS
            self.build = self.method(path=self.path, 
                                     presentation_layer=self.presentation_layer)
            self.service = oauth(Config.my_scopes, 'sheet')
            self.ms = MonthSheet(presentation_layer=self.presentation_layer, 
                                 path=self.path)

    def annfin_test_configuration(self):
        return Config.TEST_ANNFIN_PATH, Config.TEST_ANNFIN_OUTPUT, Config.TEST_DB, self.service, self.presentation_layer

    def return_configuration(self):
        return self.path, self.presentation_layer, self.close_layer, self.build, self.service, self.ms

    def reset_db(self):
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        self.build.main_db.drop_tables(models=create_tables_list1)
        if self.build.main_db.get_tables() == []:
            print('db successfully dropped')
