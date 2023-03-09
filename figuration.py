from auth_work import oauth
from backend import PopulateTable, Consume
from build_rs import BuildRS
from config import Config
from iter_rs import IterRS
from setup_month import MonthSheet


class Figuration:

    def __init__(self,
                 mode='testing', 
                 method='iter', 
                 path=None, 
                 staging_layer=None, 
                 close_layer=None,
                 consume_path=None,
                 pytest=None):
        
        '''default to iterative, testing config, sheet, path'''
        self.mode = mode

        if path:
            self.path = path
        else:
            self.path = Config.TEST_PATH
            
        if consume_path:
            self.consume_path = consume_path
        else:
            self.consume_path = Config.CONSUME_PATH 

        if staging_layer:
            self.staging_layer = staging_layer 
        else:
            self.staging_layer = Config.TEST_RS
            
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
                staging_layer=self.staging_layer, 
                mode=self.mode, 
                pytest=self.pytest)
            self.service = oauth(Config.my_scopes, 
                                 'sheet', 
                                 mode=self.mode)
            self.ms = MonthSheet(staging_layer=self.staging_layer, 
                                 path=self.path,
                                 mode=self.mode, 
                                 test_service=self.service)
        
        if self.mode == 'production':
            self.path = Config.PROD_PATH
            self.staging_layer = Config.PROD_RS
            self.build = self.method(path=self.path, 
                                     staging_layer=self.staging_layer)
            self.service = oauth(Config.my_scopes, 'sheet')
            self.ms = MonthSheet(staging_layer=self.staging_layer, 
                                 path=self.path)
            
    def analysis_test_configuration(self):
        return Config.TEST_DB

    def annfin_test_configuration(self):
        return Config.TEST_ANNFIN_PATH, Config.TEST_ANNFIN_OUTPUT, Config.TEST_DB, self.service, self.staging_layer

    def return_configuration(self):
        return self.path, self.staging_layer, self.close_layer, self.build, self.service, self.ms
    
    def return_write_configuration(self):
        return self.ms
    
    def return_just_build_configuration(self):
        return self.build, self.service
    
    def return_consume_configuration(self):
        reset_func = self.reset_consume
        self.consume = Consume()
        populate = PopulateTable()
        create_tables_list1 = populate.return_consume_table()
        self.build.main_db.create_tables(create_tables_list1)
        return self.consume_path, self.build, self.consume, reset_func
    
    def reset_consume(self):
        populate = PopulateTable()
        create_tables_list1 = populate.return_consume_table()
        self.build.main_db.drop_tables(models=create_tables_list1)
        if self.build.main_db.get_tables() == []:
            print('db successfully dropped')

    def reset_db(self):
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        self.build.main_db.drop_tables(models=create_tables_list1)
        if self.build.main_db.get_tables() == []:
            print('db successfully dropped')
