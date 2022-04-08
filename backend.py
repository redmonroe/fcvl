import os
from peewee import *
import datetime
import pandas as pd
basedir = os.path.abspath(os.path.dirname(__file__))

db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db')

class BaseModel(Model):
    class Meta:
        database = db

class Tenant(BaseModel):
    tenant_name = CharField(unique=True)

    def load_tenants(self, filename, verbose=False):
        df = pd.read_excel(filename, header=16)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(100))

        t_name = df['Name'].tolist()
        unit = df['Unit'].tolist()
        # k_rent = self.str_to_float(df['Lease Rent'].tolist())
        # t_rent = self.str_to_float(df['Actual Rent Charge'].tolist())
        # subsidy = self.str_to_float(df['Actual Subsidy Charge'].tolist())

        return t_name

# class Tweet(BaseModel):
#     user = ForeignKeyField(User, backref='tweets')
#     message = TextField()
#     created_date = DateTimeField(default=datetime.datetime.now)
#     is_published = BooleanField(default=True)