import os
from peewee import *
import datetime
import pandas as pd
from numpy import nan
basedir = os.path.abspath(os.path.dirname(__file__))

db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db')

class BaseModel(Model):
    class Meta:
        database = db

class Tenant(BaseModel):
    tenant_name = CharField(unique=True)
    status = CharField(default='active')  # this should be updated automatically when I use build_rs
    beg_bal = DecimalField(default=0.00)
    # do the join on unit and tenant name

    def load_tenants(self, filename, verbose=False):
        df = pd.read_excel(filename, header=16)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(100))

        t_name = df['Name'].tolist()
        t_name = [item.capitalize() for item in t_name if isinstance(item, str)]
        t_name = [item for item in t_name if item != 'Vacant']
        t_name = list(set(t_name))
        insert_many_list = []
        for item in t_name:
            insert_many_list.append({'tenant_name': item})

        query = Tenant.insert_many(insert_many_list)
        query.execute()

class Unit(BaseModel):
    unit_name = CharField(unique=True)
    status = CharField(default='vacant')  # this should be updated automatically when I use build_rs

    def load_units(self, filename, verbose=False):
        df = pd.read_excel(filename, header=16)
        if verbose: 
            pd.set_option('display.max_columns', None)
            print(df.head(100))

        unit = df['Unit'].tolist()
        units = list(set(units))
        insert_many_list = []
        for item in units:
            insert_many_list.append({'unit_name': item})

        query = Unit.insert_many(insert_many_list)
        query.execute()

        breakpoint()

        return units

# class Tweet(BaseModel):
#     user = ForeignKeyField(User, backref='tweets')
#     message = TextField()
#     created_date = DateTimeField(default=datetime.datetime.now)
#     is_published = BooleanField(default=True)

