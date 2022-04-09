import os
from config import Config
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
    unit = CharField()
    status = CharField(default='active')  # this should be updated automatically when I use build_rs
    beg_bal_2022 = DecimalField(default=0.00)
    # do the join on unit and tenant name

    def load_tenants(self, filename):
        df = pd.read_excel(filename, header=16)

        t_name = df['Name'].tolist()
        unit = df['Unit'].tolist()
        rent_roll_dict = dict(zip(t_name, unit))
        rent_roll_dict = {k.lower(): v for k, v in rent_roll_dict.items() if k is not nan}
        rent_roll_dict = {k: v for k, v in rent_roll_dict.items() if k != 'vacant'}
        insert_many_list = [{'tenant_name': name, 'unit': unit} for (name, unit) in rent_roll_dict.items()]

        query = Tenant.insert_many(insert_many_list)
        query.execute()

        return rent_roll_dict

class Unit(BaseModel):
    unit_name = CharField(unique=True)
    status = CharField(default='vacant')  # this should be updated automatically when I use build_rs

    def load_units(self, filename, verbose=False):
        insert_many_list = []
        for item in Config.units:
            insert_many_list.append({'unit_name': item})

        query = Unit.insert_many(insert_many_list)
        query.execute()

        # breakpoint()

        return Config.units
# class Tweet(BaseModel):
#     user = ForeignKeyField(User, backref='tweets')
#     message = TextField()
#     created_date = DateTimeField(default=datetime.datetime.now)
#     is_published = BooleanField(default=True)

