import os
import time

import click
import pytest
from peewee import *

from auth_work import oauth
from backend import StatusRS
from balance_letter import balance_letters
from build_rs import BuildRS
from config import Config, my_scopes
from file_indexer import FileIndexer
from file_manager import path_to_statements, write_hap
from receipts import RentReceipts
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet

'''
MAKE MODE EXPLICIT: DEV PROD TESTING
'''

# core functionalit:
'''
cli.add_command(rent_receipts)
cli.add_command(merchants)
cli.add_command(nbofi)
cli.add_command(annual_financial)
cli.add_command(consume_and_backup_invoices)
cli.add_command(workorders_todo)
'''
# what does UI experience look like now? 

# write cli to run and also reset
    # two month test will tell us a lot; the combined test should provide us with almost all the code we need to run bare bones
    # set up production environment for all but actual sheet
    # DON'T FORGET TO STAY CURRENT WITH TESTS
    
    # get year to date to run with a full rebuild and teardown each time WITH sleeps, then focus on speeding up process
        # skipping proc if checklist items are True

    # this would be faster in production but in this stage of testing I don't have this 

@click.group()
def cli():
    pass

@click.command()
@click.option('--mode', required=True)
@record
def autors(mode=None):
    '''the way we would run this:'''
    '''build database'''
    '''then run status and begin writing to rs'''

    if mode == 'testing':
        basedir = os.path.abspath(os.path.dirname(__file__))
        path = Config.TEST_RS_PATH_APRIL
        pw_db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db', pragmas={'foreign_keys': 1})
        build = BuildRS(path=path, main_db=pw_db)
        build.new_auto_build()
        build.summary_assertion_at_period(test_date='2022-03')

@click.command()
def sqlite_dump():
    click.echo('Dumping current tables to sqlite folder on GDrive.')
    from db_utils import DBUtils
    DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

@click.command()
def balanceletters():
    balance_letters()

cli.add_command(autors)
cli.add_command(sqlite_dump)
cli.add_command(balanceletters)

if __name__ == '__main__':
    cli()

