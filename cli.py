import os
import time

import click
import pytest
from peewee import *

from auth_work import oauth
from backend import PopulateTable, StatusRS
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
cli.add_command(rent_receipts)
cli.add_command(merchants)
cli.add_command(nbofi)
cli.add_command(annual_financial)
cli.add_command(consume_and_backup_invoices)
cli.add_command(workorders_todo)
'''

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

    path = Config.TEST_RS_PATH_APRIL
    if mode == 'testing':
        basedir = os.path.abspath(os.path.dirname(__file__))
        # pw_db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db', pragmas={'foreign_keys': 1})
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.new_auto_build()

    if mode == 'iter_testing':
        basedir = os.path.abspath(os.path.dirname(__file__))
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.iter_build()


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

