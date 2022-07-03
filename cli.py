import os
import time

import click
import pytest
from peewee import *

from annual_financials import AnnFin
from auth_work import oauth
from backend import db, PopulateTable, StatusRS
from balance_letter import balance_letters
from build_rs import BuildRS
from config import Config
from db_utils import DBUtils
from file_indexer import FileIndexer
from file_manager import path_to_statements, write_hap
from manual_entry import ManualEntry
from receipts import RentReceipts
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet

'''
cli.add_command(nbofi)
cli.add_command(consume_and_backup_invoices)
@click.command()
def merchants():
    click.echo('merchants')
    pass
'''

@click.group()
def cli():
    pass

@click.command()
@click.option('--mode', required=True)
@record
def autors(mode=None):
    """commands for creating rent sheet database and running rent sheet writing program
    
    TO GENERATE DATABASE FROM SCRATCH: USE 'build_from_scratch'
    TO RESET DATABASE: USE 'reset'
    TO WRITE ALL MONTHS TO RENT SHEETS: USE 'write_from_db'
    """

    path = Config.TEST_RS_PATH_MAY
    full_sheet = Config.TEST_RS
    build = BuildRS(path=path, full_sheet=full_sheet, main_db=Config.TEST_DB)
    service = oauth(Config.my_scopes, 'sheet', mode='testing')
    ms = MonthSheet(full_sheet=full_sheet, path=path, mode='testing', test_service=service)
    
    if mode == 'build_from_scratch':
        build.build_db_from_scratch()
    
    if mode == 'write_from_db':
        # sample_month_list = ['2022-01']
        # sample_month_list = ['2022-01', '2022-02']
        # ms.auto_control(month_list=sample_month_list)
        ms.auto_control(source='cli.py', mode='clean_build')

    if mode == 'reset': # basic drop of all tables
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        if build.main_db.is_closed() == True:
            build.main_db.connect()
        build.main_db.drop_tables(models=create_tables_list1)
        if build.main_db.get_tables() == []:
            print('db successfully dropped')

    if mode == 'test_and_write':
        build.build_db_from_scratch(write_db=True)
    
    if mode == 'iter_first':
        path = Config.TEST_RS_PATH_ITER_BUILD1
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()

    if mode == 'iter_second':
        path = Config.TEST_RS_PATH_ITER_BUILD2
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()

    if mode == 'iter_both':
        populate = PopulateTable()
        create_tables_list1 = populate.return_tables_list()
        if build.main_db.is_closed() == True:
            build.main_db.connect()
        build.main_db.drop_tables(models=create_tables_list1)
        path = Config.TEST_RS_PATH_ITER_BUILD1
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()
        path = Config.TEST_RS_PATH_ITER_BUILD2
        build = BuildRS(path=path, main_db=Config.TEST_DB)
        build.build_db_from_scratch()

    if mode == 'write_to_db':
        click.echo('NOT A COMMAND; try write_from_db instead')
    
@click.command()
def manentry():
    click.echo('delete or modify rows of the database')
    manentry = ManualEntry(db=db)
    manentry.main()

@click.command()
def sqlite_dump():
    click.echo('backup db')
    click.echo('Dumping current tables to sqlite folder on GDrive.')
    DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

@click.command()
def balanceletters():
    click.echo('balance letters')
    balance_letters()

@click.command()
def receipts():
    click.echo('receipts')
    status = StatusRS()
    status.rent_receipts_wrapper()

@click.command()
def workorders():
    click.echo('work orders')
    work_orders = RentReceipts()
    work_orders.work_orders()

@click.command()
def recvactuals():
    click.echo('receivable actuals')
    annfin = AnnFin(db=Config.TEST_DB)
    annfin.start_here()

cli.add_command(receipts)
cli.add_command(autors)
cli.add_command(sqlite_dump)
cli.add_command(balanceletters)
cli.add_command(workorders)
cli.add_command(recvactuals)
cli.add_command(manentry)

if __name__ == '__main__':
    cli()

