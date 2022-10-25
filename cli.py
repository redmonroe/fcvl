import os
import time
from pathlib import Path

import click
import pytest
from peewee import *

from annual_financials import AnnFin
from auth_work import oauth
from backend import PopulateTable, ProcessingLayer, QueryHC, StatusRS, db, Unit
from build_rs import BuildRS
from config import Config
from db_utils import DBUtils
from file_indexer import FileIndexer
from file_manager import path_to_statements, write_hap
from iter_rs import IterRS
from letters import Letters
from manual_entry import ManualEntry
from pdf import StructDataExtract
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet

'''
cli.add_command(nbofi)
cli.add_command(consume_and_backup_invoices)
'''

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

def return_test_config_incr1():
    path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/iter_build_first')
    path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/iter_build_second')

def set_db(build=None):
    """this should not DROP tables"""
    populate = PopulateTable()
    create_tables_list1 = populate.return_tables_list()
    if build.main_db.is_closed() == True:
        build.main_db.connect()

def reset_db(build=None):
    populate = PopulateTable()
    create_tables_list1 = populate.return_tables_list()
    build.main_db.drop_tables(models=create_tables_list1)
    if build.main_db.get_tables() == []:
        print('db successfully dropped')

@click.group()
def cli():
    pass

@click.command()
@click.option('--incr', default=1, help='run fresh or run incremental build')
def incremental_build(incr):
    click.echo('iter_build from cli')
    from iter_rs import IterRS

    if incr == 1:
        print('do nothing')
    elif incr == 2:
        '''this is JAN 2020 ONLY, opcash'''
        figure = Figuration(path=Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/jan_2022_only'))
        path, full_sheet, build, service, ms = figure.return_configuration()
        if build.main_db.get_tables() == []:
            print(f'path: {path}')
            print(f'sheet_url: {full_sheet}')
            build.build_db_from_scratch(write=True)
        else:
            print('reset (from scratch)')
            reset_db(build=build)
    elif incr == 3:
        '''first iterative increment: feb only, opcash'''
        path, full_sheet, build, service, ms = return_test_config_incr1()
        path, full_sheet, iterb, service, ms = return_test_config_incr2()
        if iterb.main_db.get_tables() == []:
            print('setting up initial state for "testing"')
            print(f'path: {path}')
            print(f'sheet_url: {full_sheet}')
            build.build_db_from_scratch(write=True)
            print('build from incr')
            iterb.incremental_load()
        else:
            print('reset (from incr)')
            reset_db(build=iterb)
    elif incr == 4:
        """just incremental, don't drop full db"""
        """how do I simulate the rebuild of rs_reconcile column"""
        path, full_sheet, iterb, service, ms = return_test_config_incr2()
        print(f'sheet_url: {full_sheet}')
        print(f'path: {path}')
        breakpoint()
        iterb.incremental_load()
    elif incr == 5:
        """run build from scratch from /iter_build_second"""
        path, full_sheet, iterb, service, ms = return_test_config_incr2()
        if iterb.main_db.get_tables() == []:
            print(f'path: {path}')
            print(f'sheet_url: {full_sheet}')
            iterb.build_db_from_scratch(write=True)
            print('build from incr')
            iterb.incremental_load()
        else:
            print('reset (from incr)')
            reset_db(build=iterb)
    else:
        print('exiting program')
        exit

@click.command()
def reset_db_test():
    click.echo('TEST: dropping test db . . .')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    reset_db(build=build)

@click.command()
def reset_db_prod():
    click.echo('dropping PRODUCTION db . . .')
    figure = Figuration(mode='production')
    path, full_sheet, build, service, ms = figure.return_configuration()
    reset_db(build=build)

@click.command()
def load_db_test():
    click.echo('TEST: loading all available files in path to db')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    build.build_db_from_scratch()    

@click.command()
def load_db_prod():
    click.echo('PRODUCTION: loading all available files in path to db')
    figure = Figuration(mode='production')
    path, full_sheet, build, service, ms = figure.return_configuration() 
    build.build_db_from_scratch()   

@click.command()
def write_all_prod():
    click.echo('PRODUCTION: write all db contents to rs . . .')
    figure = Figuration(mode='production')
    path, full_sheet, build, service, ms = figure.return_configuration() 
    ms.auto_control(source='cli.py', mode='clean_build')

@click.command()
def write_all_test():
    click.echo('TEST: write all db contents to rs . . .')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration() 
    ms.auto_control(source='cli.py', mode='clean_build')
    
    # sample_month_list = ['2022-01', '2022-02']
    # ms.auto_control(month_list=sample_month_list)
    
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
def addresses():
    click.echo('generating addresses')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    letters = Letters(db=build.main_db)
    letters.get_addresses()

@click.command()
def balanceletters():
    click.echo('balance letters')
    letters = Letters()
    letters.balance_letters()

@click.command()
def receipts():
    click.echo('receipts')
    player = ProcessingLayer()
    player.rent_receipts_wrapper()

@click.command()
def receipts_sixm():
    click.echo('receipts with 6 month balance')
    player = ProcessingLayer()
    player.rent_receipts_wrapper_version2()

@click.command()
def workorders():
    click.echo('work orders')
    work_orders = RentReceipts()
    work_orders.work_orders()

@click.command()
def escrow():
    """ For now we output each dataframe into fcvl/escrow"""
    click.echo('take apart escrow report')
    StructDataExtract.escrow_wrapper(output_path=Config.TEST_FCVL_BASE)
    
@click.command()
def recvactuals():
    click.echo('receivable actuals')
    annfin = AnnFin(db=Config.TEST_DB)
    annfin.start_here()
    
@click.command()
def status_findexer_test():
    click.echo('show status of findex db')
    path, full_sheet, build, service, ms = return_test_config()
    player = ProcessingLayer()
    player.show_status_table(path=path, db=db)

cli.add_command(escrow)
cli.add_command(receipts)
cli.add_command(receipts_sixm)
cli.add_command(status_findexer_test)
cli.add_command(reset_db_test)
cli.add_command(reset_db_prod)
cli.add_command(write_all_test)
cli.add_command(write_all_prod)
cli.add_command(load_db_test)
cli.add_command(load_db_prod)
cli.add_command(sqlite_dump)
cli.add_command(addresses)
cli.add_command(balanceletters)
cli.add_command(workorders)
cli.add_command(recvactuals)
cli.add_command(incremental_build)
cli.add_command(manentry)

if __name__ == '__main__':
    cli()
