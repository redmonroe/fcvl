import os
import time

import click
import pytest
from peewee import *

from annual_financials import AnnFin
from auth_work import oauth
from backend import PopulateTable, StatusRS, ProcessingLayer, db
from balance_letter import balance_letters
from build_rs import BuildRS
from config import Config
from db_utils import DBUtils
from file_indexer import FileIndexer
from file_manager import path_to_statements, write_hap
from manual_entry import ManualEntry
from pdf import StructDataExtract
from receipts import RentReceipts
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet

'''
cli.add_command(nbofi)
cli.add_command(consume_and_backup_invoices)
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
def reset_dry_run():
    click.echo('reset dry run by deleting 07 deposit')
    from backend import Findexer
    findex = FileIndexer(path=Config.TEST_RS_PATH_MAY, db=Config.TEST_DB)
    target_deposit_file = Findexer.get(Findexer.fn == 'deposits_07_2022.xls')
    target_deposit_file.delete_instance()
    # breakpoint()

    
@click.command()
def dry_run():
    from backend import QueryHC
    click.echo('dry run of findexer with new files vel non')
    findex = FileIndexer(path=Config.TEST_RS_PATH_MAY, db=Config.TEST_DB)
    query = QueryHC()
    player = ProcessingLayer()
    months_ytd, unfin_month = findex.test_for_unfinalized_months()
    
    status_objects = query.get_all_status_objects() # move this to backend > processingLayer func
    deposits = query.get_all_findexer_by_type(type1='deposits')
    deposit_months = [month for name, month in deposits]

    deposits = [(True, month) if month in deposit_months else (False, month) for month in months_ytd]    

    rent = query.get_all_findexer_by_type(type1='rent')
    click.echo('description of db')
    rent_months = [month for name, month in rent]
    rent = [(True, month) if month in rent_months else (False, month) for month in months_ytd]    

    dl_tup_list = list(zip(deposits, rent))
    breakpoint()
    header = ['month', 'deps', 'rtroll', 'oc_rec', 'ten_rec', 'rs_rec', 'scrape_rec']
    table = [header]
    for item, dep in zip(status_objects, dl_tup_list):
        row_list = []
        row_list.append(item.month)
        row_list.append(str(dep[0]))
        row_list.append('   ')
        row_list.append(str(item.opcash_processed))
        row_list.append(str(item.tenant_reconciled))
        row_list.append(str(item.rs_reconciled))
        row_list.append(str(item.scrape_reconciled))
        table.append(row_list)
    
    print('\n'.join([''.join(['{:8}'.format(x) for x in r]) for r in table]))

    click.echo('unfinalized months')
    for item in unfin_month:
        print(item)
    breakpoint()

    click.echo('unprocessed files in path')
    unproc_files, dir_contents = findex.test_for_unprocessed_file()

    for item in unproc_files:
        print(item)
    choice = int(input('running findexer now would input the above file(s)?  press 1 to proceed ...'))
    findex.iter_build_runner()

cli.add_command(escrow)
cli.add_command(receipts)
cli.add_command(autors)
cli.add_command(sqlite_dump)
cli.add_command(balanceletters)
cli.add_command(workorders)
cli.add_command(recvactuals)
cli.add_command(dry_run)
cli.add_command(reset_dry_run)
cli.add_command(manentry)

if __name__ == '__main__':
    cli()

