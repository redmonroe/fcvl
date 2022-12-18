import os
import time
from datetime import datetime
from pathlib import Path

import click
import pytest
from peewee import *

from annual_financials import AnnFin
from backend import ProcessingLayer, db
from config import Config
from db_utils import DBUtils
from figuration import Figuration
from file_manager import path_to_statements, write_hap
from letters import AddressWriter, DocxWriter, Letters
from manual_entry import ManualEntry
from pdf import StructDataExtract
from where_are_we import WhereAreWe

'''
cli.add_command(nbofi)
cli.add_command(consume_and_backup_invoices)
'''

def return_test_config_incr1():
    path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/iter_build_first')
    path = Path('/mnt/c/Users/joewa/Google Drive/fall creek village I/fcvl/fcvl_test/iter_build_second')

@click.group()
def cli():
    pass

@click.command()
@click.option('-m', '--most-recent-good', default=True, help='most recent good month or select ui')
def where_are_we(most_recent_good):
    click.echo('TEST: where_are_we')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()    
    where = WhereAreWe(path=path, full_sheet=full_sheet, build=build, service=service, ms=ms)
    
    #TODO
    if most_recent_good:
        where.select_month(date=True)
    else:
        where.select_month()
        
    # where.query_practice()
    # where.load_canon()
    
@click.command()
def status_findexer_test():
    click.echo('show status of findex db')
    # path, full_sheet, build, service, ms = return_test_config()
    # player = ProcessingLayer()
    player.show_status_table(path=path, db=db)

@click.command()
def reset_db_test():
    click.echo('TEST: dropping test db . . .')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    figure.reset_db()

@click.command()
def reset_db_prod():
    click.echo('dropping PRODUCTION db . . .')
    figure = Figuration(mode='production')
    path, full_sheet, build, service, ms = figure.return_configuration()
    figure.reset_db()

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
def docx_letters():    
    click.echo('writing rent receipts to docx in fcvl_output_test')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    docx = DocxWriter(db=build.main_db, service=service)
    docx.docx_rent_receipts_from_rent_sheet()

@click.command()
def addresses():
    """generates spreadsheet for mailmerge"""
    """this only works on test db right now"""
    click.echo('generating addresses')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    letters = AddressWriter(db=build.main_db)    
    addresses = letters.get_addresses()
    letters.export_to_excel(address_list=addresses)
    
@click.command()
def balanceletters():
    click.echo('balance letters')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    letters = Letters(db=build.main_db)
    letters.balance_letters()

@click.command()
def receipts_sixm():
    click.echo('receipts with 6 month balance; this is an attempt to make a google app; currently unfinished')
    player = ProcessingLayer()
    player.rent_receipts_wrapper_version2()

@click.command()
def workorders_to_db():
    click.echo('work orders to db')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    work_orders = Letters(db=build.main_db, gsheet_id=Config.WORK_ORDER_SHEET, work_order_range='archived_wo_2022!A1:H350')
    work_orders.get_all_archived_work_orders()

@click.command()
def export_workorders_docx():
    click.echo('export work orders to docx')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    docx = DocxWriter(db=build.main_db, service=service)
    first_dt = datetime(2022, 1, 1)
    last_dt = datetime(2022, 12, 31)
    docx.export_workorders_to_docx(first_dt=first_dt, last_dt=last_dt)

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
def delete_one_sheet():
    click.echo('deleting one sheet')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    ms.delete_one_month_sheet(service, full_sheet)

@click.command()
def make_one_sheet():
    click.echo('making one sheet')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    ms.auto_control(mode='single_sheet')

@click.command()
def db_to_excel():
    click.echo('write_db_to_excel')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    ms.auto_control(mode='to_excel')

"""ANNUAL FINANCIALS"""
@click.command()
def annfin():
    click.echo('annual financials')
    figure = Figuration()
    path, output_path, db = figure.annfin_test_configuration()
    annfin = AnnFin(db=db)
    annfin.trial_balance_portal()

"""TESTING COMMANDS"""
@click.command()
@click.option('--write', default='False', help='do you want to write to rs or not?')
def test_full(write):
    click.echo('run full test suite')

    """run and do not teardown testing sheet"""   

    if write == 'False':
        click.echo('run full test suite WITHOUT WRITE')
        no_write = pytest.main(['-s', '--write', 'False', 'tests',])
    elif write == 'True':
        click.echo('run full test suite WITH WRITE')
        write_rs = pytest.main(['-s', '--write', 'True', 'tests',])

@click.command()
@click.option('--write', default='False', help='do you want to write to rs or not?')
def test_canonical(write):
    click.echo('run test suite on fcvl/canonical_docs')

    """goal2:
        1 - do build mode for jan only
        2 - do iter mode for remaining months
    """

    if write == 'False':
        click.echo('run canonical docs test suite WITHOUT WRITE')
        no_write = pytest.main(['-s', '--write', 'False', 'tests/test_main_canonical.py',])
    elif write == 'True':
        click.echo('run canonical docs test suite WITH WRITE')
        write_rs = pytest.main(['-s', '--write', 'True', 'tests/test_main_canonical.py',])

@click.command()
def test_rent_receipts():
    click.echo('run docx rent receipt writing')
    no_write = pytest.main(['-s', 'tests/test_rent_receipts.py',])

@click.command()
def test_addresses():
    click.echo('run testing for addressing package')
    no_write = pytest.main(['-s', 'tests/test_addresses.py',])

@click.command()
@click.option('--select', default='canonical', help='which type of test would you like to run?')
def test_various(select):
    click.echo('run one of a variety of tests')

    if select == 'canonical':
        click.echo('run canonical docs test suite WITHOUT WRITE')
        no_write = pytest.main(['-s', '--write', 'False', 'tests/test_main_canonical.py',])
    elif select == 'deplist':
        click.echo('run deplist test to simulate writng from a month with a scrape only; WRITE is NOT enabled')
        no_write = pytest.main(['-s', '--write', 'False', 'tests/test_deplist.py',])
    elif select == 'deplistw':
        click.echo('run deplist test to simulate writng from a month with a scrape only; WRITE is ENABLED')
        write = pytest.main(['-s', '--write', 'True', 'tests/test_deplist.py',])


cli.add_command(escrow)
cli.add_command(receipts_sixm)
cli.add_command(status_findexer_test)
cli.add_command(where_are_we)
cli.add_command(reset_db_test)
cli.add_command(reset_db_prod)
cli.add_command(write_all_test)
cli.add_command(write_all_prod)
cli.add_command(load_db_test)
cli.add_command(load_db_prod)
cli.add_command(sqlite_dump)
cli.add_command(docx_letters)
cli.add_command(addresses)
cli.add_command(workorders_to_db)
cli.add_command(export_workorders_docx)
cli.add_command(recvactuals)
cli.add_command(manentry)
cli.add_command(delete_one_sheet)
cli.add_command(make_one_sheet)
cli.add_command(db_to_excel)
"""ANNUAL FINANCIALS"""
cli.add_command(annfin)
"""TESTING COMMANDS"""
cli.add_command(test_full)
cli.add_command(test_canonical)
cli.add_command(test_rent_receipts)
cli.add_command(test_addresses)
cli.add_command(test_various)

if __name__ == '__main__':
    cli()
