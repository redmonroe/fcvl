import os
import time
from pathlib import Path

import click
import pytest
from peewee import *

from annual_financials import AnnFin
from backend import ProcessingLayer, db
from config import Config
from db_utils import DBUtils
from file_manager import path_to_statements, write_hap
from letters import AddressWriter, DocxWriter, Letters
from manual_entry import ManualEntry
from pdf import StructDataExtract
from figuration import Figuration
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
    letters = Letters()
    letters.balance_letters()

@click.command()
def receipts():
    click.echo('receipts via google apps, cloud-based')
    player = ProcessingLayer()
    player.rent_receipts_wrapper()

@click.command()
def receipts_sixm():
    click.echo('receipts with 6 month balance; this is an attempt to make a google app; currently unfinished')
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
    ms.auto_control(mode='not clean build')

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
cli.add_command(docx_letters)
cli.add_command(addresses)
cli.add_command(balanceletters)
cli.add_command(workorders)
cli.add_command(recvactuals)
cli.add_command(incremental_build)
cli.add_command(manentry)
cli.add_command(delete_one_sheet)
cli.add_command(make_one_sheet)
"""TESTING COMMANDS"""
cli.add_command(test_full)
cli.add_command(test_canonical)
cli.add_command(test_rent_receipts)
cli.add_command(test_addresses)
cli.add_command(test_various)

if __name__ == '__main__':
    cli()
