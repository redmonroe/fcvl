import os
import time

import click
import pytest

from auth_work import oauth
from build_rs import BuildRS
from checklist import Checklist
from config import Config, my_scopes
from file_indexer import FileIndexer
from file_manager import path_to_statements, write_hap
from receipts import RentReceipts
from records import record
from setup_month import MonthSheet
from setup_year import YearSheet
from tests.test_combined_formatting import TestChecklist

from peewee import *

'''
MAKE MODE EXPLICIT: DEV PROD TESTING
'''

# core functionalit:
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
    if mode == 'testing':
        basedir = os.path.abspath(os.path.dirname(__file__))
        path = Config.TEST_RS_PATH
        f_db = Config.test_findex_db
        f_name = Config.test_findex_name
        pw_db = SqliteDatabase(f'{basedir}/sqlite/test_pw_db.db', pragmas={'foreign_keys': 1})
    build = BuildRS(path=path, main_db=pw_db, findex_db=f_db, findex_tablename=f_name)
    build.new_auto_build()
    build.summary_assertion_at_period(test_date='2022-03')
'''
@click.command()
@click.option('--mode', required=True)
def autors(mode=None):
    click.echo(f'starting **autors*** in mode: {mode}')
    click.echo('\nmust explicitly set mode: testing, dev, prod')
    
    sleep1 = 8
    dev_full_sheet = Config.dev_rs
    dev_discard_pile = Config.DEV_MOVE_PATH # is this used?  

    dev_findex_db = Config.findex_dev_db
    dev_findex_tablename = Config.findex_dev_name

    dev_cl_db = Config.cl_dev_db
    dev_cl_tablename = Config.cl_dev_name

    dev_build_db = Config.build_dev_db
    dev_build_tablename = Config.build_dev_name

    prod_findex_db = Config.findex_prod_db
    prod_findex_tablename = 'findex_prod'
    prod_rs_db = Config.test_build_db
    prod_rs_tablename = 'build_dev'

    findex = FileIndexer(checklist, path=dev_path, discard_pile=dev_discard_pile, db=dev_findex_db, table=dev_findex_tablename)
    mformat = MonthSheet(full_sheet=dev_full_sheet, path=dev_path, sleep=sleep)

    build = BuildRS(sleep1, full_sheet=dev_full_sheet, path=dev_path, mode='dev', db=dev_build_db, rs_tablename=dev_build_tablename, sleep=sleep, checklist=checklist, findex_db=dev_findex_db, findex_table=dev_findex_tablename, findex_obj=findex, mformat=mformat)    
    
    """
    NEED TO KEEP CHECKLIST IN ORDER TO DROP THAT TABLE
    """


    breakpoint()

    if mode == 'iter_dev':
        build.iterative_build(checklist_mode='iterative_cl')  

    elif mode == 'reset_dev':
        build.reset_full_sheet()
        build.findex.drop_tables()
        db, tablename = build.checklist.drop_checklist()

    elif mode == 'reset_sheet':   
        build.reset_full_sheet()

    elif mode == 'reset_cl':   
        db, tablename = build.checklist.drop_checklist()
        cur_cl = build.checklist.show_checklist()

        if cur_cl == []:
            print(f'database: {db}, table {tablename} are empty!')
        else:
            for item in cur_cl:
                print(item)
                
    elif mode == 'show_cl':   
        cur_cl = build.checklist.show_checklist(verbose=True)
            
    elif mode == 'make_checklist':  
        cur_cl = build.checklist.make_checklist()
        cur_cl = build.checklist.show_checklist()
        for item in cur_cl:
            print(item)

    elif mode == 'reset_findex':
        build.findex.drop_tables()

    elif mode == 'show_findex':
        findex_records = build.findex.show_checklist()
        print(f'showing records for {build.findex.tablename}')
        for item in findex_records:
            print(item['id'], '*', item['fn'], item['period'], item['status'], item['indexed'], item['hap'])

'''
'''
@click.command()
def rent_receipts():
    click.echo('Generate rent receipts')
    # rr1()
@click.command()
def rent_receipts():
    click.echo('Generate rent receipts')
    RentReceipts.rent_receipts()

@click.command()
def merchants():
    merchants_pdf_extract()
    click.echo('!!!1see data/output for output files #todo!!!')


@click.command()
def annual_financials():
    click.echo('extracting data to prepare periodic income reports')
 



@click.command()
def workorders_todo():
    click.echo('you have most of this just tie it into fcvfin.py or something')


cli.add_command(rent_receipts)
cli.add_command(pgdump)
cli.add_command(merchants)
cli.add_command(nbofi)
cli.add_command(workorders_todo)
'''
@click.command()
def sqlite_dump():
    click.echo('Dumping current tables to sqlite folder on GDrive.')
    from db_utils import DBUtils
    DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path, path_to_backup=Config.sqlite_dump_path)

cli.add_command(autors)
cli.add_command(sqlite_dump)

if __name__ == '__main__':
    cli()

