import time
import click
import pytest
from receipts import RentReceipts
from db_utils import pg_dump_one
from checklist import Checklist
from setup_month import MonthSheet
from setup_year import YearSheet
from build_rs import BuildRS
from tests.test_combined_formatting import TestChecklist
from file_manager import path_to_statements, write_hap
import click
from auth_work import oauth
from config import my_scopes, Config


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

    # this would be faster in production but in this stage of testing I don't have this list in memory

def timer(func):
    start_time = time.time()
    # func(mode)
    runtime = time.time() - start_time
    print(runtime)
    # print(f"n = {n} rec: --- %s seconds ---" % (rec_func_time))

@click.group()
def cli():
    pass

@click.command()
@click.option('--mode', required=True)
# @timer
def autors(mode=None):
    click.echo(f'starting **autors*** in mode: {mode}')
    click.echo('\nmust explicitly set mode: testing, dev, prod')
    
    sleep = 0
    production_full_sheet = Config.TEST_RS
    production_path = Config.TEST_RS_PATH
    production_discard_pile = Config.TEST_MOVE_PATH
    production_db = Config.test_findex_db
    prod_cl_db = Config.cl_prod_db
    prod_findex_db = Config.findex_prod_db
    prod_findex_tablename = 'findex_prod'
    prod_rs_db = Config.test_build_db
    prod_rs_tablename = 'build_prod'

    checklist = Checklist(db=prod_cl_db)
    mformat = MonthSheet(full_sheet=production_full_sheet, path=production_path)
    build = BuildRS(full_sheet=production_full_sheet, path=production_path, mode='dev', db=prod_rs_db, tablename=prod_rs_tablename, sleep=sleep, checklist=checklist, findex_db=prod_findex_db, findex_table=prod_findex_tablename, mformat=mformat)    
    # if mode == 'dev':
        # build.reset_databases() #this does nothing yet
        # build.automatic_build(checklist_mode='autoreset')     

    if mode == 'iter_dev':
        build.iterative_build(checklist_mode='iterative_cl')  

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
        build.findex.show_checklist(verbose=True)

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
def pgdump():
    click.echo('Dumping current tables to pg_backup folder.')
    pg_dump_one()


@click.command()
def workorders_todo():
    click.echo('you have most of this just tie it into fcvfin.py or something')


cli.add_command(rent_receipts)
cli.add_command(pgdump)
cli.add_command(merchants)
cli.add_command(nbofi)
cli.add_command(workorders_todo)
'''
cli.add_command(autors)

if __name__ == '__main__':
    cli()

