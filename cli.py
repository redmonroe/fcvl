# from google_api_calls_abstract import simple_batch_update#cli.py
import click
from receipts import RentReceipts
from db_utils import pg_dump_one
from setup_month import MonthSheet
from setup_year import YearSheet
from build_rs import BuildRS
from file_manager import path_to_statements, write_hap
from pdf import merchants_pdf_extract, nbofi_pdf_extract_hap, qb_extract_p_and_l, qb_extract_security_deposit, qb_extract_deposit_detail
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

@click.group()
def cli():
    pass

@click.group()
def checklist():
    pass





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

@click.command()
def placeholder():
    click.echo('put whatever you need to here')


cli.add_command(rent_receipts)
cli.add_command(pgdump)
cli.add_command(merchants)
cli.add_command(nbofi)
cli.add_command(placeholder)
cli.add_command(workorders_todo)
'''

cli.add_command(checklist)
cli.add_command(buildrs)
cli.add_command(mformat)
cli.add_command(yformat)

if __name__ == '__main__':
    cli()
