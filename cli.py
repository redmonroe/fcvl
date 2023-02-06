from datetime import datetime

import click
import pytest

from annual_financials import AnnFin
from backend import ProcessingLayer, db
from config import Config
from db_utils import DBUtils
from figuration import Figuration
from letters import AddressWriter, DocxWriter, Letters
from manual_entry import ManualEntry
from pdf import StructDataExtract
from where_are_we import WhereAreWe

'''
cli.add_command(nbofi)
cli.add_command(consume_and_backup_invoices)
'''


@click.group()
def cli():
    pass


@click.command()
@click.option('-m', '--most-recent-good',
              default=True, help='most recent good month versus select ui')
def where_are_we(most_recent_good):
    click.echo('TEST: where_are_we')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    # TODO: NEED TO FIX MONTH CHOICE WHEN WE ARE DEALING WITH YEAR OVERLAPS
    if most_recent_good:
        where = WhereAreWe(date=True, path=path, full_sheet=full_sheet,
                           build=build, service=service, ms=ms)
    else:
        where = WhereAreWe(path=path, full_sheet=full_sheet,
                           build=build, service=service, ms=ms)


@click.command()
def status_findexer_test():
    click.echo('show status of findex db')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    player = ProcessingLayer()
    player.show_status_table(path=path, db=db)


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
def reset_db(production=False):
    '''resets db, default=test db'''
    if production:
        click.echo('dropping PRODUCTION db . . .')
        figure = Figuration(mode='production')
        path, full_sheet, build, service, ms = figure.return_configuration()
        figure.reset_db()
    else:
        click.echo('TEST: dropping test db . . .')
        figure = Figuration()
        path, full_sheet, build, service, ms = figure.return_configuration()
        figure.reset_db()


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
@click.option('-l', '--last_month', type=str, help='pass an explicit final month to db builder (ie "2022-12")')
def load_db(production=False, last_month=None):
    if production:
        # TODO: last month no supported in this branche
        click.echo('PRODUCTION: loading all available files in path to db')
        figure = Figuration(mode='production')
        path, full_sheet, build, service, ms = figure.return_configuration()
        ms.auto_control(source='cli.py', mode='clean_build')
    else:
        click.echo('TEST: loading all available files in path to db')
        figure = Figuration(method='not_iter')
        path, full_sheet, build, service, ms = figure.return_configuration()
        if last_month:
            print(last_month)
            build.build_db_from_scratch(last_range_month=last_month)
        else:
            build.build_db_from_scratch()


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
def write(production=False):
    if production:
        click.echo('PRODUCTION: write all db contents to rs . . .')
        figure = Figuration(mode='production')
        path, full_sheet, build, service, ms = figure.return_configuration()
    else:
        click.echo('TEST: write all db contents to rs . . .')
        figure = Figuration()
        path, full_sheet, build, service, ms = figure.return_configuration()
        ms.auto_control(source='cli.py', mode='clean_build')


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
@click.option('-m', '--move_to_final',
              default=False, help='write month to final presentation sheet for peg')
def close_sheet(production=False, move_to_final=False):
    if production:
        click.echo('PRODUCTION: close_sheet')
        figure = Figuration(mode='production')
        print('no production branch of close_sheet')
        # path, full_sheet, build, service, ms = figure.return_configuration()
    elif move_to_final:
        click.echo('TEST: move closed month to final presentation sheet')
        figure = Figuration()
        path, full_sheet, build, service, ms = figure.return_configuration()
        ms.move_to_final(service, full_sheet, db=build)
        
    else:
        click.echo('TEST: close_sheet')
        figure = Figuration()
        path, full_sheet, build, service, ms = figure.return_configuration()
        ms.close_one_month(service, full_sheet, db=build)

        # implement a drop functionality
        # db.drop_tables([FinalMonth])

        # implement a way to finalize a series of months


@click.command()
def write_monthrange_test():
    click.echo('TEST: write all db contents to rs: EXPRESS MONTHRANGE . . .')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    # month_list = ['2022-01', '2022-02', '2022-03', '2022-04', '2022-05',
    #   '2022-06', '2022-07', '2022-08', '2022-09', '2022-10',
    #   '2022-11', '2022-12']
    month_list = ['2022-01', '2022-02', '2022-03']
    ms.auto_control(source='cli.py', mode='clean_build', month_list=month_list)


@click.command()
def manentry():
    click.echo('delete or modify rows of the database')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    manentry = ManualEntry(db=build.main_db)
    manentry.main()


@click.command()
def sqlite_dump():
    click.echo('backup db')
    click.echo('Dumping current tables to sqlite folder on GDrive.')
    DBUtils.dump_sqlite(path_to_existing_db=Config.sqlite_test_db_path,
                        path_to_backup=Config.sqlite_dump_path)


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
    docx = DocxWriter(db=build.main_db, service=service)
    docx.export_history_to_docx(
        threshold=100, startm='2022-01', endm='2022-12')
    
      
    # add boiler plate
    # read doc from command line
    # move closed month to rent_sheet_fs
    # use prior month closed month bal in end_bal for production of next month staging!!!!
    #THIS IS THE FUCKING ANSWER
    
    
    
    # should do a filter of 
        # - get most recent closed month
        # - get rent roll as of that period
        # - do look back from most recent closed month

    import subprocess
    subprocess.run('''
    # This for loop syntax is Bash only
    pandoc -f docx -t asciidoc /mnt/c/Users/joewa/Google\ Drive/fall\ creek\ village\ I/fcvl/tenbal_output/tenantbals_02-05-2023.docx
    ''',
                   shell=True, check=True,
                   executable='/bin/bash')


@click.command()
@click.option('-d', '--drop_table',
              default=False, help='drop workorder table only')
def workorders_to_db(drop_table=None):
    click.echo('work orders to db')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    work_orders = Letters(db=build.main_db,
                          gsheet_id=Config.WORK_ORDER_SHEET,
                          work_order_range='archived_wo_2022!A1:H350')
    if drop_table is True:
        WorkOrder = work_orders.get_workorder_object()
        WorkOrder.drop_table()
        print('successfully dropped WorkOrder table')
    else:
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


@click.command()
def escrow():
    """ For now we output each dataframe into fcvl/escrow"""
    click.echo('take apart escrow report')
    StructDataExtract.escrow_wrapper(output_path=Config.TEST_FCVL_BASE)


@click.command()
def recvactuals():
    click.echo('receivable actuals')
    figure = Figuration()
    path, output_path, db, service, full_sheet = figure.annfin_test_configuration()

    # setup database and configuration
    annfin = AnnFin(db=Config.TEST_DB, full_sheet=full_sheet)
    annfin.start_here()


"""TESTING COMMANDS"""


@click.command()
@click.option('--write',
              default='False',
              help='do you want to write to rs or not?')
def test_full(write):
    click.echo('run full test suite')

    """run and do not teardown testing sheet"""

    if write == 'False':
        click.echo('run full test suite WITHOUT WRITE')
        _ = pytest.main(['-s', '--write', 'False', 'tests', ])
    elif write == 'True':
        click.echo('run full test suite WITH WRITE')
        _ = pytest.main(['-s', '--write', 'True', 'tests', ])


@click.command()
@click.option('--write',
              default='False',
              help='do you want to write to rs or not?')
def test_canonical(write):
    click.echo('run test suite on fcvl/canonical_docs')

    """goal2:
        1 - do build mode for jan only
        2 - do iter mode for remaining months
    """

    if write == 'False':
        click.echo('run canonical docs test suite WITHOUT WRITE')
        _ = pytest.main(
            ['-s', '--write', 'False', 'tests/test_main_canonical.py', ])
    elif write == 'True':
        click.echo('run canonical docs test suite WITH WRITE')
        _ = pytest.main(
            ['-s', '--write', 'True', 'tests/test_main_canonical.py', ])


@click.command()
def test_rent_receipts():
    click.echo('run docx rent receipt writing')
    _ = pytest.main(['-s', 'tests/test_rent_receipts.py', ])


@click.command()
def test_addresses():
    click.echo('run testing for addressing package')
    _ = pytest.main(['-s', 'tests/test_addresses.py', ])


@click.command()
@click.option('--select',
              default='canonical',
              help='which type of test would you like to run?')
def test_various(select):
    click.echo('run one of a variety of tests')

    if select == 'canonical':
        click.echo('run canonical docs test suite WITHOUT WRITE')
        _ = pytest.main(
            ['-s', '--write', 'False', 'tests/test_main_canonical.py', ])
    elif select == 'deplist':
        click.echo(
            'run deplist test to simulate writng from ',
            'a month with a scrape only; WRITE is NOT enabled')
        _ = pytest.main(
            ['-s', '--write', 'False', 'tests/test_deplist.py', ])
    elif select == 'deplistw':
        click.echo(
            'run deplist test to simulate writng from ',
            'a month with a scrape only; WRITE is ENABLED')
        _ = pytest.main(
            ['-s', '--write', 'True', 'tests/test_deplist.py', ])


cli.add_command(escrow)
cli.add_command(status_findexer_test)
cli.add_command(where_are_we)
cli.add_command(reset_db)
cli.add_command(write)
cli.add_command(close_sheet)
cli.add_command(load_db)
cli.add_command(delete_one_sheet)
cli.add_command(make_one_sheet)
cli.add_command(db_to_excel)
cli.add_command(write_monthrange_test)
cli.add_command(sqlite_dump)
cli.add_command(docx_letters)
cli.add_command(balanceletters)
cli.add_command(addresses)
cli.add_command(workorders_to_db)
cli.add_command(export_workorders_docx)
cli.add_command(recvactuals)
cli.add_command(manentry)
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
