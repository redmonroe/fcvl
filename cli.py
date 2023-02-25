from datetime import datetime

import click
import pytest

from annual_financials import AnnFin
from backend import ProcessingLayer, db, FinalMonth, FinalMonthLog
from config import Config
from db_utils import DBUtils
from figuration import Figuration
from letters import AddressWriter, DocxWriter, Letters
from manual_entry import ManualEntry
from pdf import StructDataExtract
from where_are_we import WhereAreWe
from analysis import Analysis

'''
END OF MONTH flow
- download reports: x, y, x
- load_db with -e flag for explicit month
- write one sheet to staging sheet
- make pro-rates, any other adjustments, move-ins and move-outs
- upload back to finalmonth & finalmonth log with move_to_final() WHAT FLAGS???
- write back to close layer for peg
- generate and edit balanceletters() set threshold, start month, end month
- (should formalize funcs to pull out income data for audit: )
MIDMONTH flow
- after books are closed, download rentroll and deposits from onesite
- download scrape from NBOFI "from last statement date"

- use where_are_we
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
    path, staging_layer, close_layer, build, service, ms = figure.return_configuration()
    # TODO: NEED TO FIX MONTH CHOICE WHEN WE ARE DEALING WITH YEAR OVERLAPS
    # breakpoint()
    if most_recent_good:
        where = WhereAreWe(date=True,
                           path=path,
                           staging_layer=staging_layer,
                           close_layer=close_layer,
                           build=build,
                           service=service,
                           ms=ms,
                           suppress_scrape=True)
    else:
        where = WhereAreWe(path=path,
                           staging_layer=staging_layer,
                           close_layer=close_layer,
                           build=build,
                           service=service,
                           ms=ms,
                           suppress_scrape=True
                           )


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
    figure = Figuration()
    if production:
        click.echo('dropping PRODUCTION db . . .')
        figure = Figuration(mode='production')
        # path, full_sheet, build, service, ms = figure.return_configuration()
        figure.reset_db()
    else:
        click.echo('TEST: dropping test db . . .')
        figure.reset_db()


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
@click.option('-e', '--explicit_month',
              type=str,
              help='pass an explicit final month to db builder (ie "2022-12")')
@click.option('-l', '--last_month',
              type=str,
              help='pass an explicit final month to db builder (ie "2022-12")')
def load_db(production=False, last_month=None, explicit_month=None):
    figure = Figuration(method='not_iter')
    _, _, _, build, _, ms = figure.return_configuration()

    if production:
        # TODO: last month not supported in this branche
        click.echo('PRODUCTION: loading all available files in path to db')
        figure = Figuration(mode='production')
        path, staging_layer, close_layer, build, service, ms = figure.return_configuration()
        ms.auto_control(source='cli.py', mode='clean_build')
    elif explicit_month:
        print(
            f'passing explicit command to load information for: {explicit_month} only')
        build.build_explicit_month(explicit_month_to_load=explicit_month)
    elif last_month:
        choice = input(
            'ARE YOU ABSOLUTELY SURE YOU WANT TO DROP DB AND START OVER? \n enter "qwqz" to continue: ')
        if choice == 'qwqz':
            click.echo('TEST: loading all available files in path to db')
            build.build_db_from_scratch(last_range_month=last_month)
    else:
        choice = input(
            'ARE YOU ABSOLUTELY SURE YOU WANT TO DROP DB AND START OVER? \n enter "qwqz" to continue: ')
        if choice == 'qwqz':
            click.echo('TEST: loading all available files in path to db')
            build.build_db_from_scratch()


@click.command()
@click.option('-p', '--production',
              default=False, help='reset db?')
@click.option('-x', '--write_one_month',
              default=False,
              help='close a month selected from a list of months')
@click.option('-r', '--write_range',
              help='close up to passed month (ie 2022-12)')
def write(production=False,
          write_one_month=False,
          write_range=False,
          ):
    figure = Figuration()

    if production:
        click.echo('PRODUCTION: write all db contents to rs . . .')
        figure = Figuration(mode='production')
        ms = figure.return_write_configuration()

    elif write_range:
        ms = figure.return_write_configuration()
        ms.auto_control(source='cli.py',
                        mode='write_range',
                        last_range_month=write_range)

        # TODO: need explicit month flag: trick, but is it
        # still after new startbal/endbal has cell formulas

        # TODO: james martin does not write status effect correction; need to 
        # change logic in query function for full_position in setup_month
        '''
        @click.command()
        @click.option('-e', '--explicit_month',
                    type=str,
                    help='pass an explicit month to generate rent sheet')
        def make_one_sheet(explicit_month=None):
            click.echo('making one sheet')
            figure = Figuration()
            path, full_sheet, build, service, ms = figure.return_configuration()
            if explicit_month:
                ms.auto_control(mode='single_sheet',
                                explicit_month_to_load=explicit_month)
            else:
                print('must pass explicit month to make')
        '''

    else:
        click.echo('TEST: write all db contents to rs . . .')
        ms = figure.return_write_configuration()
        ms.auto_control(source='cli.py', mode='clean_build')


@click.command()
@click.option('-p', '--production',
              default=False,
              help='reset db?')
@click.option('-m', '--move_to_final',
              default=False,
              help='write month to final presentation sheet for peg')
@click.option('-d', '--drop_final',
              default=False,
              help='drop and RECREATE FinalMonth and FinalMonthLog tables only')
@click.option('-x', '--close_one_month',
              default=False,
              help='close a month selected from a list of months')
@click.option('-r', '--close_range',
              help='close up to passed month (2022-12)')
@click.option('-i', '--interrogate_log',
              default=False,
              help='what is closed in finalmonthlog table')
def close_sheet(production=False,
                move_to_final=False,
                drop_final=False,
                close_range=False,
                close_one_month=False,
                interrogate_log=False,
                ):
    figure = Figuration()
    path, staging_layer, close_layer, build, service, ms = figure.return_configuration()

    if interrogate_log:
        closed_dates = [(date.month, date.source)
                        for date in FinalMonthLog.select()]
        if closed_dates != []:
            print('source: ', closed_dates[0][1])
            print('closed months: ', [date[1][0]
                                      for date in enumerate(closed_dates, 1)])
        else:
            print('no months are closed in FinalMonthLog')

    if drop_final:
        build.main_db.drop_tables(models=[FinalMonth, FinalMonthLog])
        build.main_db.create_tables(models=[FinalMonth, FinalMonthLog])

    if move_to_final:
        click.echo('TEST: move closed month to final presentation sheet')
        staging_layer = '1t7KFE-WbfZ0dR9PuqlDE5EepCG3o3acZXzhbVRFW-Gc'
        ms.move_to_final(close_layer, service, staging_layer, db=build)

        # TODO audit funcs emanate from out of here

    if close_range:
        '''
        closes all months up to passed end date
        '''
        staging_layer = '1t7KFE-WbfZ0dR9PuqlDE5EepCG3o3acZXzhbVRFW-Gc'
        ms.close_range(last_date=close_range,
                       service=service,
                       staging=staging_layer,
                       db=build)

    if close_one_month:
        staging_layer = '1t7KFE-WbfZ0dR9PuqlDE5EepCG3o3acZXzhbVRFW-Gc'
        ms.close_one_month(service, staging_layer, db=build)

    if production:
        click.echo('PRODUCTION: close_sheet')
        figure = Figuration(mode='production')
        print('no production branch of close_sheet')
        path, full_sheet, build, service, ms = figure.return_configuration()


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
@click.argument('dates', nargs=2)
@click.argument('threshold', nargs=1)
def balanceletters(dates=None, threshold=None):
    click.echo('balance letters')
    click.echo('NOT READY TO USE CURRENT MONTH, AM I?')
    click.echo('example: python cli.py balanceletters 2022-07 2023-01 100')
    figure = Figuration()
    path, staging_layer, close_layer, build, service, _ = figure.return_configuration()
    docx = DocxWriter(db=build.main_db, service=service)
    docx.export_history_to_docx(
        threshold=threshold, startm=dates[0], endm=dates[1])
    click.echo(
        'remember to update https://docs.google.com/document/d/1OWvvOYvmXh5h131jjGWBoVGTmdDttFaR1_kYktoLwZ0/edit')
    click.echo('to reflect new deadlines created by letters.')

    # exclude abandoned, sick, or payment plans


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
def db_to_excel():
    click.echo('write_db_to_excel')
    figure = Figuration()
    path, full_sheet, build, service, ms = figure.return_configuration()
    ms.auto_control(mode='to_excel')


"""ANALYSIS"""


@click.command()
@click.argument('column', nargs=3)
def analysis(column=None):
    click.echo('analysis')
    figure = Figuration()
    db, tables = figure.analysis_test_configuration()
    analysis = Analysis(db=db,
                        tables=[FinalMonth])
    analysis.sum_over_range_by_type(column=column[0],
                                    period_strt=column[1],
                                    period_end=column[2],
                                    )
    analysis.print_df()


"""ANNUAL FINANCIALS"""


@click.command()
def annfin():
    click.echo('annual financials')
    figure = Figuration()
    path, output_path, db, staging_layer = figure.annfin_test_configuration()
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
cli.add_command(db_to_excel)
cli.add_command(sqlite_dump)
cli.add_command(docx_letters)
cli.add_command(balanceletters)
cli.add_command(addresses)
cli.add_command(workorders_to_db)
cli.add_command(export_workorders_docx)
cli.add_command(recvactuals)
cli.add_command(manentry)
"""ANALYSIS"""
cli.add_command(analysis)
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
