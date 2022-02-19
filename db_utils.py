from datetime import datetime as dt
from config import Config
import os


def pg_dump_one():
    bu_time = dt.now()
    print(bu_time)
    os.system(f'pg_dump --dbname={Config.PG_DUMPS_URI} > "{Config.DB_BACKUPS}\loaderdump{bu_time.month}{bu_time.day}{bu_time.year}{bu_time.hour}.sql"')

def pg_restore_one(infile, testing=True):
    print('infile name:', infile)
    os.system(f'psql -d fcvfin_tables -U postgres -f "{infile}') #just need the relative path, should be in working directory of fcvfin here


class DBUtils:

    @staticmethod
    def get_tables(self, db):
        return db.tables

    @staticmethod
    def delete_table(self, db):
        count_list = []
        item_list = []
        print(f'\nDeleting tables from {db}')
        for count, item in enumerate(db.tables, 1):
            print(count, item)
            count_list.append(count)
            item_list.append(item)

        if len(count_list) > 0:
            selection = int(input("Please select a table to delete:"))
        else:
            print(f'\nThere are no tables to delete in {db}\n')

        choice_file = dict(zip(count_list, item_list))

        for k, v in choice_file.items():
            if selection == k:
                print(f'You chose to delete option {selection}: {v}')
                table = db[v]
                table.drop()
