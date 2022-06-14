import os
from datetime import datetime as dt
from pathlib import Path

from config import Config


class DBUtils:

    @staticmethod
    def pw_connect_to_db(db=None, tables_list=None):
        if db.is_closed() == True:
            db.connect()
        db.create_tables(tables_list)

    @staticmethod
    def dump_sqlite(path_to_existing_db=None, path_to_backup=None):
        import sqlite3
        backup_time = dt.now()
        db_name = path_to_existing_db.split('/')[-1]
        db_name = db_name.split('.')[0]
        filename = db_name + '_' + str(backup_time.year) + str(backup_time.month) + str(backup_time.day) + '.sql'
        write_path = path_to_backup + '/' + filename

        con = sqlite3.connect(path_to_existing_db)
        with open(write_path, 'w') as f:
            for line in con.iterdump():
                f.write('%s\n' % line)

    @staticmethod
    def find_sqlite(path_to_existing_db=None, path_to_backup=None):
        path_to_backup= Path(path_to_backup)
        dir_contents = [item for item in path_to_backup.iterdir() if item.suffix != '.ini'] 

        backup_time = dt.now()
        db_name = path_to_existing_db.split('/')[-1]
        db_name = db_name.split('.')[0]
        target_file_name = db_name + '_' + str(backup_time.year) + str(backup_time.month) + str(backup_time.day)
        fn_list = [filename.stem for filename in dir_contents]

        if target_file_name in fn_list:
            match_bool = True
        else:
            match_bool = False

        return match_bool

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
    
    def pg_dump_one():
        bu_time = dt.now()
        print(bu_time)
        os.system(f'pg_dump --dbname={Config.PG_DUMPS_URI} > "{Config.DB_BACKUPS}\loaderdump{bu_time.month}{bu_time.day}{bu_time.year}{bu_time.hour}.sql"')

    def pg_restore_one(infile, testing=True):
        print('infile name:', infile)
        os.system(f'psql -d fcvfin_tables -U postgres -f "{infile}') #just need the relative path, should be in working directory of fcvfin here
