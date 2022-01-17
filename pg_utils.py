# pg_utils.py
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

  