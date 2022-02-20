from config import Config
import dataset
import pandas as pd

class Checklist:

    def __init__(self, db):
        self.db = db
        self.tablename = 'checklist'

    def make_checklist(self):
        table = self.db[self.tablename]
        table.drop()

        month_list = pd.date_range(f'{Config.current_year}-01-01',f'{Config.current_year}-12-31', 
              freq='MS').strftime("%b").tolist()

        for month in month_list:  
            table.insert(dict(
                year=f'{Config.current_year}', 
                month=month,
                rs_exist=False,
                yfor=False, 
                mfor=False, 
                rr_proc=False,
                depdetail_proc=False, 
                stmt_proc=False,
                dep_rec=False,
                rs_write=False,
                ))



    def show_checklist(self):
        check_items = [item for item in self.db[self.tablename]]
        return check_items

    


if __name__ == '__main__':
    clist = Checklist(db=Config.test_checklist_db)
    clist.sample_checklist()
    clist.show_checklist()