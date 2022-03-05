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
                month=month.lower(),
                rs_exist=False,
                yfor=False, 
                mfor=False, 
                rr_proc=False,
                depdetail_proc=False, 
                stmt_proc=False,
                dep_rec=False,
                rs_write=False,
                ))

    def get_checklist(self):
        checklist = self.db[self.tablename]
        return checklist

    def show_checklist(self):
        yfor_list = []
        rs_exist_list = []
        check_items = [item for item in self.db[self.tablename]]
        for item in check_items:
            print(item)
            yfor_list.append(item['yfor'])
            rs_exist_list.append(item['rs_exist'])
        return check_items, yfor_list, rs_exist_list


if __name__ == '__main__':
    clist = Checklist(db=Config.test_checklist_db)
    # clist.make_checklist()
    clist.show_checklist()