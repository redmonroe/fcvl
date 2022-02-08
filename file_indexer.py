from utils import Utils

# list all files
# divide by .ext (pdf, xls, xlsx)

# GET TO MOST RECENT RENT ROLL 

class FileIndexer:

    def __init__(self, path=None):
        self.path = path

    
    def to_xlsx(self):
        import os
        import datetime
        target = 'TEST_deposits_01_2022.xls'
        target_file = os.path.join(self.path, target)
        # print(target_file)


        # SO GET MOST RECENT FILE, MAKE CHANGES BUT PRESERVE TIMING FOR EVENTUAL DISPLAY

        target_dir = os.listdir(self.path)
        date_dict = {}
        for item in target_dir:
            td = os.path.join(self.path, item)
            file_stat = os.stat(td)
            date_time = datetime. datetime.fromtimestamp(file_stat.st_ctime)
            print(item, date_time)